package backlogadmin

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// waitsReader is a Reader that also owns task-bound waits and supervision
// readiness snapshots, which is what the coordinator store is.
type waitsReader struct {
	explainReader
	waits []domain.TaskWait
}

func (r waitsReader) ListTaskWaits(context.Context) ([]domain.TaskWait, error) {
	return r.waits, nil
}

// A parked task's live waits and a supervised run's gates are part of the
// workflow detail, so "campaign show" says what a parked task is waiting for
// and which observed task a pending gate is still missing evidence from.
// Before this, an operator saw "waiting-external" and "pending-evidence" and
// had to read the store to learn either.
func TestWorkflowDetailCarriesLiveWaitsAndGateEvidenceGaps(t *testing.T) {
	now := explainTestNow
	reader := waitsReader{explainReader: explainFixture(true)}
	reader.explainReader.records.Tasks = []domain.Task{
		{ID: "task-analyse", Name: "analyse", WorkflowID: "wf-1"},
		{ID: "task-nest", Name: "nest-model", WorkflowID: "wf-1"},
		{ID: "task-publish", Name: "publish", WorkflowID: "wf-1"},
	}
	reader.explainReader.records.Attempts = []domain.Attempt{
		{ID: "attempt-nest", WorkflowRunID: "run-1", TaskID: "task-nest", Number: 1,
			Progress: domain.ProgressWaitingExternal, Control: domain.ControlWaitingExternal, Revision: 5},
	}
	reader.explainReader.snapshot.Gates = []domain.Gate{{
		Definition: domain.GateDefinition{
			ID: "gate-1", Name: "analysis_review",
			ObservedTaskIDs:  []string{"task-analyse", "task-nest"},
			ProtectedTaskIDs: []string{"task-publish"},
		},
		RunID: "run-1", State: domain.GatePendingEvidence, GraphRevision: 12, Revision: 1,
	}}
	reader.waits = []domain.TaskWait{
		{
			ID: "w-tw-nest-model-1", WorkflowRunID: "run-1", TaskID: "task-nest", AttemptID: "attempt-nest",
			Name: "nest model answered", Condition: "jocasta exists home-assistant/inputs/nest-model.md",
			RegisteredAt: now.Add(-time.Hour), Deadline: now.Add(23 * time.Hour),
		},
		{
			// A settled wait no longer parks anything and is not reported.
			ID: "w-tw-old", WorkflowRunID: "run-1", TaskID: "task-nest", AttemptID: "attempt-nest",
			Name: "earlier", Condition: "true", RegisteredAt: now.Add(-2 * time.Hour), Deadline: now,
			SettledAt: &now, Result: &domain.TaskWaitResult{Outcome: domain.TaskWaitMet, ExitCode: 0},
		},
		{
			// Another run's wait is not this run's business.
			ID: "w-tw-other", WorkflowRunID: "run-2", TaskID: "task-x", AttemptID: "attempt-x",
			Name: "elsewhere", Condition: "true", RegisteredAt: now, Deadline: now.Add(time.Hour),
		},
	}
	// The service reads waits through the reader it was built with, so it is
	// built around the reader that owns them.
	service, err := New(reader, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now })
	service.SetSupervisorClientConfigured(true)

	response, err := service.Query(context.Background(), Query{
		Version: Version, Kind: QueryWorkflow,
		Principal:     Principal{ID: "local:1000", Roles: []string{LocalAdminRole}},
		WorkflowRunID: "run-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	detail := response.Workflow
	if detail == nil {
		t.Fatal("no workflow detail")
	}
	if len(detail.Waits) != 1 {
		t.Fatalf("waits = %#v, want the one live wait of this run", detail.Waits)
	}
	wait := detail.Waits[0]
	if wait.ID != "w-tw-nest-model-1" || wait.TaskID != "task-nest" || wait.TaskName != "nest-model" ||
		wait.AttemptID != "attempt-nest" || wait.Name != "nest model answered" ||
		wait.Condition != "jocasta exists home-assistant/inputs/nest-model.md" ||
		!wait.Deadline.Equal(now.Add(23*time.Hour)) {
		t.Fatalf("wait = %#v", wait)
	}
	if len(detail.Gates) != 1 {
		t.Fatalf("gates = %#v, want the run's one gate", detail.Gates)
	}
	gate := detail.Gates[0]
	if gate.ID != "gate-1" || gate.Name != "analysis_review" || gate.State != domain.GatePendingEvidence ||
		len(gate.ProtectedTaskIDs) != 1 || gate.ProtectedTaskIDs[0] != "task-publish" {
		t.Fatalf("gate = %#v", gate)
	}
	// analyse has never run and nest-model is parked: neither has produced
	// evidence, and both are named.
	if len(gate.MissingEvidence) != 2 || gate.MissingEvidence[0].TaskID != "task-analyse" ||
		gate.MissingEvidence[0].TaskName != "analyse" || gate.MissingEvidence[0].Progress != domain.ProgressQueued ||
		gate.MissingEvidence[1].TaskID != "task-nest" || gate.MissingEvidence[1].Progress != domain.ProgressWaitingExternal {
		t.Fatalf("missing evidence = %#v", gate.MissingEvidence)
	}
}

// A gate whose observed tasks all succeeded reports nothing missing.
func TestWorkflowDetailReportsCompleteEvidence(t *testing.T) {
	now := explainTestNow
	reader := waitsReader{explainReader: explainFixture(true)}
	reader.explainReader.records.Tasks = []domain.Task{
		{ID: "task-analyse", Name: "analyse", WorkflowID: "wf-1"},
		{ID: "task-publish", Name: "publish", WorkflowID: "wf-1"},
	}
	reader.explainReader.records.Attempts = []domain.Attempt{
		{ID: "attempt-analyse", WorkflowRunID: "run-1", TaskID: "task-analyse", Number: 1,
			Progress: domain.ProgressSucceeded, Control: domain.ControlStopped, Revision: 9},
	}
	service, err := New(reader, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now })
	service.SetSupervisorClientConfigured(true)
	response, err := service.Query(context.Background(), Query{
		Version: Version, Kind: QueryWorkflow,
		Principal:     Principal{ID: "local:1000", Roles: []string{LocalAdminRole}},
		WorkflowRunID: "run-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	detail := response.Workflow
	if len(detail.Waits) != 0 {
		t.Fatalf("waits = %#v, want none", detail.Waits)
	}
	if len(detail.Gates) != 1 || len(detail.Gates[0].MissingEvidence) != 0 || detail.Gates[0].State != domain.GateReadyForReview {
		t.Fatalf("gates = %#v", detail.Gates)
	}
}

// A reader that does not own task waits, which is every reader that is not the
// coordinator store, still answers the workflow query.
func TestWorkflowDetailWithoutAWaitStoreStillAnswers(t *testing.T) {
	service := explainService(t, explainFixture(true), true)
	response, err := service.Query(context.Background(), Query{
		Version: Version, Kind: QueryWorkflow,
		Principal:     Principal{ID: "local:1000", Roles: []string{LocalAdminRole}},
		WorkflowRunID: "run-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Workflow == nil || len(response.Workflow.Waits) != 0 || len(response.Workflow.Gates) != 1 {
		t.Fatalf("workflow = %#v", response.Workflow)
	}
	var _ sqlite.CoordinatorRecords
}
