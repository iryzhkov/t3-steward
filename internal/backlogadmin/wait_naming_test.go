package backlogadmin

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

var waitNamingNow = time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)

// waitNamingView is one run with one task and two task waits, one live and one
// settled, built without a store: the naming of the two wait families is a
// property of the documents, not of where the records came from.
func waitNamingView() view {
	records := sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: "workflow-1", Version: 1, Name: "workflow", Class: domain.TaskClassSurplus,
			TaskIDs: []string{"task-1"},
		}},
		WorkflowRuns: []domain.WorkflowRun{{
			ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressActive,
			Revision: 1, CreatedAt: waitNamingNow, UpdatedAt: waitNamingNow,
		}},
		Tasks: []domain.Task{{ID: "task-1", WorkflowID: "workflow-1", Name: "alpha", Class: domain.TaskClassSurplus}},
		Attempts: []domain.Attempt{{
			ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1,
			Progress: domain.ProgressWaitingExternal, Control: domain.ControlWaitingExternal,
			Revision: 2, UpdatedAt: waitNamingNow,
		}},
	}
	settledAt := waitNamingNow.Add(-time.Hour)
	v := newView(records, nil, nil, RuntimeInfo{}, waitNamingNow)
	v.taskWaits = []domain.TaskWait{
		{
			ID: "tw-live", WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1",
			Name: "ci", Condition: "gh run view", Kind: domain.WaitKindShell,
			RegisteredAt: waitNamingNow.Add(-2 * time.Hour), Deadline: waitNamingNow.Add(time.Hour),
		},
		{
			ID: "tw-settled", WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1",
			Name: "review", Condition: "gh pr view", Kind: domain.WaitKindShell,
			RegisteredAt: waitNamingNow.Add(-3 * time.Hour), Deadline: waitNamingNow.Add(time.Hour),
			SettledAt: &settledAt,
			Result: &domain.TaskWaitResult{
				Outcome: domain.TaskWaitFailed, ExitCode: 2, Reason: "the check failed",
				ObservedAt: settledAt,
			},
		},
	}
	return v
}

// A settled wait is why a task stopped waiting, and it was dropped from the
// run document entirely: "campaign show" could say a task was parked and, one
// tick later, say nothing at all about what became of the wait.
func TestWorkflowDetailListsSettledTaskWaitsWithTheirOutcome(t *testing.T) {
	detail, ok := waitNamingView().workflowDetail("run-1")
	if !ok {
		t.Fatal("run-1 has no detail")
	}
	if len(detail.TaskWaits) != 2 {
		t.Fatalf("taskWaits = %+v, want the live and the settled one", detail.TaskWaits)
	}
	byID := map[string]TaskWaitDetail{}
	for _, wait := range detail.TaskWaits {
		byID[wait.ID] = wait
	}
	settled := byID["tw-settled"]
	if settled.Outcome != string(domain.TaskWaitFailed) || settled.ExitCode != 2 ||
		settled.Reason != "the check failed" || settled.SettledAt == nil {
		t.Fatalf("settled wait = %+v", settled)
	}
	if live := byID["tw-live"]; live.Outcome != "" || live.SettledAt != nil {
		t.Fatalf("a live wait reported an outcome: %+v", live)
	}
	// The old key keeps its old meaning for one release: the live waits only.
	if len(detail.Waits) != 1 || detail.Waits[0].ID != "tw-live" {
		t.Fatalf("waits = %+v, want the live wait alone", detail.Waits)
	}
}

// One key, two meanings: "waits" was the task waits in a run document and the
// node waits in a diagnosis. Both documents now name each family, and both
// keep the old key for one release so an rc.69 client still reads them.
func TestWaitKeysNameTheSameFamilyInBothDocuments(t *testing.T) {
	detail, ok := waitNamingView().workflowDetail("run-1")
	if !ok {
		t.Fatal("run-1 has no detail")
	}
	raw, err := json.Marshal(detail)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"waits", "taskWaits"} {
		if _, ok := document[key]; !ok {
			t.Fatalf("the run document has no %q key: %s", key, raw)
		}
	}

	diagnosis := Diagnosis{
		NodeWaits: []domain.NodeWait{{Request: domain.NodeWaitRequest{ID: "nw-1"}}},
		TaskWaits: []domain.TaskWait{{ID: "tw-live"}},
	}
	diagnosis.Waits = diagnosis.NodeWaits
	raw, err = json.Marshal(diagnosis)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"waits", "nodeWaits", "taskWaits"} {
		if _, ok := document[key]; !ok {
			t.Fatalf("the diagnosis has no %q key: %s", key, raw)
		}
	}
}

// Both keys are present even when there is nothing to report, so a reader
// branches on the array rather than on whether the field exists at all.
func TestWaitKeysAreAlwaysPresent(t *testing.T) {
	raw, err := json.Marshal(WorkflowDetail{})
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"waits", "taskWaits"} {
		if _, ok := document[key]; !ok {
			t.Fatalf("an empty run document omits %q: %s", key, raw)
		}
	}
}
