package backlogadmin

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// cancelRunRecords is a run of three independent tasks, the shape a fan-out
// start produces: no task needs another, so cancelling one cascades to
// nothing and an operator had to send one command per task.
func cancelRunRecords(now time.Time) sqlite.CoordinatorRecords {
	return sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: "workflow-1", Version: 1, Name: "workflow", Class: domain.TaskClassSurplus,
			TaskIDs: []string{"task-alpha", "task-beta", "task-gamma"},
		}},
		WorkflowRuns: []domain.WorkflowRun{{
			ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressActive,
			Revision: 4, CreatedAt: now, UpdatedAt: now,
		}},
		Tasks: []domain.Task{
			{ID: "task-alpha", WorkflowID: "workflow-1", Name: "alpha", Class: domain.TaskClassSurplus},
			{ID: "task-beta", WorkflowID: "workflow-1", Name: "beta", Class: domain.TaskClassSurplus},
			{ID: "task-gamma", WorkflowID: "workflow-1", Name: "gamma", Class: domain.TaskClassSurplus},
		},
		Attempts: []domain.Attempt{
			{ID: "attempt-alpha", WorkflowRunID: "run-1", TaskID: "task-alpha", Number: 1, Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 2, UpdatedAt: now},
			{ID: "attempt-beta", WorkflowRunID: "run-1", TaskID: "task-beta", Number: 1, Progress: domain.ProgressReady, Control: domain.ControlUnassigned, Revision: 6, UpdatedAt: now},
			{ID: "attempt-gamma", WorkflowRunID: "run-1", TaskID: "task-gamma", Number: 1, Progress: domain.ProgressQueued, Control: domain.ControlUnassigned, Revision: 1, UpdatedAt: now},
		},
	}
}

func cancelRunMutation(id string) Mutation {
	return Mutation{
		Version: Version, Principal: Principal{ID: "operator"}, ID: id,
		Kind: domain.AdminCommandCancel, WorkflowRunID: "run-1",
		ExpectedRevision: 2, Reason: "the whole run is obsolete",
		Payload: json.RawMessage(`{"scope":"run"}`),
	}
}

// Cancelling a fan-out run took one command per task, each with its own
// revision fence, and a partial failure left the run half cancelled. One
// command now cancels every non-terminal task of the run in one application:
// one command id, one fence per attempt, one decision.
func TestCancelRunCancelsEveryTaskInOneApplication(t *testing.T) {
	store := openAdminTestStore(t)
	now := adminTestNow
	if err := store.SaveCoordinatorRecords(context.Background(), cancelRunRecords(now)); err != nil {
		t.Fatal(err)
	}
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now.Add(time.Minute) })
	if _, err := service.Mutate(context.Background(), cancelRunMutation("cancel-run-1")); err != nil {
		t.Fatal(err)
	}
	report, err := service.ExecutePendingCommands(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Decisions) != 1 {
		t.Fatalf("decisions = %#v, want exactly one application", report.Decisions)
	}
	if state := report.Decisions[0].Command.State; state != domain.AdminCommandApplied {
		t.Fatalf("command state = %q, failure = %q", state, report.Decisions[0].Command.Failure)
	}
	loaded, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantRevision := map[string]int64{"attempt-alpha": 3, "attempt-beta": 7, "attempt-gamma": 2}
	for _, attempt := range loaded.Attempts {
		if attempt.Progress != domain.ProgressCancelled {
			t.Fatalf("%s is %s, want cancelled", attempt.ID, attempt.Progress)
		}
		// Each attempt moved exactly one revision from its own, which is what
		// "one fence per attempt" means: they did not share the target's.
		if attempt.Revision != wantRevision[attempt.ID] {
			t.Fatalf("%s revision = %d, want %d", attempt.ID, attempt.Revision, wantRevision[attempt.ID])
		}
	}
	if loaded.WorkflowRuns[0].Progress != domain.ProgressCancelled || loaded.WorkflowRuns[0].CompletedAt == nil {
		t.Fatalf("run = %#v", loaded.WorkflowRuns[0])
	}
}

// A run whose tasks are all terminal has nothing to cancel, and the refusal
// says that rather than reporting a command that did nothing.
func TestCancelRunRefusesATerminalRun(t *testing.T) {
	store := openAdminTestStore(t)
	now := adminTestNow
	records := cancelRunRecords(now)
	for i := range records.Attempts {
		records.Attempts[i].Progress = domain.ProgressSucceeded
		records.Attempts[i].Control = domain.ControlStopped
	}
	records.WorkflowRuns[0].Progress = domain.ProgressSucceeded
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now.Add(time.Minute) })
	_, err = service.Mutate(context.Background(), cancelRunMutation("cancel-run-2"))
	if err == nil || !strings.Contains(err.Error(), "terminal") {
		t.Fatalf("error = %v, want a refusal naming the terminal run", err)
	}
}

// The command is idempotent under its id, exactly as a task cancel is: a
// resubmission of the same request replays rather than cancelling twice.
func TestCancelRunReplaysUnderTheSameCommandID(t *testing.T) {
	store := openAdminTestStore(t)
	now := adminTestNow
	if err := store.SaveCoordinatorRecords(context.Background(), cancelRunRecords(now)); err != nil {
		t.Fatal(err)
	}
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now.Add(time.Minute) })
	if _, err := service.Mutate(context.Background(), cancelRunMutation("cancel-run-3")); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ExecutePendingCommands(context.Background()); err != nil {
		t.Fatal(err)
	}
	replay, err := service.Mutate(context.Background(), cancelRunMutation("cancel-run-3"))
	if err != nil {
		t.Fatalf("the replay of a run cancel was refused: %v", err)
	}
	if replay.Command.ID != "cancel-run-3" {
		t.Fatalf("replay = %#v", replay)
	}
}
