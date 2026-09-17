package backlogadmin

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// A parked attempt is parked on a task-bound wait, and the diagnosis is where
// an operator looks for why a run is not moving. Listing node waits only left
// `"waits": null` beside an attempt that was waiting on something.
func TestDiagnoseListsTheTaskWaitsOfAParkedAttempt(t *testing.T) {
	ctx := context.Background()
	store := openAdminTestStore(t)
	now := adminTestNow
	tasks := []domain.Task{{ID: "task-1", WorkflowID: "workflow-1", Name: "nest-model", Class: domain.TaskClassRequired}}
	run, err := domain.BindRunSink(domain.WorkflowRun{
		ID: "run-1", WorkflowID: "workflow-1", Revision: 1,
		Progress: domain.ProgressActive, CreatedAt: now, UpdatedAt: now,
	}, tasks)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "workflow-1", Version: 2, Name: "ha", Project: "home-assistant", Class: domain.TaskClassRequired, TaskIDs: []string{"task-1"}, CreatedAt: now}},
		WorkflowRuns: []domain.WorkflowRun{run},
		Tasks:        tasks,
		Attempts: []domain.Attempt{{
			ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1, Revision: 7,
			Progress: domain.ProgressActive, Control: domain.ControlRunning,
			ThreadID: "thread-1", AssignmentID: "assign-1", UpdatedAt: now,
		}},
		Assignments: []domain.Assignment{{
			ID: "assign-1", AttemptID: "attempt-1", WorkerID: "worker", Epoch: 1,
			State: domain.AssignmentClaimed, LeaseToken: "lease", DispatchToken: "dispatch",
			ThreadID: "thread-1", CreatedAt: now, UpdatedAt: now,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	registered, err := store.RegisterTaskWait(ctx, domain.TaskWaitRegistration{
		RequestID: "req-1", WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1",
		IssuedRevision: 7, ThreadID: "thread-1", Wake: domain.WakeEach, MaxDuration: time.Hour,
		Name: "nest model answer", Condition: "jocasta exists home-assistant/inputs/nest-model.md",
	}, now)
	if err != nil {
		t.Fatal(err)
	}

	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now })
	response, err := service.Query(ctx, Query{Version: Version, Kind: QueryDiagnose, WorkflowRunID: "run-1", Principal: Principal{ID: "operator"}})
	if err != nil {
		t.Fatal(err)
	}
	d := response.Diagnosis
	if d == nil || len(d.TaskWaits) != 1 {
		t.Fatalf("diagnosis lists %d task waits, want 1: %+v", len(d.TaskWaits), d)
	}
	got := d.TaskWaits[0]
	if got.ID != registered.ID || got.AttemptID != "attempt-1" || got.TaskID != "task-1" ||
		got.Name != "nest model answer" || got.Condition == "" || got.Deadline.IsZero() || got.RegisteredAt.IsZero() {
		t.Fatalf("task wait evidence is incomplete: %+v", got)
	}
	if got.Result != nil {
		t.Fatalf("a live wait reports an outcome: %+v", got.Result)
	}
	for _, task := range d.Workflow.Tasks {
		if task.Attempt != nil && task.Attempt.Progress != domain.ProgressWaitingExternal {
			t.Fatalf("the attempt is not parked: %+v", task.Attempt)
		}
	}
}
