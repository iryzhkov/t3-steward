package backlog

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// eachWakeFixture parks one attempt on two task-bound waits and settles the
// first, which under each resumes the attempt while the second stays live.
//
// It uses the real store because the defect it reproduces is in what the store
// answers, and a fake told to answer the intended thing would prove nothing.
func eachWakeFixture(t *testing.T, second domain.WakeMode) (*sqlite.Store, string) {
	t.Helper()
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	run := domain.WorkflowRun{ID: "run-1", WorkflowID: "workflow-1", Revision: 1,
		Progress: domain.ProgressActive, CreatedAt: now, UpdatedAt: now}
	tasks := []domain.Task{{ID: "task-1", WorkflowID: "workflow-1", Name: "task", Class: domain.TaskClassRequired}}
	if run, err = domain.BindRunSink(run, tasks); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{run}, Tasks: tasks,
		Attempts: []domain.Attempt{{
			ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1, Revision: 3,
			Progress: domain.ProgressActive, Control: domain.ControlRunning,
			ThreadID: "thread-1", AssignmentID: "assign-1", UpdatedAt: now,
		}},
		Assignments: []domain.Assignment{{
			ID: "assign-1", AttemptID: "attempt-1", WorkerID: "worker-b", Epoch: 2,
			State: domain.AssignmentClaimed, LeaseToken: "lease", DispatchToken: "dispatch",
			ThreadID: "thread-1", CreatedAt: now, UpdatedAt: now,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveWorkerSnapshot(ctx, domain.WorkerSnapshot{
		WorkerID: "worker-b", WorkerEpoch: "worker-epoch-1", CoordinatorEpoch: 1, Sequence: 1,
		Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Hour),
		Inventory: domain.WorkerInventory{ID: "worker-b", AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: now},
	}); err != nil {
		t.Fatal(err)
	}
	register := func(requestID string, wake domain.WakeMode) domain.TaskWait {
		t.Helper()
		wait, err := store.RegisterTaskWait(ctx, domain.TaskWaitRegistration{
			RequestID: requestID, WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1",
			IssuedRevision: 3, ThreadID: "thread-1", Wake: wake, MaxDuration: time.Hour,
			Name: requestID, Condition: "test -f signal",
		}, now)
		if err != nil {
			t.Fatalf("registering %s: %v", requestID, err)
		}
		return wait
	}
	urgent := register("urgent", domain.WakeEach)
	register("slow", second)
	if _, err := store.SettleTaskWait(ctx, urgent.ID, domain.TaskWaitResult{
		Outcome: domain.TaskWaitMet, Reason: "condition met",
	}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	wakes, err := store.WakeTaskWaits(ctx, now.Add(time.Minute))
	if err != nil || len(wakes) != 1 {
		t.Fatalf("the each settlement did not wake the attempt: %v %v", wakes, err)
	}
	return store, "attempt-1"
}

// deliverEveryTaskWake carries every pending wake through sending to delivered,
// which is what the wait runner does once the message reaches the thread. Until
// that happens the attempt is still parked on purpose, so a test that asserts
// the resumed attempt is unparked has to get past it rather than around it.
func deliverEveryTaskWake(ctx context.Context, t *testing.T, store *sqlite.Store) {
	t.Helper()
	at := time.Now().UTC()
	pending, err := store.TaskWakesAwaitingDelivery(ctx, at)
	if err != nil {
		t.Fatal(err)
	}
	for _, wake := range pending {
		for _, wait := range wake.Waits {
			if _, err := store.TransitionTaskWake(ctx, wait.ID, wait.Delivery, "sending", at); err != nil {
				t.Fatal(err)
			}
			if _, err := store.TransitionTaskWake(ctx, wait.ID, "sending", "delivered", at); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// F6. An attempt resumed by an each settlement is not parked, however many of
// its other waits are still live, and every consumer of "is this parked" must
// agree with the attempt's own progress.
//
// The fleet disagreed. The coordinator told the worker the assignment was still
// parked, because a wait was still live, so when the resumed turn ended the
// worker put the attempt straight back into waiting-external instead of
// collecting it. The each wake was undone as fast as it was applied, and from
// outside the task simply never woke.
func TestAnEachWakeUnparksTheAttemptForEveryConsumer(t *testing.T) {
	for _, second := range []domain.WakeMode{domain.WakeEach, domain.WakeAll} {
		t.Run("second wait is "+string(second), func(t *testing.T) {
			ctx := context.Background()
			store, attemptID := eachWakeFixture(t, second)

			parked, err := store.LiveTaskWaitAttempts(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if waitID, ok := parked[attemptID]; ok {
				t.Fatalf("the resumed attempt is reported parked on %q", waitID)
			}

			// Until the wake reaches the thread the attempt is still parked, and
			// must be: the turn that parked has ended and the resumed one has not
			// begun, so a worker reading "not parked" here collects a task that is
			// about to run again. This half and the one below are the two defects
			// that met in this predicate; asserting only one of them is what let
			// each fix reintroduce the other.
			waking, err := ParkedAssignmentsFor(ctx, store, "worker-b")
			if err != nil {
				t.Fatal(err)
			}
			if len(waking.Parked) != 1 {
				t.Fatalf("an undelivered wake left the attempt unparked: %+v", waking.Parked)
			}
			deliverEveryTaskWake(ctx, t, store)

			// What the worker is told, which is what re-parked the attempt.
			statement, err := ParkedAssignmentsFor(ctx, store, "worker-b")
			if err != nil {
				t.Fatal(err)
			}
			if !statement.ParkedReported {
				t.Fatal("the coordinator said nothing about parked assignments")
			}
			if len(statement.Parked) != 0 {
				t.Fatalf("the worker was told the resumed assignment is parked: %+v", statement.Parked)
			}

			// And the turn the wake started may finish: its done marker settles
			// the attempt instead of being refused as a contradiction.
			now := time.Date(2026, 9, 14, 12, 30, 0, 0, time.UTC)
			attempts, _, err := store.LoadTurnOutcomeState(ctx)
			if err != nil {
				t.Fatal(err)
			}
			live, err := store.LiveTaskWaitAttempts(ctx)
			if err != nil {
				t.Fatal(err)
			}
			transitions, refusals, err := PlanTurnOutcomesWithTaskWaits(attempts, nil, []domain.TurnOutcome{{
				ID: "outcome-1", AttemptID: attemptID, Marker: domain.TurnOutcomeDone,
				VerificationPassed: true, ObservedAt: now,
			}}, live, now)
			if err != nil {
				t.Fatal(err)
			}
			if len(refusals) != 0 {
				t.Fatalf("the resumed turn was refused as done-while-waiting: %+v", refusals)
			}
			if len(transitions) != 1 || transitions[0].Attempt.Progress != domain.ProgressSucceeded {
				t.Fatalf("the resumed turn did not settle the attempt: %+v", transitions)
			}
		})
	}
}

// The guard itself is unchanged for an attempt that really is parked: a done
// marker for it is still refused and recorded, which is the failure the guard
// was added for.
func TestAParkedAttemptStillRefusesADoneMarker(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	run := domain.WorkflowRun{ID: "run-1", WorkflowID: "workflow-1", Revision: 1,
		Progress: domain.ProgressActive, CreatedAt: now, UpdatedAt: now}
	tasks := []domain.Task{{ID: "task-1", WorkflowID: "workflow-1", Name: "task", Class: domain.TaskClassRequired}}
	if run, err = domain.BindRunSink(run, tasks); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{run}, Tasks: tasks,
		Attempts: []domain.Attempt{{
			ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1, Revision: 3,
			Progress: domain.ProgressActive, Control: domain.ControlRunning,
			ThreadID: "thread-1", AssignmentID: "assign-1", UpdatedAt: now,
		}},
		Assignments: []domain.Assignment{{
			ID: "assign-1", AttemptID: "attempt-1", WorkerID: "worker-b", Epoch: 2,
			State: domain.AssignmentClaimed, LeaseToken: "lease", DispatchToken: "dispatch",
			ThreadID: "thread-1", CreatedAt: now, UpdatedAt: now,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RegisterTaskWait(ctx, domain.TaskWaitRegistration{
		RequestID: "ci", WorkflowRunID: "run-1", TaskID: "task-1", AttemptID: "attempt-1",
		IssuedRevision: 3, ThreadID: "thread-1", Wake: domain.WakeEach, MaxDuration: time.Hour,
		Name: "ci", Condition: "gh run view",
	}, now); err != nil {
		t.Fatal(err)
	}
	parked, err := store.LiveTaskWaitAttempts(ctx)
	if err != nil || parked["attempt-1"] != "tw-ci" {
		t.Fatalf("a parked attempt is not reported parked: %v %v", parked, err)
	}
	statement, err := ParkedAssignmentsFor(ctx, store, "worker-b")
	if err != nil || len(statement.Parked) != 1 || statement.Parked[0].AssignmentID != "assign-1" {
		t.Fatalf("the worker was not told its assignment is parked: %+v %v", statement, err)
	}
	_, err = ReconcileTurnOutcomes(ctx, store, []domain.TurnOutcome{{
		ID: "outcome-1", AttemptID: "attempt-1", Marker: domain.TurnOutcomeDone,
		VerificationPassed: true, ObservedAt: now,
	}}, now)
	if !errors.Is(err, ErrTurnOutcomeWaiting) {
		t.Fatalf("a done marker was honoured while the attempt was parked: %v", err)
	}
}
