package sqlite

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// namedRun saves a one-task run with the given id and progress, so a task can
// wait for several runs at once.
func namedRun(t *testing.T, store *Store, now time.Time, runID string, progress domain.ProgressState) {
	t.Helper()
	tasks := []domain.Task{{ID: runID + "-t", Name: "deploy", WorkflowID: runID + "-w"}}
	run, err := domain.BindRunSink(domain.WorkflowRun{ID: runID, WorkflowID: runID + "-w", Revision: 1, Progress: domain.ProgressActive, CreatedAt: now, UpdatedAt: now}, tasks)
	if err != nil {
		t.Fatal(err)
	}
	attempt := domain.Attempt{ID: runID + "-a", TaskID: runID + "-t", WorkflowRunID: runID, Number: 1, Revision: 1, Progress: progress, Control: domain.ControlRunning, UpdatedAt: now}
	if progress.Terminal() {
		attempt.Control = domain.ControlStopped
		if run, err = domain.ProjectRunSink(run, tasks, []domain.Attempt{attempt}, nil, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}, Tasks: tasks, Attempts: []domain.Attempt{attempt}}); err != nil {
		t.Fatal(err)
	}
}

// W2: one task waits for two runs in one park. Under all, the first run
// settling does not wake the task; the second does, and the one wake carries
// both outcomes, in the trailer and in the prose. Under any, the first run
// settling wakes it.
func TestTaskWaitsForSeveralRunsInOnePark(t *testing.T) {
	for _, mode := range []domain.WakeMode{domain.WakeAll, domain.WakeEach} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			store, attempt, now := taskWaitFixture(t)
			namedRun(t, store, now, "run-a", domain.ProgressActive)
			namedRun(t, store, now, "run-b", domain.ProgressActive)
			var ids []string
			for _, run := range []string{"run-a", "run-b"} {
				registration := nodeRegistration(loadAttempt(t, store, attempt.ID), "park-"+run, domain.NodeRef{RunID: run, TaskID: domain.SinkTaskName}, "")
				registration.Wake = mode
				registered, err := store.RegisterTaskWait(ctx, registration, now)
				if err != nil {
					t.Fatalf("registering the wait for %s: %v", run, err)
				}
				ids = append(ids, registered.ID)
			}
			if waits, _ := store.ListTaskWaits(ctx); len(waits) != 2 {
				t.Fatalf("the park holds %d conditions, want 2", len(waits))
			}

			namedRun(t, store, now, "run-a", domain.ProgressFailed)
			if err := store.SettleNodeWaits(ctx, now.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			wakes, err := store.WakeTaskWaits(ctx, now.Add(2*time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			if mode == domain.WakeEach {
				if len(wakes) != 1 || len(wakes[0].Waits) != 1 || wakes[0].Waits[0].ID != ids[0] {
					t.Fatalf("any did not wake on the first run alone: %+v", wakes)
				}
				return
			}
			if len(wakes) != 0 {
				t.Fatalf("all woke the task after run-a alone: %+v", wakes)
			}
			if parked := loadAttempt(t, store, attempt.ID); parked.Progress != domain.ProgressWaitingExternal {
				t.Fatalf("the attempt left the park after one of two conditions: %q", parked.Progress)
			}

			namedRun(t, store, now, "run-b", domain.ProgressSucceeded)
			if err := store.SettleNodeWaits(ctx, now.Add(3*time.Minute)); err != nil {
				t.Fatal(err)
			}
			wakes, err = store.WakeTaskWaits(ctx, now.Add(4*time.Minute))
			if err != nil || len(wakes) != 1 || len(wakes[0].Waits) != 2 {
				t.Fatalf("all did not wake once with both conditions: %+v %v", wakes, err)
			}
			clock := now.Add(5 * time.Minute)
			runner, control := fleetRunner(t, store, &clock, nil)
			runner.Tick(ctx, nil, nil)
			if len(control.texts) != 1 {
				t.Fatalf("the task was told %d times, want once", len(control.texts))
			}
			text := control.texts[0]
			trailer, ok := wait.ParseWakeTrailer(text)
			if !ok || trailer["count"] != "2" || trailer["waits"] != ids[0]+":met,"+ids[1]+":met" {
				t.Fatalf("the trailer does not list each condition: %v", trailer)
			}
			for _, want := range []string{"run-a", "run-b", "Each condition's outcome follows"} {
				if !strings.Contains(text, want) {
					t.Fatalf("the wake does not report %q:\n%s", want, text)
				}
			}
		})
	}
}

// A request ID reused for a different condition is refused by name, never
// answered with the first wait. A retry of the same condition still replays,
// and so does one against a record written before conditions were digested,
// which refuses a different condition just the same.
func TestTaskWaitReplayOfAnotherConditionIsRefused(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	namedRun(t, store, now, "run-a", domain.ProgressActive)
	namedRun(t, store, now, "run-b", domain.ProgressActive)
	first := nodeRegistration(attempt, "park-same", domain.NodeRef{RunID: "run-a", TaskID: domain.SinkTaskName}, "")
	first.Wake = domain.WakeAll
	registered, err := store.RegisterTaskWait(ctx, first, now)
	if err != nil {
		t.Fatal(err)
	}
	if registered.ConditionDigest == "" {
		t.Fatal("the registration did not record its condition digest")
	}

	retry := nodeRegistration(attempt, "park-same", domain.NodeRef{RunID: "run-a", TaskID: domain.SinkTaskName}, "")
	retry.Wake = domain.WakeAll
	replayed, err := store.RegisterTaskWait(ctx, retry, now)
	if err != nil || replayed.ID != registered.ID {
		t.Fatalf("a retry of the same condition did not replay: %+v %v", replayed, err)
	}

	second := nodeRegistration(attempt, "park-same", domain.NodeRef{RunID: "run-b", TaskID: domain.SinkTaskName}, "")
	second.Wake = domain.WakeAll
	_, err = store.RegisterTaskWait(ctx, second, now)
	if !errors.Is(err, domain.ErrTaskWaitReplayCondition) || !errors.Is(err, domain.ErrTaskWaitReplayChanged) {
		t.Fatalf("a different condition under a reused request ID was not refused: %v", err)
	}
	for _, want := range []string{registered.ID, "park-same", "with a different one", "without --request-id"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal does not say %q: %v", want, err)
		}
	}
	if waits, _ := store.ListTaskWaits(ctx); len(waits) != 1 {
		t.Fatalf("the refused condition left %d waits", len(waits))
	}

	// A record from before the digest has none. It is compared on the
	// canonical condition it stored: the same condition replays, and run B
	// under run A's request ID is refused rather than answered with run A's
	// wait.
	legacy := registered
	legacy.ConditionDigest = ""
	saveLegacyTaskWait(t, ctx, store, legacy)
	if _, err := store.RegisterTaskWait(ctx, second, now); !errors.Is(err, domain.ErrTaskWaitReplayCondition) {
		t.Fatalf("a legacy record answered another condition: %v", err)
	} else if !strings.Contains(err.Error(), "registered before conditions were recorded") {
		t.Fatalf("the legacy refusal does not say why: %v", err)
	}
	if replayed, err := store.RegisterTaskWait(ctx, retry, now); err != nil || replayed.ID != registered.ID {
		t.Fatalf("a legacy record did not replay the same condition: %+v %v", replayed, err)
	}
	// The target is compared after resolution, as it was stored: the run's
	// sink spelled by its ID rather than its name is the same condition.
	byID := nodeRegistration(attempt, "park-same", domain.NodeRef{RunID: "run-a", TaskID: domain.SinkTaskID("run-a")}, "")
	byID.Wake = domain.WakeAll
	if replayed, err := store.RegisterTaskWait(ctx, byID, now); err != nil || replayed.ID != registered.ID {
		t.Fatalf("a legacy record did not replay the same resolved target %+v: %+v %v", legacy.Node.Target, replayed, err)
	}
	if waits, _ := store.ListTaskWaits(ctx); len(waits) != 1 {
		t.Fatalf("the legacy replays left %d waits", len(waits))
	}
}

// saveLegacyTaskWait overwrites a stored wait, as a record written before a
// field existed would read.
func saveLegacyTaskWait(t *testing.T, ctx context.Context, store *Store, legacy domain.TaskWait) {
	t.Helper()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := saveTaskWaitTx(ctx, tx, legacy); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// TestTaskWaitLegacyReplayOfALocalCondition covers a digest-less record of a
// local kind, stored exactly as it was sent: the same command replays, and a
// different one under the same request ID is refused.
func TestTaskWaitLegacyReplayOfALocalCondition(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	request := domain.TaskWaitRegistration{
		RequestID: "park-local", WorkflowRunID: attempt.WorkflowRunID, TaskID: attempt.TaskID,
		AttemptID: attempt.ID, IssuedRevision: attempt.Revision, ThreadID: attempt.ThreadID,
		Wake: domain.WakeAll, MaxDuration: time.Hour, Name: "./ready.sh a", Condition: "./ready.sh a",
	}
	registered, err := store.RegisterTaskWait(ctx, request, now)
	if err != nil {
		t.Fatal(err)
	}
	legacy := registered
	legacy.ConditionDigest = ""
	saveLegacyTaskWait(t, ctx, store, legacy)
	if replayed, err := store.RegisterTaskWait(ctx, request, now); err != nil || replayed.ID != registered.ID {
		t.Fatalf("a legacy local record did not replay the same command: %+v %v", replayed, err)
	}
	other := request
	other.Name, other.Condition = "./ready.sh b", "./ready.sh b"
	if _, err := store.RegisterTaskWait(ctx, other, now); !errors.Is(err, domain.ErrTaskWaitReplayCondition) {
		t.Fatalf("a legacy local record answered another command: %v", err)
	}
	other = request
	other.OrTimeout = true
	if _, err := store.RegisterTaskWait(ctx, other, now); !errors.Is(err, domain.ErrTaskWaitReplayCondition) {
		t.Fatalf("a legacy local record answered another deadline treatment: %v", err)
	}
}
