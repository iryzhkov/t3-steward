package wait

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// listingTaskMemStore is taskMemStore with the coordinator's list surface,
// which is how a worker learns that a wait it still polls for was settled
// elsewhere: by a cancellation, by the coordinator's own settlement pass.
type listingTaskMemStore struct {
	*taskMemStore
	lists int
}

func (s *listingTaskMemStore) ListTaskWaits(context.Context) ([]domain.TaskWait, error) {
	s.lists++
	var waits []domain.TaskWait
	for _, wait := range s.taskWaits {
		waits = append(waits, wait)
	}
	return waits, nil
}

// U-4, worker side: the check row bound to a coordinator wait that was
// settled without the check (cancelled here) is cancelled on the worker's
// next reconcile instead of polling until its own deadline.
func TestWorkerCancelsTheCheckRowOfAWaitSettledElsewhere(t *testing.T) {
	runner, base, _, now := taskWaitRunner(t, domain.ProgressWaitingExternal)
	store := &listingTaskMemStore{taskMemStore: base}
	runner.TaskStore = store
	runner.Exec = func(context.Context, Wait) (string, int, error) { return "not yet", 1, nil }
	// The coordinator cancelled the task: the wait is settled as cancelled and
	// the attempt is terminal.
	settled := *now
	wait := store.taskWaits["tw-1"]
	wait.Result = &domain.TaskWaitResult{Outcome: domain.TaskWaitCancelled, ExitCode: 2, Reason: "the task was cancelled"}
	wait.SettledAt = &settled
	store.taskWaits["tw-1"] = wait
	attempt := store.attempts["a1"]
	attempt.Progress, attempt.Control = domain.ProgressCancelled, domain.ControlStopped
	store.attempts["a1"] = attempt

	*now = now.Add(time.Minute)
	runner.Tick(context.Background(), nil, healthyBuckets())
	row := store.waits["w1"]
	if row.Status != StatusCancelled || !strings.Contains(row.Reason, "cancelled") {
		t.Fatalf("the local check kept polling for a settled wait: %+v", row)
	}
	if store.lists == 0 {
		t.Fatal("the worker never asked the coordinator about its bound waits")
	}
	// The runner told the coordinator nothing: the outcome already stood.
	if len(store.settled) != 0 {
		t.Fatalf("the worker re-settled a settled wait: %v", store.settled)
	}
	// Later ticks do not list again for a row that is no longer waiting.
	lists := store.lists
	*now = now.Add(time.Hour)
	runner.Tick(context.Background(), nil, healthyBuckets())
	if store.lists != lists {
		t.Fatal("the worker keeps listing coordinator waits with no live bound row")
	}
}
