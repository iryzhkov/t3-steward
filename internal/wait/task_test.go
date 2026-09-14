package wait

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// taskMemStore is memStore plus the coordinator's task-bound wait surface. It
// records what the runner asked the coordinator to do, which is the whole point
// of the seam: the runner never resumes a parked attempt itself.
type taskMemStore struct {
	*memStore
	taskWaits map[string]domain.TaskWait
	settled   []string
	expired   int
	wakes     int
	delivered []string
	pending   []domain.TaskWaitWakeContext
}

func (s *taskMemStore) SettleTaskWait(_ context.Context, id string, result domain.TaskWaitResult, now time.Time) (domain.TaskWait, error) {
	wait := s.taskWaits[id]
	if wait.SettledAt != nil {
		return wait, nil
	}
	s.settled = append(s.settled, id+":"+string(result.Outcome))
	settled := now
	wait.Result = &result
	wait.SettledAt = &settled
	s.taskWaits[id] = wait
	return wait, nil
}

func (s *taskMemStore) ExpireTaskWaits(context.Context, time.Time) ([]domain.TaskWait, error) {
	s.expired++
	return nil, nil
}

func (s *taskMemStore) WakeTaskWaits(_ context.Context, now time.Time) ([]domain.TaskWaitWakeContext, error) {
	s.wakes++
	var woken []domain.TaskWaitWakeContext
	for id, wait := range s.taskWaits {
		if wait.SettledAt == nil || wait.WokenAt != nil {
			continue
		}
		at := now
		wait.WokenAt = &at
		wait.Delivery = "pending"
		s.taskWaits[id] = wait
		woken = append(woken, domain.TaskWaitWakeContext{AttemptID: wait.AttemptID, ThreadID: wait.ThreadID, Waits: []domain.TaskWait{wait}})
	}
	s.pending = append(s.pending, woken...)
	return woken, nil
}

func (s *taskMemStore) PendingTaskWakes(context.Context) ([]domain.TaskWaitWakeContext, error) {
	return s.pending, nil
}

func (s *taskMemStore) MarkTaskWaitDelivered(_ context.Context, id string, _ time.Time) error {
	s.delivered = append(s.delivered, id)
	s.pending = nil
	return nil
}

func healthyBuckets() []domain.BucketState {
	return []domain.BucketState{{
		Key:   domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"},
		Phase: domain.PhaseNormal, Healthy: true,
	}}
}

// A task-bound check settles into the coordinator and is never woken by the
// interactive path. Resuming a parked task also resumes its attempt and
// reacquires its capacity, and only the coordinator can do that.
func TestTaskBoundWaitSettlesThroughTheCoordinatorAndIsNotWokenLocally(t *testing.T) {
	store := &taskMemStore{memStore: &memStore{waits: map[string]Wait{}}, taskWaits: map[string]domain.TaskWait{}}
	control := &memControl{threads: map[string]*domain.Thread{"t1": {ID: "t1", ProviderInstanceID: "claudeAgent"}}}
	runner := New(store, control, nil)
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	runner.SetClock(func() time.Time { return now })
	runner.Exec = func(context.Context, Wait) (string, int, error) { return "completed", 0, nil }

	store.taskWaits["tw-1"] = domain.TaskWait{
		ID: "tw-1", AttemptID: "a1", ThreadID: "t1", Wake: domain.WakeEach,
		Name: "ci", RegisteredAt: now, Deadline: now.Add(time.Hour),
	}
	started := now
	_ = store.SaveWait(context.Background(), Wait{
		ID: "w1", ThreadID: "t1", Name: "ci", Command: []string{"check"}, TaskWaitID: "tw-1",
		Every: 30 * time.Second, MaxEvery: time.Minute, Timeout: time.Hour, Wake: WakeEach,
		Status: StatusWaiting, CreatedAt: now, LastRunAt: &started, Runs: 1, LastExit: 1,
	})

	now = now.Add(time.Minute)
	runner.Tick(context.Background(), nil, healthyBuckets())

	if got := store.waits["w1"].Status; got != StatusMet {
		t.Fatalf("the check did not settle: %q", got)
	}
	if len(store.settled) != 1 || store.settled[0] != "tw-1:met" {
		t.Fatalf("the coordinator was not told the outcome: %v", store.settled)
	}
	if store.expired == 0 || store.wakes == 0 {
		t.Fatalf("the coordinator tick did not run: expired=%d wakes=%d", store.expired, store.wakes)
	}
	if store.waits["w1"].Status == StatusWoken {
		t.Fatal("the interactive path woke a task-bound wait")
	}
	if len(control.resumed) != 1 || control.resumed[0] != "t1" {
		t.Fatalf("the parked task was not resumed exactly once: %v", control.resumed)
	}
	if !strings.Contains(control.texts[0], "waking this task") || !strings.Contains(control.texts[0], "met") {
		t.Fatalf("the resumed turn was given no usable evidence: %q", control.texts[0])
	}
	if len(store.delivered) != 1 {
		t.Fatalf("delivery was not recorded once: %v", store.delivered)
	}

	// Further ticks settle nothing again and resume nothing again.
	now = now.Add(time.Hour)
	runner.Tick(context.Background(), nil, healthyBuckets())
	runner.Tick(context.Background(), nil, healthyBuckets())
	if len(store.settled) != 1 || len(control.resumed) != 1 {
		t.Fatalf("repeated ticks settled or resumed again: settled=%v resumed=%v", store.settled, control.resumed)
	}
}

// An interactive wait on a store with no task-bound surface behaves exactly as
// before: it wakes its thread and touches no workflow state.
func TestInteractiveWaitIsUnaffectedByTaskBoundWaits(t *testing.T) {
	store := &memStore{waits: map[string]Wait{}}
	control := &memControl{threads: map[string]*domain.Thread{"t1": {ID: "t1", ProviderInstanceID: "claudeAgent"}}}
	runner := New(store, control, nil)
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	runner.SetClock(func() time.Time { return now })
	runner.Exec = func(context.Context, Wait) (string, int, error) { return "ready", 0, nil }
	started := now
	_ = store.SaveWait(context.Background(), Wait{
		ID: "w1", ThreadID: "t1", Name: "deploy", Command: []string{"check"},
		Every: 30 * time.Second, MaxEvery: time.Minute, Timeout: time.Hour, Wake: WakeEach,
		Status: StatusWaiting, CreatedAt: now, LastRunAt: &started, Runs: 1, LastExit: 1,
	})
	now = now.Add(time.Minute)
	runner.Tick(context.Background(), nil, healthyBuckets())
	runner.Tick(context.Background(), nil, healthyBuckets())
	if len(control.resumed) != 1 || store.waits["w1"].Status != StatusWoken {
		t.Fatalf("resumed=%v status=%q", control.resumed, store.waits["w1"].Status)
	}
	if store.waits["w1"].TaskWaitID != "" {
		t.Fatal("an interactive wait acquired a task binding")
	}
}
