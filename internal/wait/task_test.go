package wait

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// taskMemStore is memStore plus the coordinator's task-bound wait surface. It
// records what the runner asked the coordinator to do, which is the point of
// the seam: the runner never resumes a parked attempt itself.
type taskMemStore struct {
	*memStore
	taskWaits map[string]domain.TaskWait
	attempts  map[string]domain.Attempt
	settled   []string
	expired   int
	wakes     int
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

// WakeTaskWaits mirrors the store: a parked attempt resumes, an attempt that is
// already running is told anyway, and a terminal one is abandoned.
func (s *taskMemStore) WakeTaskWaits(_ context.Context, now time.Time) ([]domain.TaskWaitWakeContext, error) {
	s.wakes++
	var woken []domain.TaskWaitWakeContext
	for id, wait := range s.taskWaits {
		if wait.SettledAt == nil || wait.WokenAt != nil {
			continue
		}
		attempt := s.attempts[wait.AttemptID]
		if attempt.Progress.Terminal() {
			at := now
			wait.WokenAt, wait.Delivery = &at, "abandoned"
			s.taskWaits[id] = wait
			continue
		}
		resumption := attempt.Progress == domain.ProgressWaitingExternal
		if resumption {
			attempt.Revision++
			attempt.Progress = domain.ProgressActive
			attempt.Control = domain.ControlResuming
			s.attempts[wait.AttemptID] = attempt
		}
		at := now
		wait.WokenAt = &at
		wait.Delivery = "pending"
		wait.Resumption = resumption
		wait.WakeRevision = attempt.Revision
		wait.DeliveryID = "task-wake:" + wait.ID
		s.taskWaits[id] = wait
		woken = append(woken, domain.TaskWaitWakeContext{
			AttemptID: wait.AttemptID, ThreadID: wait.ThreadID,
			AttemptRevision: attempt.Revision, Waits: []domain.TaskWait{wait},
		})
	}
	return woken, nil
}

func (s *taskMemStore) TaskWakesAwaitingDelivery(context.Context, time.Time) ([]domain.TaskWaitWakeContext, error) {
	var pending []domain.TaskWaitWakeContext
	for _, wait := range s.taskWaits {
		if wait.WokenAt == nil || wait.Delivery == "delivered" || wait.Delivery == "abandoned" {
			continue
		}
		pending = append(pending, domain.TaskWaitWakeContext{
			AttemptID: wait.AttemptID, ThreadID: wait.ThreadID,
			AttemptRevision: wait.WakeRevision, Waits: []domain.TaskWait{wait},
		})
	}
	return pending, nil
}

func (s *taskMemStore) TransitionTaskWake(_ context.Context, id, from, to string, now time.Time) (bool, error) {
	wait, ok := s.taskWaits[id]
	if !ok || wait.Delivery != from {
		return false, nil
	}
	wait.Delivery = to
	if to == "delivered" {
		at := now
		wait.DeliveredAt = &at
	}
	s.taskWaits[id] = wait
	return true, nil
}

// taskControl is memControl plus the observable-delivery seam node waits use.
type taskControl struct {
	*memControl
	sends     []string
	sendErr   error
	observed  map[string]bool
	observeOK bool
}

func (c *taskControl) SendNodeWake(_ context.Context, thread domain.Thread, messageID, text string) error {
	c.sends = append(c.sends, messageID)
	c.texts = append(c.texts, text)
	c.resumed = append(c.resumed, thread.ID)
	if c.sendErr != nil {
		return c.sendErr
	}
	if c.observed == nil {
		c.observed = map[string]bool{}
	}
	c.observed[messageID] = true
	return nil
}

func (c *taskControl) ObserveNodeWake(_ context.Context, _ string, messageID string) (bool, error) {
	if !c.observeOK {
		return false, errors.New("observation unavailable")
	}
	return c.observed[messageID], nil
}

func healthyBuckets() []domain.BucketState {
	return []domain.BucketState{{
		Key:   domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"},
		Phase: domain.PhaseNormal, Healthy: true,
	}}
}

func taskWaitRunner(t *testing.T, progress domain.ProgressState) (*Runner, *taskMemStore, *taskControl, *time.Time) {
	t.Helper()
	store := &taskMemStore{
		memStore:  &memStore{waits: map[string]Wait{}},
		taskWaits: map[string]domain.TaskWait{},
		attempts:  map[string]domain.Attempt{},
	}
	control := &taskControl{
		memControl: &memControl{threads: map[string]*domain.Thread{"t1": {ID: "t1", ProviderInstanceID: "claudeAgent"}}},
		observeOK:  true,
	}
	runner := New(store, control, nil)
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	runner.SetClock(func() time.Time { return now })
	runner.Exec = func(context.Context, Wait) (string, int, error) { return "completed", 0, nil }
	store.attempts["a1"] = domain.Attempt{ID: "a1", Revision: 4, Progress: progress, ThreadID: "t1"}
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
	return runner, store, control, &now
}

// A task-bound check settles into the coordinator and is never woken by the
// interactive path. Resuming a parked task also resumes its attempt and
// reacquires its capacity, and only the coordinator can do that.
func TestTaskBoundWaitSettlesThroughTheCoordinatorAndIsNotWokenLocally(t *testing.T) {
	runner, store, control, now := taskWaitRunner(t, domain.ProgressWaitingExternal)
	*now = now.Add(time.Minute)
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
	if len(control.sends) != 1 || len(control.resumed) != 1 {
		t.Fatalf("the parked task was not resumed exactly once: %v", control.sends)
	}
	if !strings.Contains(control.texts[0], "waking this task") || !strings.Contains(control.texts[0], "met") {
		t.Fatalf("the resumed turn was given no usable evidence: %q", control.texts[0])
	}
	if store.taskWaits["tw-1"].Delivery != "delivered" {
		t.Fatalf("delivery was not recorded: %q", store.taskWaits["tw-1"].Delivery)
	}

	*now = now.Add(time.Hour)
	runner.Tick(context.Background(), nil, healthyBuckets())
	runner.Tick(context.Background(), nil, healthyBuckets())
	if len(store.settled) != 1 || len(control.sends) != 1 {
		t.Fatalf("repeated ticks settled or sent again: settled=%v sends=%v", store.settled, control.sends)
	}
}

// A lost response must not become a second turn. The send is claimed durably
// first and resolved only by observing its own message identity, so a retry
// sends nothing and the wake is delivered exactly once.
func TestTaskWakeIsDeliveredExactlyOnceAcrossALostResponse(t *testing.T) {
	runner, store, control, now := taskWaitRunner(t, domain.ProgressWaitingExternal)
	control.sendErr = errors.New("connection reset after dispatch")
	*now = now.Add(time.Minute)
	runner.Tick(context.Background(), nil, healthyBuckets())
	if len(control.sends) != 1 {
		t.Fatalf("sends = %v, want one attempt", control.sends)
	}
	if got := store.taskWaits["tw-1"].Delivery; got != "recovery-required" {
		t.Fatalf("an uncertain send was recorded as %q", got)
	}

	// The message did land. Observation resolves it; nothing is sent again.
	control.sendErr = nil
	control.observed = map[string]bool{"task-wake:tw-1": true}
	*now = now.Add(time.Minute)
	runner.Tick(context.Background(), nil, healthyBuckets())
	if len(control.sends) != 1 {
		t.Fatalf("a second send started another turn: %v", control.sends)
	}
	if got := store.taskWaits["tw-1"].Delivery; got != "delivered" {
		t.Fatalf("observed delivery recorded as %q", got)
	}
}

// Absence of the message is not proof it never arrived, so an unobservable
// wake stays recovery-required and is never re-sent.
func TestUnobservedTaskWakeStaysRecoveryRequiredAndIsNeverResent(t *testing.T) {
	runner, store, control, now := taskWaitRunner(t, domain.ProgressWaitingExternal)
	control.sendErr = errors.New("lost response")
	*now = now.Add(time.Minute)
	runner.Tick(context.Background(), nil, healthyBuckets())
	control.sendErr = nil
	control.observed = map[string]bool{}
	for pass := 0; pass < 3; pass++ {
		*now = now.Add(time.Minute)
		runner.Tick(context.Background(), nil, healthyBuckets())
	}
	if len(control.sends) != 1 {
		t.Fatalf("an unresolved wake was re-sent: %v", control.sends)
	}
	if got := store.taskWaits["tw-1"].Delivery; got != "recovery-required" {
		t.Fatalf("delivery = %q, want recovery-required until observed", got)
	}
}

// A dry run must not resume anything. Waking is a workflow mutation, and a
// flag meant to change nothing would otherwise un-park every task on the fleet.
func TestDryRunHoldsTheWakeInsteadOfResumingParkedAttempts(t *testing.T) {
	runner, store, control, now := taskWaitRunner(t, domain.ProgressWaitingExternal)
	runner.DryRun = true
	*now = now.Add(time.Minute)
	runner.Tick(context.Background(), nil, healthyBuckets())
	if store.wakes != 0 {
		t.Fatal("a dry run resumed parked attempts")
	}
	if len(control.sends) != 0 {
		t.Fatalf("a dry run delivered wakes: %v", control.sends)
	}
	if attempt := store.attempts["a1"]; attempt.Progress != domain.ProgressWaitingExternal {
		t.Fatalf("a dry run un-parked the attempt: %q", attempt.Progress)
	}
	if store.taskWaits["tw-1"].WokenAt != nil {
		t.Fatal("a dry run marked the wait woken")
	}
	// The outcome is still observed and recorded: only the mutation is held.
	if len(store.settled) != 1 {
		t.Fatalf("a dry run suppressed settlement: %v", store.settled)
	}
	// Turning it off performs the held intent.
	runner.DryRun = false
	*now = now.Add(time.Minute)
	runner.Tick(context.Background(), nil, healthyBuckets())
	if store.wakes == 0 || len(control.sends) != 1 {
		t.Fatalf("the held wake was not performed: wakes=%d sends=%v", store.wakes, control.sends)
	}
}

// A wait that settles while its attempt is already running still has to reach
// the agent. Marking it woken and dropping it is the silence this design
// refuses.
func TestSettlementOnARunningAttemptIsStillDelivered(t *testing.T) {
	runner, store, control, now := taskWaitRunner(t, domain.ProgressActive)
	*now = now.Add(time.Minute)
	runner.Tick(context.Background(), nil, healthyBuckets())
	if len(control.sends) != 1 {
		t.Fatalf("the settlement was not delivered to the running turn: %v", control.sends)
	}
	if !strings.Contains(control.texts[0], "already running") {
		t.Fatalf("the message does not say the turn was already running: %q", control.texts[0])
	}
	if !strings.Contains(control.texts[0], "met") {
		t.Fatalf("the outcome was not delivered: %q", control.texts[0])
	}
	if attempt := store.attempts["a1"]; attempt.Revision != 4 {
		t.Fatalf("delivering evidence moved the running attempt: revision=%d", attempt.Revision)
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
