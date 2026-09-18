package sqlite

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// fleetControl is the thread control seam the wait runner drives, recorded
// rather than mocked away: what reached a thread is the only evidence that a
// parked attempt was actually resumed.
type fleetControl struct {
	threads  map[string]*domain.Thread
	sends    []string
	texts    []string
	resumed  []string
	observed map[string]bool
}

func (c *fleetControl) GetThread(_ context.Context, id string) (*domain.Thread, error) {
	return c.threads[id], nil
}

func (c *fleetControl) ResumeThread(_ context.Context, thread domain.Thread, text string) error {
	c.resumed = append(c.resumed, thread.ID)
	c.texts = append(c.texts, text)
	return nil
}

func (c *fleetControl) SendNodeWake(_ context.Context, thread domain.Thread, messageID, text string) error {
	c.sends = append(c.sends, messageID)
	c.texts = append(c.texts, text)
	c.resumed = append(c.resumed, thread.ID)
	if c.observed == nil {
		c.observed = map[string]bool{}
	}
	c.observed[messageID] = true
	return nil
}

func (c *fleetControl) ObserveNodeWake(_ context.Context, _, messageID string) (bool, error) {
	return c.observed[messageID], nil
}

// fleetRunner wires the real wait runner to the real store, which is the
// composition the fleet runs. The runner owns the local checks, the store owns
// the coordinator records, and the defect this file reproduces lives in how the
// first hands settlements to the second.
func fleetRunner(t *testing.T, store *Store, now *time.Time, exit map[string]int) (*wait.Runner, *fleetControl) {
	t.Helper()
	control := &fleetControl{threads: map[string]*domain.Thread{
		"thread-1": {ID: "thread-1", ProviderInstanceID: "claudeAgent"},
	}}
	runner := wait.New(store, control, nil)
	runner.SetClock(func() time.Time { return *now })
	runner.DisableQuotaChecks = true
	runner.Exec = func(_ context.Context, w wait.Wait) (string, int, error) {
		code, ok := exit[w.ID]
		if !ok {
			code = 1
		}
		return "output of " + w.ID, code, nil
	}
	return runner, control
}

// saveLocalCheck stores the local shell check that polls one registered
// task-bound wait, exactly as `t3-steward wait add --task current` leaves it:
// bound to the coordinator record, already run once, not yet settled.
func saveLocalCheck(t *testing.T, store *Store, id, taskWaitID string, wake wait.WakeMode, now time.Time) {
	t.Helper()
	started := now
	if err := store.SaveWait(context.Background(), wait.Wait{
		ID: id, ThreadID: "thread-1", Name: id, Command: []string{"check", id},
		TaskWaitID: taskWaitID, Every: 30 * time.Second, MaxEvery: time.Minute,
		Timeout: time.Hour, RunTimeout: time.Minute, Wake: wake,
		Status: wait.StatusWaiting, CreatedAt: now, LastRunAt: &started, Runs: 1, LastExit: 1,
	}); err != nil {
		t.Fatal(err)
	}
}

// F6: two each waits on one attempt, registered one after the other so the
// second parks an already-parked attempt. One check settles. The attempt must
// resume, because each means each.
//
// The fleet observed the opposite: the local check reported met, and the
// attempt stayed in waiting-external at the revision the second registration
// left it at, for as long as it was sampled.
func TestTwoEachWaitsWakeTheAttemptOnTheFirstSettlement(t *testing.T) {
	for _, settlement := range []struct {
		check   string
		exit    int
		local   wait.Status
		outcome domain.TaskWaitOutcome
	}{
		{check: "w-first", exit: 0, local: wait.StatusMet, outcome: domain.TaskWaitMet},
		{check: "w-second", exit: 0, local: wait.StatusMet, outcome: domain.TaskWaitMet},
		// A check that gives up settles too. A failed condition wakes the task
		// with its evidence; only silence would be wrong.
		{check: "w-second", exit: 2, local: wait.StatusGaveUp, outcome: domain.TaskWaitGaveUp},
	} {
		settles := settlement.check
		t.Run(fmt.Sprintf("%s settles as %s", settles, settlement.outcome), func(t *testing.T) {
			ctx := context.Background()
			store, attempt, now := taskWaitFixture(t)
			first, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "req-1", domain.WakeEach), now)
			if err != nil {
				t.Fatal(err)
			}
			saveLocalCheck(t, store, "w-first", first.ID, wait.WakeEach, now)

			parked := loadAttempt(t, store, attempt.ID)
			second, err := store.RegisterTaskWait(ctx, taskWaitRegistration(parked, "req-2", domain.WakeEach), now)
			if err != nil {
				t.Fatal(err)
			}
			saveLocalCheck(t, store, "w-second", second.ID, wait.WakeEach, now)

			bound := map[string]string{"w-first": first.ID, "w-second": second.ID}
			exit := map[string]int{"w-first": 1, "w-second": 1}
			exit[settles] = settlement.exit
			clock := now
			runner, control := fleetRunner(t, store, &clock, exit)
			clock = clock.Add(2 * time.Minute)
			runner.Tick(ctx, nil, nil)

			waits, err := store.ListWaits(ctx, "")
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range waits {
				if w.ID == settles && w.Status != settlement.local {
					t.Fatalf("the local check settled as %q, want %q", w.Status, settlement.local)
				}
			}
			records, err := store.ListTaskWaits(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, record := range records {
				if record.Wake != domain.WakeEach {
					t.Fatalf("the coordinator stored wake mode %q for an each registration", record.Wake)
				}
				if record.ID == bound[settles] && !record.Settled() {
					t.Fatal("the settled check was never carried into the coordinator record")
				}
				if record.ID == bound[settles] && record.Result.Outcome != settlement.outcome {
					t.Fatalf("the coordinator recorded outcome %q, want %q", record.Result.Outcome, settlement.outcome)
				}
				if record.ID != bound[settles] && (record.Settled() || record.Woken()) {
					t.Fatal("the live wait was settled or woken by the other one")
				}
			}
			resumed := loadAttempt(t, store, attempt.ID)
			if resumed.Progress != domain.ProgressActive || resumed.Control != domain.ControlResuming {
				t.Fatalf("the attempt did not wake on an each settlement: %q/%q at revision %d",
					resumed.Progress, resumed.Control, resumed.Revision)
			}
			if len(control.sends) != 1 {
				t.Fatalf("the resumed turn was not told exactly once: %v", control.sends)
			}
			// The resumed attempt is no longer parked, and everything that asks
			// whether it is must say so. A live wait is not a park: the fleet read
			// it as one and put the attempt straight back into waiting-external
			// the moment the resumed turn ended.
			stillParked, err := store.LiveTaskWaitAttempts(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if waitID, ok := stillParked[attempt.ID]; ok {
				t.Fatalf("the resumed attempt is still reported parked on %q", waitID)
			}
		})
	}
}
