package wait

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A time wait polls at the backoff ceiling while the instant is far, then at
// exactly the remaining time so the last poll lands within 30 s of the
// instant, and settles met with the instant in the trailer.
func TestTimeWaitSchedulesItsPollsFromTheRemainingTime(t *testing.T) {
	store := &memStore{waits: map[string]Wait{}}
	control := &memControl{threads: map[string]*domain.Thread{"t1": {ID: "t1"}}}
	runner := New(store, control, nil)
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	runner.SetClock(func() time.Time { return now })
	runner.Exec = func(context.Context, Wait) (string, int, error) {
		t.Fatal("a time wait ran a command")
		return "", 0, nil
	}
	at := now.Add(25 * time.Minute)
	_ = store.SaveWait(context.Background(), Wait{
		ID: "w1", ThreadID: "t1", Name: "lunch", Kind: domain.WaitKindTime, At: &at,
		Every: 30 * time.Second, MaxEvery: 10 * time.Minute, Timeout: time.Hour, Wake: WakeEach,
		Status: StatusWaiting, CreatedAt: now,
	})
	ctx := context.Background()
	// First tick: not yet, next poll at the ceiling.
	runner.Tick(ctx, nil, nil)
	if w := store.waits["w1"]; w.Status != StatusWaiting || w.Runs != 1 || w.Interval != 10*time.Minute {
		t.Fatalf("after the first tick: %+v", w)
	}
	// Not due before the ceiling.
	now = now.Add(9 * time.Minute)
	runner.Tick(ctx, nil, nil)
	if store.waits["w1"].Runs != 1 {
		t.Fatal("polled before the interval")
	}
	// At 10 minutes, 15 remain: still the ceiling. At 20, 5 remain: the
	// interval is the remaining time, so the next poll is the instant.
	now = now.Add(time.Minute)
	runner.Tick(ctx, nil, nil)
	if w := store.waits["w1"]; w.Runs != 2 || w.Interval != 10*time.Minute {
		t.Fatalf("at 10 minutes: %+v", w)
	}
	now = now.Add(10 * time.Minute)
	runner.Tick(ctx, nil, nil)
	if w := store.waits["w1"]; w.Runs != 3 || w.Interval != 5*time.Minute {
		t.Fatalf("at 20 minutes the interval is not the remaining 5 minutes: %+v", w)
	}
	// One second short: still waiting.
	now = at.Add(-time.Second)
	runner.Tick(ctx, nil, nil)
	if w := store.waits["w1"]; w.Status != StatusWaiting {
		t.Fatalf("settled before the instant: %+v", w)
	}
	// The instant: met, and the wake carries it.
	now = at
	runner.Tick(ctx, nil, nil)
	runner.Tick(ctx, nil, nil)
	w := store.waits["w1"]
	if w.Status != StatusWoken || w.Outcome != "met" || w.LastExit != 0 {
		t.Fatalf("not met at the instant: %+v", w)
	}
	if len(control.texts) != 1 || !strings.HasPrefix(control.texts[0], "t3-steward-wait kind=time outcome=met wait=w1 at=2030-01-01T00:25:00Z") {
		t.Fatalf("wake = %v", control.texts)
	}
}

// The interval never drops below 30 s, and a remaining time under that still
// lands the poll within 30 s of the instant.
func TestTimeIntervalIsBoundedBelowBy30Seconds(t *testing.T) {
	for remaining, want := range map[time.Duration]time.Duration{
		5 * time.Second:  30 * time.Second,
		45 * time.Second: 45 * time.Second,
		2 * time.Hour:    10 * time.Minute,
	} {
		if got := TimeInterval(remaining, 10*time.Minute); got != want {
			t.Fatalf("TimeInterval(%s) = %s, want %s", remaining, got, want)
		}
	}
}
