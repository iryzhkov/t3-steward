package daemon

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The interactive override (S-18 remainder). The omarchy-pc sequence of
// 2026-09-17 10:11 to 10:13: the watchdog stopped the overseer session at
// 87% on a burn-rate projection, the user resumed it by hand, and every later
// turn was stopped again within a poll. A thread the user resumed or started
// after a watchdog stop is recorded in the bucket's thread notices as
// user-resumed, with the message time as evidence, and for the rest of that
// epoch no path stops or drains it and it is warned at most once.

// stoppedBucket stores the bucket as stopped at the given usage, stopped at
// stoppedAt, in a window that resets three hours later.
func (h *harness) stoppedBucket(key domain.BucketKey, used float64, stoppedAt time.Time) domain.BucketState {
	h.t.Helper()
	reset := stoppedAt.Add(3 * time.Hour)
	st := domain.BucketState{
		Key: key, Phase: domain.PhaseStopped, Epoch: domain.EpochFor(&reset), LimitName: key.String(),
		UsedPercent: used, ResetsAt: &reset, ObservedAt: stoppedAt, StoppedAt: &stoppedAt, UpdatedAt: stoppedAt,
		AppliedThresholds: &domain.ThresholdSet{WarnPercent: 85, DrainPercent: 90, StopPercent: 95},
	}
	h.storeBucket(st)
	return st
}

// userMessage marks the thread's latest user message.
func (h *harness) userMessage(id string, at time.Time) {
	h.fake.mu.Lock()
	defer h.fake.mu.Unlock()
	t := at
	h.fake.threads[id].LatestUserMessageAt = &t
}

func (h *harness) userResumedAt(key domain.BucketKey, epoch, thread string) (time.Time, bool) {
	h.t.Helper()
	notices, err := h.store.ThreadNotices(context.Background(), key, epoch, domain.NoticeUserResumed)
	if err != nil {
		h.t.Fatal(err)
	}
	at, ok := notices[thread]
	return at, ok
}

// (1) Stop at 87%, user message thirty seconds later, thread running: the
// poll leaves it alone and records it as user-resumed with the message time.
// A thread whose last user message predates the stop is held.
func TestUserResumedThreadIsNotStoppedByThePoll(t *testing.T) {
	h := newHarness(t, nil)
	stopped := h.clock
	st := h.stoppedBucket(codexPrimary, 87, stopped)
	h.fake.add("user", "codex", "gpt", true)
	h.fake.add("harness", "codex", "gpt", true)
	h.userMessage("user", stopped.Add(30*time.Second))
	h.userMessage("harness", stopped.Add(-time.Hour))
	h.clock = stopped.Add(45 * time.Second)
	h.poll()
	if fmt.Sprint(h.fake.stops) != "[interrupt:harness]" {
		t.Fatalf("stops = %v, want only the harness-started thread", h.fake.stops)
	}
	at, ok := h.userResumedAt(codexPrimary, st.Epoch, "user")
	if !ok || !at.Equal(stopped.Add(30*time.Second)) {
		t.Fatalf("user-resumed notice = %v ok=%v, want the message time", at, ok)
	}
	if _, ok := h.userResumedAt(codexPrimary, st.Epoch, "harness"); ok {
		t.Fatal("the harness-started thread was recorded as user-resumed")
	}
	// A message inside the ten-second tolerance is the watchdog's own.
	h.fake.add("tolerance", "codex", "gpt", true)
	h.userMessage("tolerance", stopped.Add(5*time.Second))
	h.poll()
	if fmt.Sprint(h.fake.stops) != "[interrupt:harness interrupt:tolerance]" {
		t.Fatalf("stops = %v", h.fake.stops)
	}
}

// (2) A reading at 96% arrives while the user-resumed thread runs: the
// reading path sends it no stop, and execute itself, handed a stop for the
// bucket, excludes it while stopping a harness-started thread beside it.
func TestUserResumedThreadIsNotStoppedByExecute(t *testing.T) {
	h := newHarness(t, nil)
	stopped := h.clock
	st := h.stoppedBucket(codexPrimary, 87, stopped)
	h.fake.add("user", "codex", "gpt", true)
	h.userMessage("user", stopped.Add(30*time.Second))
	h.clock = stopped.Add(45 * time.Second)
	h.poll()
	if len(h.fake.stops) != 0 {
		t.Fatalf("stops = %v", h.fake.stops)
	}
	// Two more user turns while the phase is stopped, then the reading.
	for i := 0; i < 2; i++ {
		h.clock = h.clock.Add(time.Minute)
		h.userMessage("user", h.clock)
		h.fake.mu.Lock()
		h.fake.threads["user"].TurnID = fmt.Sprintf("turn-%d", i)
		h.fake.mu.Unlock()
		h.poll()
	}
	h.fake.add("harness", "codex", "gpt", true)
	h.userMessage("harness", stopped.Add(-time.Hour))
	h.clock = h.clock.Add(time.Minute)
	h.snap(codexPrimary, 96, *st.ResetsAt, "96")
	h.poll()
	if fmt.Sprint(h.fake.stops) != "[interrupt:harness]" {
		t.Fatalf("stops after the 96%% reading = %v", h.fake.stops)
	}
	// The engine never re-issues a stop inside an already stopped epoch, so
	// execute is exercised directly with the stop it would carry.
	h.fake.add("late", "codex", "gpt", true)
	h.userMessage("late", stopped.Add(-time.Hour))
	current := h.loadBucket(codexPrimary)
	h.d.execute(context.Background(), domain.Action{Kind: domain.ActionStop, Bucket: codexPrimary, Snapshot: snapshotFromState(current), Reason: "96%"}, current)
	if fmt.Sprint(h.fake.stops) != "[interrupt:harness interrupt:late]" {
		t.Fatalf("stops after execute = %v", h.fake.stops)
	}
	h.d.execute(context.Background(), domain.Action{Kind: domain.ActionDrain, Bucket: codexPrimary, Snapshot: snapshotFromState(current), Reason: "drain"}, current)
	for _, w := range h.fake.warnings {
		if w == "drain:user" {
			t.Fatalf("the user-resumed thread was drained: %v", h.fake.warnings)
		}
	}
}

// (3) A drain grace expiry stops the harness-started thread and not the
// thread recorded as user-resumed in this epoch.
func TestUserResumedThreadIsNotStoppedByTheGraceTimer(t *testing.T) {
	h := newHarness(t, nil)
	reset := h.clock.Add(3 * time.Hour)
	deadline := h.clock.Add(-time.Second)
	st := domain.BucketState{
		Key: codexPrimary, Phase: domain.PhaseDraining, Epoch: domain.EpochFor(&reset), LimitName: "codex",
		UsedPercent: 96, ResetsAt: &reset, ObservedAt: h.clock.Add(-2 * time.Minute), DrainDeadline: &deadline,
	}
	h.storeBucket(st)
	h.fake.add("user", "codex", "gpt", true)
	h.fake.add("harness", "codex", "gpt", true)
	if _, err := h.store.MarkThreadNotice(context.Background(), "user", codexPrimary, st.Epoch, domain.NoticeUserResumed, h.clock.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	h.d.Tick(context.Background())
	if fmt.Sprint(h.fake.stops) != "[interrupt:harness]" {
		t.Fatalf("stops after the grace expiry = %v", h.fake.stops)
	}
}

// (4) A user-resumed thread is warned at most once in the window: the
// draining poll sends it the warning once and never the drain request, and
// a later warn action adds nothing.
func TestUserResumedThreadIsWarnedAtMostOnce(t *testing.T) {
	h := newHarness(t, nil)
	reset := h.clock.Add(3 * time.Hour)
	deadline := h.clock.Add(time.Minute)
	st := domain.BucketState{
		Key: codexPrimary, Phase: domain.PhaseDraining, Epoch: domain.EpochFor(&reset), LimitName: "codex",
		UsedPercent: 91, ResetsAt: &reset, ObservedAt: h.clock, DrainDeadline: &deadline,
	}
	h.storeBucket(st)
	h.fake.add("user", "codex", "gpt", true)
	h.fake.add("harness", "codex", "gpt", true)
	if _, err := h.store.MarkThreadNotice(context.Background(), "user", codexPrimary, st.Epoch, domain.NoticeUserResumed, h.clock.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	h.poll()
	h.poll()
	h.d.execute(context.Background(), domain.Action{Kind: domain.ActionWarn, Bucket: codexPrimary, Snapshot: snapshotFromState(st), Reason: "warn"}, st)
	h.d.execute(context.Background(), domain.Action{Kind: domain.ActionDrain, Bucket: codexPrimary, Snapshot: snapshotFromState(st), Reason: "drain"}, st)
	userWarnings, harnessDrains := 0, 0
	for _, w := range h.fake.warnings {
		switch w {
		case "warn:user":
			userWarnings++
		case "drain:user":
			t.Fatalf("the user-resumed thread was drained: %v", h.fake.warnings)
		case "drain:harness":
			harnessDrains++
		}
	}
	if userWarnings != 1 || harnessDrains != 1 {
		t.Fatalf("warnings = %v, want one warning for the user thread and one drain for the harness thread", h.fake.warnings)
	}
	if len(h.fake.stops) != 0 {
		t.Fatalf("stops = %v", h.fake.stops)
	}
}

// (5) An attempt-owned thread with a newer user message is the worker's:
// never warned, never stopped, and never recorded as user-resumed.
func TestOwnedThreadWithNewerUserMessageIsLeftToTheWorker(t *testing.T) {
	h := newHarness(t, nil)
	h.d.Ownership = &fakeOwnership{owned: map[string]string{"owned": "attempt-1"}}
	stopped := h.clock
	st := h.stoppedBucket(codexPrimary, 87, stopped)
	h.fake.add("owned", "codex", "gpt", true)
	h.userMessage("owned", stopped.Add(30*time.Second))
	h.clock = stopped.Add(45 * time.Second)
	h.poll()
	current := h.loadBucket(codexPrimary)
	h.d.execute(context.Background(), domain.Action{Kind: domain.ActionStop, Bucket: codexPrimary, Snapshot: snapshotFromState(current), Reason: "stop"}, current)
	h.d.execute(context.Background(), domain.Action{Kind: domain.ActionWarn, Bucket: codexPrimary, Snapshot: snapshotFromState(current), Reason: "warn"}, current)
	if len(h.fake.stops) != 0 || len(h.fake.warnings) != 0 {
		t.Fatalf("owned thread touched: stops=%v warnings=%v", h.fake.stops, h.fake.warnings)
	}
	if _, ok := h.userResumedAt(codexPrimary, st.Epoch, "owned"); ok {
		t.Fatal("an owned thread was recorded as user-resumed")
	}
}

// The exemption ends with the epoch: a rearm clears the notices, and a stop
// in the next window holds the thread again until the user acts.
func TestUserResumedExemptionEndsWithTheEpoch(t *testing.T) {
	h := newHarness(t, nil)
	stopped := h.clock
	st := h.stoppedBucket(codexPrimary, 87, stopped)
	h.fake.add("user", "codex", "gpt", true)
	h.userMessage("user", stopped.Add(30*time.Second))
	h.clock = stopped.Add(45 * time.Second)
	h.poll()
	if _, ok := h.userResumedAt(codexPrimary, st.Epoch, "user"); !ok {
		t.Fatal("not recorded")
	}
	// The window resets and a reading confirms it; later the bucket stops
	// again in the new window.
	h.clock = st.ResetsAt.Add(time.Minute)
	nextReset := h.clock.Add(5 * time.Hour)
	h.snap(codexPrimary, 2, nextReset, "reset")
	if _, ok := h.userResumedAt(codexPrimary, st.Epoch, "user"); ok {
		t.Fatal("the rearm did not clear the notice")
	}
	h.clock = h.clock.Add(10 * time.Minute)
	h.snap(codexPrimary, 96, nextReset, "stop-again")
	if fmt.Sprint(h.fake.stops) != "[interrupt:user]" {
		t.Fatalf("stops in the next window = %v", h.fake.stops)
	}
}
