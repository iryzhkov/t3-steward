package daemon

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type fakeOwnership struct {
	owned map[string]string
	err   error
	calls int
}

func (f *fakeOwnership) OwnedThreads(context.Context) (map[string]string, error) {
	f.calls++
	return f.owned, f.err
}

// The S-16 incident: the watchdog stopped the threads of campaign tasks at the
// quota threshold, the coordinator cancelled the run, and after the reset the
// watchdog resumed every one of those threads with no task owning them. An
// intent for a thread a steward attempt owns is cancelled on the first tick
// that sees the ownership, whether it was recorded before or after, and the
// thread is never resumed by the watchdog.
func TestOwnedThreadIntentIsCancelledNeverResumed(t *testing.T) {
	h := newHarness(t, nil)
	h.fake.add("a", "codex", "gpt", true)
	reset := h.clock.Add(5 * time.Hour)
	h.snap(codexPrimary, 96, reset, "1")
	if fmt.Sprint(h.fake.stops) != "[interrupt:a]" {
		t.Fatalf("stops = %v", h.fake.stops)
	}
	intent, ok, _ := h.store.LoadResumeIntent(context.Background(), "a")
	if !ok || intent.Status != domain.ResumePending {
		t.Fatalf("intent = %+v ok=%v", intent, ok)
	}
	// The ownership becomes visible after the intent was recorded, as it
	// does for every intent written before the upgrade.
	ownership := &fakeOwnership{owned: map[string]string{"a": "attempt-1"}}
	h.d.Ownership = ownership
	h.clock = reset.Add(time.Minute)
	h.snap(codexPrimary, 1, reset.Add(5*time.Hour), "2")
	h.clock = h.clock.Add(3 * time.Minute)
	h.poll()
	if len(h.fake.resumes) != 0 {
		t.Fatalf("an owned thread was resumed by the watchdog: %v", h.fake.resumes)
	}
	intent, _, _ = h.store.LoadResumeIntent(context.Background(), "a")
	if intent.Status != domain.ResumeCancelled || intent.Reason != OwnedThreadReason("attempt-1") {
		t.Fatalf("intent = %+v", intent)
	}
	if ownership.calls == 0 {
		t.Fatal("ownership was never consulted")
	}
	// Later polls do not resurrect the cancelled intent.
	h.clock = h.clock.Add(time.Minute)
	h.poll()
	if len(h.fake.resumes) != 0 {
		t.Fatalf("resumes = %v", h.fake.resumes)
	}
}

// A thread owned by a live attempt is excluded from warn, drain and stop; an
// unowned thread on the same bucket is handled as before, and only it gets a
// resume intent.
func TestOwnedThreadsAreExcludedFromWatchdogActions(t *testing.T) {
	h := newHarness(t, nil)
	h.d.Ownership = &fakeOwnership{owned: map[string]string{"owned": "attempt-1"}}
	h.fake.add("owned", "codex", "gpt", true)
	h.fake.add("free", "codex", "gpt", true)
	reset := h.clock.Add(5 * time.Hour)
	h.snap(codexPrimary, 86, reset, "1")
	if fmt.Sprint(h.fake.warnings) != "[warn:free]" {
		t.Fatalf("warnings = %v", h.fake.warnings)
	}
	h.advance(10 * time.Minute)
	h.snap(codexPrimary, 91, reset, "2")
	if fmt.Sprint(h.fake.warnings) != "[warn:free drain:free]" {
		t.Fatalf("warnings = %v", h.fake.warnings)
	}
	h.advance(10 * time.Minute)
	h.snap(codexPrimary, 96, reset, "3")
	if fmt.Sprint(h.fake.stops) != "[interrupt:free]" {
		t.Fatalf("stops = %v", h.fake.stops)
	}
	// The poll path stops newly running threads in a stopped bucket; the
	// owned one is still running and still left alone.
	h.poll()
	h.poll()
	if fmt.Sprint(h.fake.stops) != "[interrupt:free]" {
		t.Fatalf("stops after poll = %v", h.fake.stops)
	}
	if _, ok, _ := h.store.LoadResumeIntent(context.Background(), "owned"); ok {
		t.Fatal("an owned thread received a resume intent")
	}
	if intent, ok, _ := h.store.LoadResumeIntent(context.Background(), "free"); !ok || intent.Status != domain.ResumePending {
		t.Fatalf("free intent = %+v ok=%v", intent, ok)
	}
}

// An unreadable ownership source degrades to today's behaviour, never to
// "everything is owned": the thread is stopped and the failure is logged, not
// used as a reason to stand down.
func TestOwnershipReadFailureDegradesToUnowned(t *testing.T) {
	h := newHarness(t, nil)
	h.d.Ownership = &fakeOwnership{err: errors.New("journal: permission denied")}
	h.fake.add("a", "codex", "gpt", true)
	reset := h.clock.Add(5 * time.Hour)
	h.snap(codexPrimary, 96, reset, "1")
	if fmt.Sprint(h.fake.stops) != "[interrupt:a]" {
		t.Fatalf("stops = %v", h.fake.stops)
	}
	if !h.d.ownershipWarned {
		t.Fatal("the degradation was not recorded for the once-only log")
	}
}

func TestGoverningPauseAndRecoveryRules(t *testing.T) {
	h := newHarness(t, nil)
	thread := domain.Thread{ID: "a", ProviderInstanceID: "codex", Model: "gpt"}
	reset := h.clock.Add(5 * time.Hour)
	stopped := domain.BucketState{Key: codexPrimary, Phase: domain.PhaseStopped, UsedPercent: 97, ResetsAt: &reset, ObservedAt: h.clock}
	draining := domain.BucketState{Key: codexSecondary, Phase: domain.PhaseDraining, UsedPercent: 91, ResetsAt: &reset, ObservedAt: h.clock}
	other := domain.BucketState{Key: domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "seven_day"}, Phase: domain.PhaseStopped, UsedPercent: 99, ResetsAt: &reset, ObservedAt: h.clock}
	governing, ok := GoverningPause(h.cfg, thread, []domain.BucketState{draining, other, stopped}, h.clock)
	if !ok || governing.Key != codexPrimary || governing.Phase != domain.PhaseStopped {
		t.Fatalf("governing = %+v ok=%v", governing, ok)
	}
	if _, ok := GoverningPause(h.cfg, thread, []domain.BucketState{other}, h.clock); ok {
		t.Fatal("another provider's bucket governs the thread")
	}
	expired := stopped
	past := h.clock.Add(-time.Minute)
	expired.ResetsAt = &past
	if _, ok := GoverningPause(h.cfg, thread, []domain.BucketState{expired}, h.clock); ok {
		t.Fatal("an expired window still governs the thread")
	}

	byKey := map[domain.BucketKey]domain.BucketState{codexPrimary: stopped}
	if ok, why := BucketsRecovered(h.cfg, []domain.BucketKey{codexPrimary}, h.clock, thread, byKey, h.clock.Add(time.Hour), nil); ok || why == "" {
		t.Fatalf("recovered before the reset: %v %q", ok, why)
	}
	recoveredAt := reset.Add(time.Minute)
	nextReset := reset.Add(5 * time.Hour)
	healthy := domain.BucketState{Key: codexPrimary, Phase: domain.PhaseNormal, UsedPercent: 2, ResetsAt: &nextReset, ObservedAt: recoveredAt, RecoveredAt: &recoveredAt, Healthy: true}
	byKey[codexPrimary] = healthy
	if ok, why := BucketsRecovered(h.cfg, []domain.BucketKey{codexPrimary}, h.clock, thread, byKey, recoveredAt.Add(time.Minute), nil); ok {
		t.Fatalf("recovered inside the settle delay: %q", why)
	}
	if ok, why := BucketsRecovered(h.cfg, []domain.BucketKey{codexPrimary}, h.clock, thread, byKey, recoveredAt.Add(3*time.Minute), nil); !ok {
		t.Fatalf("not recovered after the settle delay: %q", why)
	}
	// The probe window opens after the reset plus probe_after_reset plus the
	// settle delay when no reading newer than the reset exists.
	if ProbeWindowOpen(h.cfg, stopped, reset.Add(3*time.Minute)) {
		t.Fatal("probe window open before the probe delay")
	}
	if !ProbeWindowOpen(h.cfg, stopped, reset.Add(8*time.Minute)) {
		t.Fatal("probe window closed after the probe delay")
	}
}
