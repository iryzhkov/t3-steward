package workerruntime

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/daemon"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// fakeThreads is the host's T3 thread list as the guard sees it.
type fakeThreads struct{ threads []domain.Thread }

func (f *fakeThreads) ListThreads(context.Context) ([]domain.Thread, error) { return f.threads, nil }

// recoveryFixture is a real state database and a real HostQuotaGuard over it,
// so the worker's resume rule is tested against the same bucket rows the
// watchdog and the bucket verb write.
type recoveryFixture struct {
	cfg     config.Config
	store   *sqlite.Store
	threads *fakeThreads
	guard   HostQuotaGuard
}

func newRecoveryFixture(t *testing.T, now *time.Time) *recoveryFixture {
	t.Helper()
	cfg := config.Default()
	cfg.Policy.DryRun = false
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.OpenMigrated(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	threads := &fakeThreads{}
	return &recoveryFixture{cfg: cfg, store: store, threads: threads, guard: HostQuotaGuard{
		Config: cfg, Buckets: store, Threads: threads, Now: func() time.Time { return *now },
	}}
}

// stoppedBucket stores the route's bucket as stopped at the given usage,
// observed and stopped at observed, with the reset three hours ahead.
func (f *recoveryFixture) stoppedBucket(t *testing.T, used float64, observed time.Time) domain.BucketState {
	t.Helper()
	reset := observed.Add(3 * time.Hour)
	st := domain.BucketState{
		Key: sevenDay, Phase: domain.PhaseStopped, Epoch: domain.EpochFor(&reset), LimitName: "codex",
		UsedPercent: used, ResetsAt: &reset, ObservedAt: observed, StoppedAt: &observed, UpdatedAt: observed,
		AppliedThresholds: &domain.ThresholdSet{WarnPercent: 85, DrainPercent: 90, StopPercent: 95},
	}
	if err := f.store.SaveBucket(context.Background(), st); err != nil {
		t.Fatal(err)
	}
	return st
}

func (f *recoveryFixture) bucket(t *testing.T) domain.BucketState {
	t.Helper()
	st, err := f.store.LoadBucket(context.Background(), sevenDay)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// pausedRuntime runs one owned attempt into a local quota pause under the
// fixture's guard and returns it with the pause in force.
func (f *recoveryFixture) pausedRuntime(t *testing.T, now *time.Time, observations ...backlog.DispatchThreadState) (*Runtime, *fakeDriver) {
	t.Helper()
	driver := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true, observations: observations}
	runtime := runningRuntime(t, driver, f.guard, now)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	record := journalRecord(t, runtime)
	if record.Phase != PhaseStopped || record.LocalThrottle == nil || driver.resumeCalls != 0 {
		t.Fatalf("attempt not paused: %+v resumes=%d", record, driver.resumeCalls)
	}
	return runtime, driver
}

// F-1, contract 1: an operator rearm is a confirmed recovery for the worker.
// An owned attempt paused at T on a stopped bucket, rearmed at T+1m through
// bucket rearm's code path, resumes within one reconcile once the settle
// delay has passed, without any reading. On the stage 1 base this fails only
// because daemon.RearmBucket does not exist; the resume rule itself needed no
// change.
func TestRearmResumesAPausedAttemptWithinOneReconcile(t *testing.T) {
	now := runtimeTestNow
	f := newRecoveryFixture(t, &now)
	f.stoppedBucket(t, 40, now.Add(-time.Minute))
	runtime, driver := f.pausedRuntime(t, &now, backlog.DispatchThreadActive, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped)
	pausedAt := journalRecord(t, runtime).LocalThrottle.RequestedAt
	// Not recovered: the pause holds.
	now = now.Add(30 * time.Second)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.resumeCalls != 0 {
		t.Fatal("resumed before the rearm")
	}
	// The operator rearms the bucket a minute after the pause.
	now = pausedAt.Add(time.Minute)
	if _, err := daemon.RearmBucket(context.Background(), f.cfg, f.store, daemon.RearmRequest{Key: sevenDay, Actor: "igor@homelab", Reason: "thresholds restored"}, now); err != nil {
		t.Fatal(err)
	}
	// Inside the settle delay: still paused.
	now = now.Add(time.Minute)
	renewLease(t, runtime, now.Add(10*time.Minute))
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.resumeCalls != 0 {
		t.Fatal("resumed inside the settle delay")
	}
	// Past the settle delay: one reconcile resumes it.
	now = now.Add(f.cfg.Resume.ResetSettleDelay.D())
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	record := journalRecord(t, runtime)
	if driver.resumeCalls != 1 || record.Phase != PhaseRunning || record.LocalThrottle != nil || record.LastLocalThrottle == nil {
		t.Fatalf("after the rearm: resumes=%d record=%+v", driver.resumeCalls, record)
	}
	if !strings.Contains(driver.resumeReasons[0], "recovered") {
		t.Fatalf("resume reason = %q", driver.resumeReasons[0])
	}
}

// The probe rule: a bucket stopped below stop_percent whose stored reading is
// older than resume.probe_after_reset, with nothing running on the host to
// refresh it, lets exactly one paused attempt resume per epoch as a probe.
// The second waits; a running matching thread, an unknown thread list, or a
// re-stop after the probe's reading all refuse.
func TestProbeResumesOnePausedAttemptPerBucketEpoch(t *testing.T) {
	now := runtimeTestNow
	f := newRecoveryFixture(t, &now)
	observed := now.Add(-10 * time.Minute)
	f.stoppedBucket(t, 60, observed)
	route := domain.ProviderRoute{ProviderInstanceID: "codex", Model: "gpt-5.6-sol"}
	first := LocalThrottleRequest{Kind: domain.ThrottleCommandDrain, Bucket: sevenDay, Phase: domain.PhaseStopped, UsedPercent: 60, RequestedAt: observed.Add(time.Minute)}
	second := first
	second.RequestedAt = observed.Add(2 * time.Minute)
	ctx := context.Background()

	// A thread list the guard cannot see means no probe.
	blind := f.guard
	blind.Threads = nil
	if ok, why, err := blind.ResumeAllowed(ctx, first, route); ok || err != nil {
		t.Fatalf("probed without a thread list: ok=%v why=%q err=%v", ok, why, err)
	}
	// A running thread on the bucket will produce a reading: no probe.
	f.threads.threads = []domain.Thread{{ID: "interactive", ProviderInstanceID: "codex", Model: "gpt-5.6-sol", Running: true}}
	if ok, why, _ := f.guard.ResumeAllowed(ctx, first, route); ok {
		t.Fatalf("probed while a matching thread runs: %q", why)
	}
	f.threads.threads = []domain.Thread{{ID: "other", ProviderInstanceID: "claudeAgent", Model: "opus", Running: true}}
	// A stored reading younger than probe_after_reset: no probe yet.
	young := f.bucket(t)
	young.ObservedAt = now.Add(-time.Minute)
	if err := f.store.SaveBucket(ctx, young); err != nil {
		t.Fatal(err)
	}
	if ok, why, _ := f.guard.ResumeAllowed(ctx, first, route); ok {
		t.Fatalf("probed on a fresh reading: %q", why)
	}
	f.stoppedBucket(t, 60, observed)
	// Stopped at or above stop_percent: no probe, the reading would re-stop.
	high := f.bucket(t)
	high.UsedPercent = 96
	if err := f.store.SaveBucket(ctx, high); err != nil {
		t.Fatal(err)
	}
	if ok, why, _ := f.guard.ResumeAllowed(ctx, first, route); ok {
		t.Fatalf("probed at 96%%: %q", why)
	}
	f.stoppedBucket(t, 60, observed)

	// Every condition holds: the first attempt probes, the second waits.
	ok, why, err := f.guard.ResumeAllowed(ctx, first, route)
	if err != nil || !ok || !strings.Contains(why, "probe") {
		t.Fatalf("first: ok=%v why=%q err=%v", ok, why, err)
	}
	st := f.bucket(t)
	if st.ProbedAt == nil || !st.ProbedAt.Equal(now) || st.Phase != domain.PhaseStopped {
		t.Fatalf("probe not recorded on the bucket: %+v", st)
	}
	if ok, why, _ := f.guard.ResumeAllowed(ctx, second, route); ok {
		t.Fatalf("second attempt probed too: %q", why)
	} else if !strings.Contains(why, "probe") {
		t.Fatalf("refusal does not name the outstanding probe: %q", why)
	}
	actions, _ := f.store.RecentActions(ctx, 10)
	if len(actions) != 1 || actions[0].Kind != domain.ActionResume || !strings.Contains(actions[0].Detail, "probe") {
		t.Fatalf("actions = %+v, want one probe record", actions)
	}

	// The probe's reading re-stops the bucket at 62%: no second probe this
	// epoch, for either attempt.
	now = now.Add(time.Minute)
	engine := f.guard.engine(sevenDay, "codex", 0)
	decision := engine.Evaluate(domain.QuotaSnapshot{Key: sevenDay, LimitName: "codex", UsedPercent: 62, ResetsAt: st.ResetsAt, ObservedAt: now, SourceEventID: "probe-reading"}, st, now)
	if decision.State.Phase != domain.PhaseStopped || decision.State.ProbedAt == nil {
		t.Fatalf("reading after the probe: %+v", decision.State)
	}
	if err := f.store.SaveBucket(ctx, decision.State); err != nil {
		t.Fatal(err)
	}
	now = now.Add(20 * time.Minute)
	for _, pause := range []LocalThrottleRequest{first, second} {
		if ok, why, _ := f.guard.ResumeAllowed(ctx, pause, route); ok {
			t.Fatalf("probed twice in one epoch: %q", why)
		}
	}
	// A new epoch clears the probe: the reset's first reading rearms, and
	// the recovery, not a probe, resumes both.
	st = f.bucket(t)
	now = st.ResetsAt.Add(time.Minute)
	nextReset := now.Add(5 * time.Hour)
	decision = engine.Evaluate(domain.QuotaSnapshot{Key: sevenDay, LimitName: "codex", UsedPercent: 3, ResetsAt: &nextReset, ObservedAt: now, SourceEventID: "new-window"}, st, now)
	if decision.State.Phase != domain.PhaseNormal || decision.State.ProbedAt != nil {
		t.Fatalf("new window: %+v", decision.State)
	}
}

// Through the runtime: the probing attempt resumes with a reason naming the
// probe, the bucket is not asked to pause it again while the probe's reading
// is outstanding, and the reading that re-stops the bucket pauses it again
// without a second probe.
func TestProbeResumesThePausedAttemptThroughTheRuntime(t *testing.T) {
	now := runtimeTestNow
	f := newRecoveryFixture(t, &now)
	f.stoppedBucket(t, 60, now.Add(-time.Minute))
	runtime, driver := f.pausedRuntime(t, &now,
		backlog.DispatchThreadActive, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped,
		backlog.DispatchThreadActive, backlog.DispatchThreadActive, backlog.DispatchThreadStopped)
	// Before the stored reading is old enough: no probe.
	now = now.Add(2 * time.Minute)
	renewLease(t, runtime, now.Add(time.Hour))
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.resumeCalls != 0 {
		t.Fatal("probed before probe_after_reset")
	}
	// Old enough, nothing running on the host: the probe.
	now = now.Add(10 * time.Minute)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	record := journalRecord(t, runtime)
	if driver.resumeCalls != 1 || record.Phase != PhaseRunning || !strings.Contains(driver.resumeReasons[0], "probe") {
		t.Fatalf("probe: resumes=%d reasons=%v record=%+v", driver.resumeCalls, driver.resumeReasons, record)
	}
	// The thread runs; while the probe's reading is outstanding the stopped
	// bucket does not pause it again.
	now = now.Add(30 * time.Second)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.checkpointCalls != 1 || driver.stopCalls != 0 || journalRecord(t, runtime).Phase != PhaseRunning {
		t.Fatalf("probe paused again before its reading: checkpoints=%d stops=%d", driver.checkpointCalls, driver.stopCalls)
	}
	// The reading arrives and re-stops the bucket: the next reconcile pauses
	// the thread again, and nothing probes again this epoch.
	st := f.bucket(t)
	decision := f.guard.engine(sevenDay, "codex", 0).Evaluate(domain.QuotaSnapshot{Key: sevenDay, LimitName: "codex", UsedPercent: 61, ResetsAt: st.ResetsAt, ObservedAt: now, SourceEventID: "probe-reading"}, st, now)
	if err := f.store.SaveBucket(context.Background(), decision.State); err != nil {
		t.Fatal(err)
	}
	now = now.Add(30 * time.Second)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.checkpointCalls != 2 || journalRecord(t, runtime).Phase != PhaseStopped {
		t.Fatalf("not paused again after the reading: checkpoints=%d phase=%s", driver.checkpointCalls, journalRecord(t, runtime).Phase)
	}
	now = now.Add(20 * time.Minute)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.resumeCalls != 1 {
		t.Fatalf("probed twice in one epoch: resumes=%d reasons=%v", driver.resumeCalls, driver.resumeReasons)
	}
}

// U-3 guard: a locally paused attempt's lease keeps being renewed. Across one
// lease duration the renewal batch still names it, so the coordinator never
// sees the lease lapse and reassigns work the paused attempt still holds.
func TestU3PausedAttemptLeaseIsRenewedAcrossOneLeaseDuration(t *testing.T) {
	now := runtimeTestNow
	f := newRecoveryFixture(t, &now)
	f.stoppedBucket(t, 97, now.Add(-time.Minute))
	// One observation per reconcile: the thread stays stopped throughout.
	observations := []backlog.DispatchThreadState{backlog.DispatchThreadActive}
	for range 8 {
		observations = append(observations, backlog.DispatchThreadStopped)
	}
	runtime, _ := f.pausedRuntime(t, &now, observations...)
	for step := 0; step < 4; step++ {
		now = now.Add(runtime.config.LeaseDuration / 2)
		renewals, err := runtime.LeaseRenewals()
		if err != nil || len(renewals.Renewals) != 1 || renewals.Renewals[0].AssignmentID != "assignment-1" {
			t.Fatalf("step %d: paused attempt missing from the renewals: %+v err=%v", step, renewals, err)
		}
		if err := runtime.ApplyLeaseRenewals(renewals); err != nil {
			t.Fatal(err)
		}
		if err := runtime.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	record := journalRecord(t, runtime)
	if record.Phase != PhaseStopped || record.LocalThrottle == nil || !record.Assignment.LeaseExpiresAt.After(now) {
		t.Fatalf("after two lease durations: %+v", record)
	}
}
