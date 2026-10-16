package workerruntime

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func coveredClaudeConfig() config.Config {
	cfg := config.Default()
	cfg.Policy.DryRun = false
	cfg.BacklogV2.Workers = map[string]config.V2Worker{"normandy": {Providers: map[string]config.V2Provider{
		"claudeAgent": {Models: []string{"claude-fable-5-1"}, QuotaPool: "claude-main"},
	}}}
	cfg.BacklogV2.QuotaPools = map[string]config.V2QuotaPool{"claude-main": {Provider: "claudeAgent", AccountID: "account-a", ObservationMaxAge: config.Duration(10 * time.Minute),
		Collector: &config.V2QuotaCollectorCoverage{SourceID: "native", Origins: []domain.QuotaCollectorOrigin{{WorkerID: "normandy", ProviderInstanceID: "claudeAgent"}},
			RequiredWindows: []domain.QuotaCollectorWindow{{Window: "five_hour"}, {Window: "seven_day"}}}}}
	return cfg
}

func coveredClaudeRuntime(t *testing.T, guard QuotaGuard, now *time.Time, observations ...backlog.DispatchThreadState) (*Runtime, *fakeDriver) {
	t.Helper()
	driver := &fakeDriver{workspace: filepath.Join(t.TempDir(), "workspace"), workspaceReady: true, observations: observations}
	runtime := newTestRuntimeWithClock(t, t.TempDir(), driver, func() time.Time { return *now })
	pkg := testPackage()
	pkg.Route = domain.ProviderRoute{WorkerID: "normandy", ProviderInstanceID: "claudeAgent", Model: "claude-fable-5-1", QuotaPoolID: "claude-main"}
	offer := testOffer(t)
	offer.Assignment.Route = pkg.Route
	manifest, err := workerproto.BuildExecutionPackageManifest(pkg)
	if err != nil {
		t.Fatal(err)
	}
	offer.Package = manifest
	if _, err := runtime.AcceptOffers(context.Background(), workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{offer}}); err != nil {
		t.Fatal(err)
	}
	runtime.config.Quota = guard
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}
	return runtime, driver
}

func claudeAnonymousStop(now time.Time) domain.BucketState {
	observed := now.Add(-12 * time.Minute)
	reset := now.Add(-6 * time.Minute)
	return domain.BucketState{Key: domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "legacy", Window: "seven_day"},
		Phase: domain.PhaseStopped, UsedPercent: 97, ObservedAt: observed, UpdatedAt: observed, StoppedAt: &observed, ResetsAt: &reset, Epoch: domain.EpochFor(&reset)}
}

func saveClaudeAcquisition(t *testing.T, f *recoveryFixture, now time.Time) {
	t.Helper()
	at := now.Add(-3 * time.Minute)
	reset := now.Add(time.Hour)
	for _, window := range []string{"five_hour", "seven_day"} {
		key := domain.BucketKey{ProviderInstanceID: "claudeAgent", AccountID: "account-a", LimitID: "native", Window: window}
		state := domain.BucketState{Key: key, Phase: domain.PhaseNormal, Healthy: true, UsedPercent: 20, ObservedAt: at, UpdatedAt: at,
			ResetsAt: &reset, Epoch: domain.EpochFor(&reset), Provenance: &domain.QuotaProvenance{SourceID: "native", AcquisitionID: "batch-1", OriginWorkerID: "normandy", AcquiredAt: at}}
		if err := f.store.SaveBucket(context.Background(), state); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCoveredClaudeWorkerWaitsForVerifiedRecoveryWithoutInference(t *testing.T) {
	now := runtimeTestNow
	f := newRecoveryFixture(t, &now)
	f.cfg = coveredClaudeConfig()
	f.guard.Config = f.cfg
	old := claudeAnonymousStop(now)
	if err := f.store.SaveBucket(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	runtime, driver := coveredClaudeRuntime(t, f.guard, &now, backlog.DispatchThreadActive, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped, backlog.DispatchThreadStopped)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if journalRecord(t, runtime).LocalThrottle == nil {
		t.Fatal("covered restrictive state did not pause")
	}
	now = now.Add(10 * time.Minute)
	renewLease(t, runtime, now.Add(time.Hour))
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.resumeCalls != 0 {
		t.Fatal("collector outage resumed for telemetry")
	}
	saved, err := f.store.LoadBucket(context.Background(), old.Key)
	if err != nil {
		t.Fatal(err)
	}
	if saved.ProbedAt != nil {
		t.Fatal("covered worker recorded an inference probe")
	}
	saveClaudeAcquisition(t, f, now)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.resumeCalls != 1 || journalRecord(t, runtime).LocalThrottle != nil {
		t.Fatalf("verified recovery resumes=%d record=%+v", driver.resumeCalls, journalRecord(t, runtime))
	}
	saved, err = f.store.LoadBucket(context.Background(), old.Key)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Key.AccountID != "" || saved.Phase != domain.PhaseStopped || saved.ProbedAt != nil {
		t.Fatalf("anonymous state rewritten: %+v", saved)
	}
}

func TestCoveredClaudeElapsedResetDoesNotRestartDrainEscalation(t *testing.T) {
	now := runtimeTestNow
	f := newRecoveryFixture(t, &now)
	f.cfg = coveredClaudeConfig()
	f.guard.Config = f.cfg
	old := claudeAnonymousStop(now)
	if err := f.store.SaveBucket(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	runtime, driver := coveredClaudeRuntime(t, f.guard, &now, backlog.DispatchThreadActive, backlog.DispatchThreadActive)
	runtime.config.PauseEscalation = 10 * time.Second
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(20 * time.Second)
	if err := runtime.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.checkpointCalls != 1 || driver.stopCalls != 1 {
		t.Fatalf("elapsed reset restarted notice: drains=%d stops=%d", driver.checkpointCalls, driver.stopCalls)
	}
}

func TestCoveredClaudeProbeOutstandingCannotBypassPause(t *testing.T) {
	now := runtimeTestNow
	f := newRecoveryFixture(t, &now)
	f.cfg = coveredClaudeConfig()
	f.guard.Config = f.cfg
	old := claudeAnonymousStop(now)
	probed := now.Add(-time.Minute)
	old.ProbedAt = &probed
	if err := f.store.SaveBucket(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	route := domain.ProviderRoute{WorkerID: "normandy", ProviderInstanceID: "claudeAgent", Model: "claude-fable-5-1", QuotaPoolID: "claude-main"}
	if f.guard.probeOutstanding(old, now) {
		t.Fatal("legacy probe marker bypassed covered quota guard")
	}
	if _, required, err := f.guard.PauseRequired(context.Background(), route); err != nil || !required {
		t.Fatalf("covered pause disappeared: required=%v err=%v", required, err)
	}
	if allowed, _, err := f.guard.probe(context.Background(), old, now); err != nil || allowed {
		t.Fatalf("covered stage2 inference allowed=%v err=%v", allowed, err)
	}
}

func TestCoveredClaudeAuthenticatedResumeNeedsRecoveryAuthority(t *testing.T) {
	now := runtimeTestNow
	guard := HostQuotaGuard{Config: coveredClaudeConfig(), Now: func() time.Time { return now }}
	runtime, driver := coveredClaudeRuntime(t, guard, &now)
	command := testThrottle(runtime, domain.ThrottleCommandResume, "no-recovery-proof")
	command.Route = journalRecord(t, runtime).Package.Package.Route
	command.QuotaPoolID = command.Route.QuotaPoolID
	acks, err := runtime.DeliverThrottle(context.Background(), []domain.ThrottleCommand{command})
	if err != nil {
		t.Fatal(err)
	}
	if len(acks) != 1 || acks[0].Accepted || driver.resumeCalls != 0 {
		t.Fatalf("identity fences substituted for quota authority: acks=%+v resumes=%d", acks, driver.resumeCalls)
	}
}
