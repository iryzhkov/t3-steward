package workerruntime

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestCoveredClaudeCredentialFreeRecoveryBindsLocalStopObservation(t *testing.T) {
	for _, wrongSequence := range []bool{false, true} {
		t.Run(map[bool]string{false: "current observation", true: "old observation"}[wrongSequence], func(t *testing.T) {
			now := runtimeTestNow
			f := newRecoveryFixture(t, &now)
			f.cfg = coveredClaudeConfig()
			f.guard.Config = f.cfg
			old := claudeAnonymousStop(now)
			if err := f.store.SaveBucket(context.Background(), old); err != nil {
				t.Fatal(err)
			}
			runtime, driver := coveredClaudeRuntime(t, f.guard, &now, backlog.DispatchThreadActive, backlog.DispatchThreadStopped)
			if err := runtime.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := runtime.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			record := journalRecord(t, runtime)
			if record.LocalThrottle == nil || record.LocalThrottle.StoppedAt == nil || record.StopObservedSequence == 0 {
				t.Fatalf("missing actual stop observation: %+v", record)
			}
			now = now.Add(5 * time.Minute)
			renewLease(t, runtime, now.Add(time.Hour))
			// This worker cannot acquire source quota itself. Its declared coverage stays.
			remoteOnly := f.guard
			remoteOnly.Buckets = nil
			runtime.config.Quota = remoteOnly
			command := testThrottle(runtime, domain.ThrottleCommandResume, "local-stop-recovery")
			command.Route = record.Package.Package.Route
			command.QuotaPoolID = command.Route.QuotaPoolID
			command.CreatedAt = now.Add(-30 * time.Second)
			acquired := now.Add(-3 * time.Minute)
			command.QuotaRecovery = &domain.QuotaRecoveryGrant{
				OriginProviderInstanceID: "claudeAgent", CoverageDigest: record.LocalThrottle.CollectorCoverageDigest, QuotaPoolID: command.QuotaPoolID, AccountID: "account-a",
				StoppedAt: *record.LocalThrottle.StoppedAt, StopObservedSequence: record.StopObservedSequence,
				Provenance: domain.QuotaProvenance{SourceID: "native", AcquisitionID: "joint-acquisition", OriginWorkerID: "normandy", AcquiredAt: acquired},
				ExpiresAt:  acquired.Add(10 * time.Minute),
			}
			pool, _ := remoteOnly.Config.CollectorCoverage(command.Route)
			command.QuotaRecovery.EvidenceScope = domain.RecoveryEvidenceScope(pool)
			command.QuotaRecovery.EvidenceDigest = "test-qualified-source-authority"
			if wrongSequence {
				command.QuotaRecovery.StopObservedSequence--
			}
			if allowed, _, err := remoteOnly.ResumeAllowed(context.Background(), *record.LocalThrottle, command.Route); err != nil || allowed {
				t.Fatalf("credential-free worker minted autonomous recovery: %v %v", allowed, err)
			}
			acks, err := runtime.DeliverThrottle(context.Background(), []domain.ThrottleCommand{command})
			if err != nil || len(acks) != 1 || acks[0].Accepted == wrongSequence {
				t.Fatalf("stop observation validation: acks=%+v err=%v", acks, err)
			}
			expected := 1
			if wrongSequence {
				expected = 0
			}
			if driver.resumeCalls != expected {
				t.Fatalf("native resume calls=%d expected=%d", driver.resumeCalls, expected)
			}
		})
	}
}
