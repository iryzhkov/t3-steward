package domain

import (
	"slices"
	"testing"
	"time"
)

func TestRecoveryEvidenceDigestIncludesOnlyRelevantSourceFactsAndEnvelopes(t *testing.T) {
	now := time.Date(2030, 1, 1, 1, 0, 0, 0, time.UTC)
	pool := QuotaPool{AccountID: "account-a", ProviderInstanceIDs: []string{"claudeSource", "claudeExec"}, Models: []string{"claude-fable-5-1"},
		Collector: &QuotaCollectorCoverage{RequiredWindows: []QuotaCollectorWindow{{Window: "five_hour"}, {Window: "seven_day"}}}}
	scope := RecoveryEvidenceScope(pool)
	base := []BucketState{{Key: BucketKey{ProviderInstanceID: "claudeSource", AccountID: "account-a", Window: "five_hour"}, Phase: PhaseNormal, Healthy: true, UsedPercent: 20, ObservedAt: now}}
	digest := QuotaRecoveryEvidenceDigest(pool.AccountID, scope, base, nil)
	restriction := BucketState{Key: BucketKey{ProviderInstanceID: "claudeSource", Window: "unmapped-window"}, Phase: PhaseStopped, UsedPercent: 100, ObservedAt: now}
	if next := QuotaRecoveryEvidenceDigest(pool.AccountID, scope, append(append([]BucketState(nil), base...), restriction), nil); next == digest {
		t.Fatal("new unmapped account restriction escaped source evidence fence")
	}
	for _, unrelated := range []BucketState{
		{Key: BucketKey{ProviderInstanceID: "codex", Window: "primary"}, Phase: PhaseStopped, UsedPercent: 100},
		{Key: BucketKey{ProviderInstanceID: "claudeSource", AccountID: "other-account", Window: "seven_day"}, Phase: PhaseStopped, UsedPercent: 100},
		{Key: BucketKey{ProviderInstanceID: "claudeSource", Window: "unmapped-model"}, ModelSelector: "haiku", Phase: PhaseStopped, UsedPercent: 100},
	} {
		if next := QuotaRecoveryEvidenceDigest(pool.AccountID, scope, append(append([]BucketState(nil), base...), unrelated), nil); next != digest {
			t.Fatal("unrelated account/instance/model blocked recovery commit")
		}
	}
	both := append(append([]BucketState(nil), base...), restriction)
	ordered := QuotaRecoveryEvidenceDigest(pool.AccountID, scope, both, nil)
	slices.Reverse(both)
	if next := QuotaRecoveryEvidenceDigest(pool.AccountID, scope, both, nil); next != ordered {
		t.Fatal("source evidence digest depended on tie order")
	}
	worker := WorkerSnapshot{WorkerID: "source-worker", WorkerEpoch: "source-1", Sequence: 1, Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Minute),
		QuotaObservations: []WorkerQuotaObservation{{Key: base[0].Key, Phase: PhaseNormal, Healthy: true, UsedPercent: 20, ObservedAt: now}}}
	original := QuotaRecoveryEvidenceDigest(pool.AccountID, scope, nil, []WorkerSnapshot{worker})
	worker.Sequence++
	if next := QuotaRecoveryEvidenceDigest(pool.AccountID, scope, nil, []WorkerSnapshot{worker}); next == original {
		t.Fatal("changed authenticated source envelope escaped commit fence")
	}
}
