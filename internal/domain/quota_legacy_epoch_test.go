package domain

import (
	"testing"
	"time"
)

func TestQuotaLegacyOmittedEpochMatchesSameResetWorkerObservation(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	reset := now.Add(time.Hour)
	key := BucketKey{ProviderInstanceID: "codex", LimitID: "limit", Window: WindowPrimary}
	local := BucketState{Key: key, ObservedAt: now, ResetsAt: &reset, Phase: PhaseNormal, Healthy: true, UsedPercent: 10}
	worker := WorkerSnapshot{WorkerID: "worker", ObservedAt: now, Connected: true, QuotaObservations: []WorkerQuotaObservation{{Key: key, ObservedAt: now, ResetsAt: &reset, Phase: PhaseNormal, Healthy: true, UsedPercent: 10}}}
	pool := QuotaPool{ID: "codex", Provider: "codex", ProviderInstanceIDs: []string{"codex"}, Buckets: []BucketKey{key}, BucketSelection: BucketSelectionResolved}
	merged := MergeQuotaObservations([]BucketState{local}, []WorkerSnapshot{worker})
	if len(merged) != 2 {
		t.Fatal("equally timed competitors discarded")
	}
	if set := ReadQuotaWindows(pool, merged, now, time.Minute); !set.Fresh() {
		t.Fatalf("equivalent omitted epoch became unknown: %+v", set)
	}
	for _, reverse := range []bool{false, true} {
		ordered := append([]BucketState{}, merged...)
		if reverse {
			ordered[0], ordered[1] = ordered[1], ordered[0]
		}
		if set := ReadQuotaWindows(pool, ordered, now, time.Minute); !set.Fresh() {
			t.Fatalf("equivalent epoch depends on input order: %+v", set)
		}
		for _, mutate := range []func(*BucketState){
			func(state *BucketState) { state.Epoch = "explicit-mismatch" },
			func(state *BucketState) { state.Epoch = ""; state.ResetsAt = nil },
			func(state *BucketState) { different := reset.Add(time.Second); state.ResetsAt = &different },
		} {
			conflicting := append([]BucketState{}, ordered...)
			mutate(&conflicting[0])
			if !ReadQuotaWindows(pool, conflicting, now, time.Minute).Unknown {
				t.Fatal("genuine epoch/reset conflict was normalized away")
			}
		}
	}
	if QuotaObservationEpoch(BucketState{}) != "" {
		t.Fatal("unknown reset acquired an invented epoch")
	}
	if local.Epoch != "" {
		t.Fatal("raw legacy state rewritten")
	}
}
