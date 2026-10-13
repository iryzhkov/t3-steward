package backlog

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
	"time"
)

func TestQuotaAdmissionOmittedEpochUsesOnlyItsOwnReset(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	reset := now.Add(time.Hour)
	key := domain.BucketKey{ProviderInstanceID: "codex", LimitID: "limit", Window: domain.WindowPrimary}
	local := domain.BucketState{Key: key, ObservedAt: now, ResetsAt: &reset, Phase: domain.PhaseNormal, Healthy: true, UsedPercent: 10}
	explicit := local
	explicit.Epoch = domain.EpochFor(&reset)
	pool := domain.QuotaPool{ID: "codex", Provider: "codex", ProviderInstanceIDs: []string{"codex"}, Buckets: []domain.BucketKey{key}, MaxConcurrent: 1}
	for _, ordered := range [][]domain.BucketState{{local, explicit}, {explicit, local}} {
		got, err := DeriveQuotaPoolAdmissions(QuotaAdmissionDerivationInput{Now: now, MaxObservationAge: time.Minute, Pools: []domain.QuotaPool{pool}, BucketStates: ordered})
		if err != nil || got[0].Admission != domain.AdmissionOpen {
			t.Fatalf("equivalent legacy epoch refused: %v %+v", err, got)
		}
		ordered[0].Epoch = "explicit-mismatch"
		got, err = DeriveQuotaPoolAdmissions(QuotaAdmissionDerivationInput{Now: now, MaxObservationAge: time.Minute, Pools: []domain.QuotaPool{pool}, BucketStates: ordered})
		if err != nil || got[0].Admission != domain.AdmissionClosed {
			t.Fatal("explicit mismatch opened admission")
		}
	}
}
