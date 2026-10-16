package domain_test

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"math"
	"testing"
	"time"
)

func TestFoldQuotaEvidenceRetainsRestrictionsAndSourceBoundary(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	early := now.Add(time.Second)
	late := now.Add(time.Minute)
	p := &domain.QuotaProvenance{SourceID: "source", AcquisitionID: "batch", OriginWorkerID: "worker", AcquiredAt: now}
	a := domain.BucketState{UsedPercent: 20, RatePerMinute: 5, Provenance: domain.CloneQuotaProvenance(p), OriginValidUntil: &late, Phase: domain.PhaseNormal, Healthy: true}
	b := domain.BucketState{UsedPercent: 90, RatePerMinute: 2, Provenance: domain.CloneQuotaProvenance(p), OriginValidUntil: &early, Phase: domain.PhaseStopped, Healthy: false}
	for _, pair := range [][2]domain.BucketState{{a, b}, {b, a}} {
		folded := domain.FoldQuotaEvidence(pair[0], pair[1])
		if folded.UsedPercent != 90 || folded.RatePerMinute != 5 || folded.Provenance == nil || !folded.OriginValidUntil.Equal(early) {
			t.Fatalf("conservative evidence lost: %+v", folded)
		}
		if folded.Phase != pair[0].Phase || folded.Healthy != pair[0].Healthy {
			t.Fatal("evidence helper took over caller phase/health policy")
		}
	}
	for _, other := range []*domain.QuotaProvenance{nil, {SourceID: "source", AcquisitionID: "other", OriginWorkerID: "worker", AcquiredAt: now}, {SourceID: "source", AcquisitionID: "batch", OriginWorkerID: "different", AcquiredAt: now}} {
		b.Provenance = other
		if domain.FoldQuotaEvidence(a, b).Provenance != nil || domain.FoldQuotaEvidence(b, a).Provenance != nil {
			t.Fatal("mixed or missing acquisition inherited native age")
		}
	}
	b.Provenance = domain.CloneQuotaProvenance(p)
	b.OriginValidUntil = nil
	if got := domain.FoldQuotaEvidence(b, a); got.OriginValidUntil == nil || !got.OriginValidUntil.Equal(late) {
		t.Fatal("missing expiry extended a bounded origin")
	}
}

type foldQuotaStore struct{ states []domain.BucketState }

func (s foldQuotaStore) ListBuckets(context.Context) ([]domain.BucketState, error) {
	return s.states, nil
}
func (foldQuotaStore) LoadQuotaAdmissions(context.Context) ([]domain.QuotaAdmissionRecord, error) {
	return nil, nil
}
func (foldQuotaStore) CommitQuotaAdmissionTransitions(context.Context, []domain.QuotaAdmissionTransition) error {
	return nil
}
func TestEvidenceFoldCallersStillRejectInvalidTiedPercent(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	key := domain.BucketKey{ProviderInstanceID: "codex", LimitID: "limit", Window: domain.WindowPrimary}
	valid := domain.BucketState{Key: key, UsedPercent: 10, ObservedAt: now, Phase: domain.PhaseNormal, Healthy: true}
	for _, percent := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), -1, 101} {
		invalid := valid
		invalid.UsedPercent = percent
		for _, states := range [][]domain.BucketState{{valid, invalid}, {invalid, valid}} {
			pool := domain.QuotaPool{ID: "pool", Provider: "codex", ProviderInstanceIDs: []string{"codex"}, Buckets: []domain.BucketKey{key}}
			if domain.ReadQuotaWindows(pool, states, now, time.Minute).Fresh() {
				t.Fatal("invalid tie gained usable admission evidence")
			}
			bridge := backlog.QuotaBridge{Store: foldQuotaStore{states}, MaxObservationAge: time.Minute, Now: func() time.Time { return now }, Pools: []backlog.QuotaPoolBinding{{ID: "pool", Provider: "codex", ProviderInstanceIDs: []string{"codex"}, MaxConcurrent: 1}}}
			if _, err := bridge.Reconcile(context.Background(), nil); err == nil {
				t.Fatal("planner silently replaced invalid tied percentage")
			}
		}
	}
}
