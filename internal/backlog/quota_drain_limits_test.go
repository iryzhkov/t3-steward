package backlog

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A worker drain threshold above 100% is ignored rather than failing the
// reconcile for every pool; see TestMalformedWorkerRunwayDoesNotFailReconcile.
func TestWorkerDrainPercentIgnoresOversizedThreshold(t *testing.T) {
	now := time.Date(2026, 10, 7, 0, 40, 0, 0, time.UTC)
	for _, tc := range []struct {
		percent float64
		applied bool
	}{{90.5, true}, {100, true}, {101, false}} {
		obs := domain.WorkerQuotaObservation{Key: admissionBucket("claudeAgent", "five_hour"), Phase: domain.PhaseWarned, UsedPercent: 86, ObservedAt: now, DrainPercent: tc.percent}
		bridge := QuotaBridge{Store: &quotaBridgeStoreFake{}, MaxObservationAge: 5 * time.Minute, Now: func() time.Time { return now }, Pools: []QuotaPoolBinding{{ID: "claude-main", Provider: "claudeAgent", ProviderInstanceIDs: []string{"claudeAgent"}, MaxConcurrent: 2}}}
		if _, err := bridge.ReconcileObservations(context.Background(), nil, []domain.WorkerSnapshot{{QuotaObservations: []domain.WorkerQuotaObservation{obs}}}); err != nil {
			t.Fatalf("drainPercent %v: %v", tc.percent, err)
		}
		merged := domain.MergeQuotaObservations(nil, []domain.WorkerSnapshot{{QuotaObservations: []domain.WorkerQuotaObservation{obs}}})
		if len(merged) != 1 || (merged[0].AppliedThresholds != nil) != tc.applied {
			t.Fatalf("drainPercent %v: applied thresholds %+v, want applied %v", tc.percent, merged, tc.applied)
		}
	}
}
