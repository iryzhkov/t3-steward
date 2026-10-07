package backlog

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestWorkerDrainPercentRejectsOversizedThreshold(t *testing.T) {
	now := time.Date(2026, 10, 7, 0, 40, 0, 0, time.UTC)
	for _, tc := range []struct {
		percent float64
		invalid bool
	}{{90.5, false}, {100, false}, {101, true}} {
		bridge := QuotaBridge{Store: &quotaBridgeStoreFake{}, MaxObservationAge: 5 * time.Minute, Now: func() time.Time { return now }, Pools: []QuotaPoolBinding{{ID: "claude-main", Provider: "claudeAgent", ProviderInstanceIDs: []string{"claudeAgent"}, MaxConcurrent: 2}}}
		obs := domain.WorkerQuotaObservation{Key: admissionBucket("claudeAgent", "five_hour"), Phase: domain.PhaseWarned, UsedPercent: 86, ObservedAt: now, DrainPercent: tc.percent}
		_, err := bridge.ReconcileObservations(context.Background(), nil, []domain.WorkerSnapshot{{QuotaObservations: []domain.WorkerQuotaObservation{obs}}})
		if (err != nil) != tc.invalid {
			t.Fatalf("drainPercent %v error=%v invalid=%v", tc.percent, err, tc.invalid)
		}
	}
}
