package backlog

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Runway metadata from a worker is advisory. One worker reporting an
// impossible burn rate or drain threshold must not fail quota reconciliation
// for every pool; the bad fields are ignored and the observation keeps the
// behaviour of an older worker that sends none.
func TestMalformedWorkerRunwayDoesNotFailReconcile(t *testing.T) {
	now := time.Date(2026, 10, 7, 0, 40, 0, 0, time.UTC)
	for _, extra := range []string{`"ratePerMinute":-1`, `"ratePerMinute":1e308`, `"drainPercent":150`, `"drainPercent":-5`} {
		t.Run(extra, func(t *testing.T) {
			bridge := QuotaBridge{Store: &quotaBridgeStoreFake{}, MaxObservationAge: 5 * time.Minute, Now: func() time.Time { return now },
				Pools: []QuotaPoolBinding{{ID: "claude-main", Provider: "claudeAgent", ProviderInstanceIDs: []string{"claudeAgent"}, MaxConcurrent: 2}}}
			good := domain.WorkerQuotaObservation{Key: admissionBucket("claudeAgent", "seven_day"), Phase: domain.PhaseNormal, UsedPercent: 40, ObservedAt: now, Healthy: true}
			key, err := json.Marshal(admissionBucket("claudeAgent", "five_hour"))
			if err != nil {
				t.Fatal(err)
			}
			var bad domain.WorkerQuotaObservation
			raw := `{"key":` + string(key) + `,"phase":"warned","usedPercent":86,"healthy":true,"observedAt":"2026-10-07T00:40:00Z",` + extra + `}`
			if err := json.Unmarshal([]byte(raw), &bad); err != nil {
				t.Fatal(err)
			}
			if _, err := bridge.ReconcileObservations(context.Background(), nil, []domain.WorkerSnapshot{{QuotaObservations: []domain.WorkerQuotaObservation{good, bad}}}); err != nil {
				t.Fatalf("one malformed worker observation failed the reconcile: %v", err)
			}
		})
	}
}
