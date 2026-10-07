package workerruntime

import (
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
	"time"
)

type runwayBuckets []domain.BucketState

func (b runwayBuckets) ListBuckets(context.Context) ([]domain.BucketState, error) { return b, nil }

func TestHostObservationFallbackAndNoProjection(t *testing.T) {
	now := time.Date(2026, 10, 7, 0, 40, 0, 0, time.UTC)
	for _, tc := range []struct {
		name       string
		used, rate float64
		projected  bool
	}{
		{"configured ladder", 86, 0.8, true}, {"unknown rate", 86, 0, false}, {"falling rate", 86, -1, false}, {"already draining", 90, 0.8, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deadline := now.Add(time.Minute)
			cfg := config.Default()
			cfg.Policy.DrainPercent = 90
			guard := HostQuotaGuard{Config: cfg, Buckets: runwayBuckets{{Key: domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"}, UsedPercent: tc.used, RatePerMinute: tc.rate, ObservedAt: now, DrainDeadline: &deadline}}}
			got, err := guard.Observations(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || got[0].DrainPercent != 90 || (got[0].DrainsAt != nil) != tc.projected || got[0].DrainDeadline == nil || !got[0].DrainDeadline.Equal(deadline) {
				t.Fatalf("observation: %+v", got)
			}
		})
	}
}

func TestHostObservationReportsDrainRunway(t *testing.T) {
	now := time.Date(2026, 10, 7, 0, 40, 0, 0, time.UTC)
	guard := HostQuotaGuard{Buckets: runwayBuckets{{Key: domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"}, Phase: domain.PhaseWarned, UsedPercent: 86, RatePerMinute: 0.8, ObservedAt: now, AppliedThresholds: &domain.ThresholdSet{WarnPercent: 85, DrainPercent: 90, StopPercent: 95}}}}
	observations, err := guard.Observations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(observations[0])
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		RatePerMinute float64
		DrainPercent  float64
		DrainsAt      *time.Time
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.RatePerMinute != 0.8 || got.DrainPercent != 90 || got.DrainsAt == nil || !got.DrainsAt.Equal(now.Add(5*time.Minute)) {
		t.Fatalf("runway observation = %s", raw)
	}
}
