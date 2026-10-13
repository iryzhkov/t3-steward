package coordinatormetrics

import (
	"math"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestCollectorPoolSelectionUsesUniqueNativeScope(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	first, _ := collectorFixture(now)
	second, state := collectorFixture(now)
	second.ID = "second-pool"
	second.AccountID = "00000000-0000-0000-0000-000000000001"
	second.Collector.AccountID = second.AccountID
	second.Collector.SourceID = "second-native"
	state.Key.AccountID = second.AccountID
	state.Provenance.SourceID = second.Collector.SourceID
	state.ObservedAt = now.Add(-5 * time.Minute)
	state.Provenance.AcquiredAt = state.ObservedAt
	pools := []domain.QuotaPool{first, second}
	for i := 0; i < 2; i++ {
		body := render(Snapshot{Buckets: []domain.BucketState{state}}, now, time.Minute, time.Minute, pools)
		requireMetric(t, body, "steward_quota_observations{window=\"five_hour\"} 1")
		requireMetric(t, body, "steward_quota_worst_used_ratio{window=\"five_hour\"} 0.2")
		pools[0], pools[1] = pools[1], pools[0]
	}
}

func TestUnrelatedAccountDoesNotClampLegacyAgeButAnonymousScopeDoes(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	pool, state := collectorFixture(now)
	state.Provenance = nil
	state.Key.AccountID = "00000000-0000-0000-0000-000000000002"
	body := render(Snapshot{Buckets: []domain.BucketState{state}}, now, time.Minute, 3*time.Minute, []domain.QuotaPool{pool})
	requireMetric(t, body, "steward_quota_observations{window=\"five_hour\"} 1")
	state.Key.AccountID = ""
	body = render(Snapshot{Buckets: []domain.BucketState{state}}, now, time.Minute, 3*time.Minute, []domain.QuotaPool{pool})
	requireMetric(t, body, "steward_quota_observations{window=\"five_hour\"} 0")
}

func TestAmbiguousNativeCoverageDoesNotExtendAge(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	pool, state := collectorFixture(now)
	duplicate := pool
	duplicate.ID = "duplicate-pool"
	for _, pools := range [][]domain.QuotaPool{{pool, duplicate}, {duplicate, pool}} {
		body := render(Snapshot{Buckets: []domain.BucketState{state}}, now, time.Minute, 3*time.Minute, pools)
		requireMetric(t, body, "steward_quota_observations{window=\"five_hour\"} 0")
	}
}

func TestInvalidOldQuotaIsStillDiagnosed(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	body := render(Snapshot{Buckets: []domain.BucketState{{
		Key: domain.BucketKey{Window: "five_hour"}, ObservedAt: now.Add(-time.Hour), UsedPercent: math.NaN(),
	}}}, now, time.Minute, time.Minute, nil)
	requireMetric(t, body, "steward_quota_invalid_observations 1")
	requireMetric(t, body, "steward_quota_observations{window=\"five_hour\"} 0")
}
