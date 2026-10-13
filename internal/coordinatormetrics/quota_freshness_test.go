package coordinatormetrics

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func collectorFixture(now time.Time) (domain.QuotaPool, domain.BucketState) {
	pool := domain.QuotaPool{
		ID: "fixture-pool", Provider: "claudeAgent",
		AccountID:           "00000000-0000-0000-0000-000000000000",
		ProviderInstanceIDs: []string{"fixture-instance"}, Models: []string{"fixture-model"},
		Collector: &domain.QuotaCollectorCoverage{
			SourceID: "fixture-native", AccountID: "00000000-0000-0000-0000-000000000000",
			Origins:           []domain.QuotaCollectorOrigin{{WorkerID: "fixture-worker", ProviderInstanceID: "fixture-instance"}},
			MaxObservationAge: 390 * time.Second,
			RequiredWindows: []domain.QuotaCollectorWindow{
				{Window: "five_hour", Models: []string{"fixture-model"}}, {Window: "seven_day"},
			},
		},
	}
	at, reset := now.Add(-2*time.Minute), now.Add(time.Hour)
	state := domain.BucketState{
		Key:        domain.BucketKey{ProviderInstanceID: "fixture-instance", AccountID: pool.AccountID, Window: "five_hour"},
		ObservedAt: at, ResetsAt: &reset, UsedPercent: 20,
		Provenance: &domain.QuotaProvenance{
			SourceID: "fixture-native", AcquisitionID: "fixture-acquisition", OriginWorkerID: "fixture-worker", AcquiredAt: at,
		},
	}
	return pool, state
}

func requireMetric(t *testing.T, body, want string) {
	t.Helper()
	if !strings.Contains(body, want+"\n") {
		t.Fatalf("missing %q in metrics:\n%s", want, body)
	}
}

func TestCollectorFreshnessRequiresNativeScope(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		change func(*domain.BucketState)
		fresh  bool
	}{
		{"qualified", func(*domain.BucketState) {}, true},
		{"anonymous", func(s *domain.BucketState) { s.Key.AccountID = ""; s.Provenance = nil }, false},
		{"legacy", func(s *domain.BucketState) { s.Provenance = nil }, false},
		{"wrong-account", func(s *domain.BucketState) { s.Key.AccountID = "different-account" }, false},
		{"wrong-source", func(s *domain.BucketState) { s.Provenance.SourceID = "log-source" }, false},
		{"wrong-origin", func(s *domain.BucketState) { s.Provenance.OriginWorkerID = "different-worker" }, false},
		{"missing-acquisition", func(s *domain.BucketState) { s.Provenance.AcquisitionID = "" }, false},
		{"restamped-observation", func(s *domain.BucketState) { s.Provenance.AcquiredAt = now }, false},
		{"missing-reset", func(s *domain.BucketState) { s.ResetsAt = nil }, false},
		{"unmapped-window", func(s *domain.BucketState) { s.Key.Window = "unmapped" }, false},
		{"expired-scope", func(s *domain.BucketState) {
			s.ObservedAt = now.Add(-391 * time.Second)
			s.Provenance.AcquiredAt = s.ObservedAt
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool, state := collectorFixture(now)
			tc.change(&state)
			body := render(Snapshot{Buckets: []domain.BucketState{state}}, now, time.Minute, time.Minute, []domain.QuotaPool{pool})
			count := "0"
			if tc.fresh {
				count = "1"
			}
			window := state.Key.Window
			if window == "unmapped" {
				window = "other"
			}
			requireMetric(t, body, "steward_quota_observations{window=\""+window+"\"} "+count)
			if !tc.fresh && strings.Contains(body, "steward_quota_worst_used_ratio{window=") {
				t.Fatal("unqualified old source received collector freshness", body)
			}
		})
	}
}

// Configured model windows share the fixed "other" label; exact catalog/model
// and source names remain internal, while qualified readings are retained.
func TestScopedModelWindowUsesBoundedOtherLabel(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	pool, state := collectorFixture(now)
	pool.Collector.RequiredWindows = append(pool.Collector.RequiredWindows, domain.QuotaCollectorWindow{Window: "weekly_fable", Models: []string{"fixture-model"}})
	state.Key.Window = "weekly_fable"
	body := render(Snapshot{Buckets: []domain.BucketState{state}}, now, time.Minute, time.Minute, []domain.QuotaPool{pool})
	requireMetric(t, body, "steward_quota_observations{window=\"other\"} 1")
	requireMetric(t, body, "steward_quota_worst_used_ratio{window=\"other\"} 0.2")
	if strings.Contains(body, "weekly_fable") || strings.Contains(body, "fixture-model") {
		t.Fatal("configurable model/window identity became a metric label", body)
	}
}

func TestQuotaFreshnessRejectsOriginResetAndFuture(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		change func(*domain.BucketState)
	}{
		{"expired-origin", func(s *domain.BucketState) { at := now; s.OriginValidUntil = &at }},
		{"passed-reset", func(s *domain.BucketState) { reset := now.Add(-time.Minute); s.ResetsAt = &reset }},
		{"future", func(s *domain.BucketState) {
			s.ObservedAt = now.Add(time.Second)
			s.Provenance.AcquiredAt = s.ObservedAt
		}},
		{"zero", func(s *domain.BucketState) { s.ObservedAt = time.Time{}; s.Provenance.AcquiredAt = time.Time{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, state := collectorFixture(now)
			tc.change(&state)
			body := render(Snapshot{Buckets: []domain.BucketState{state}}, now, time.Minute, time.Minute, []domain.QuotaPool{pool})
			requireMetric(t, body, "steward_quota_observations{window=\"five_hour\"} 0")
			requireMetric(t, body, "steward_quota_stale_observations 1")
		})
	}
}

func TestAuthenticatedWorkerOriginLimitsCollectorValidity(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	pool, state := collectorFixture(now)
	state.Provenance.OriginWorkerID = "claimed-origin"
	worker := domain.WorkerSnapshot{
		WorkerID: "fixture-worker", Connected: true, ObservedAt: now.Add(-40 * time.Second), ValidUntil: now.Add(time.Minute),
		QuotaObservations: []domain.WorkerQuotaObservation{{
			Key: state.Key, Provenance: state.Provenance, ObservedAt: state.ObservedAt, UsedPercent: state.UsedPercent, ResetsAt: state.ResetsAt,
		}},
	}
	s := Snapshot{Workers: []domain.WorkerSnapshot{worker}}
	body := render(s, now, time.Minute, time.Minute, []domain.QuotaPool{pool})
	requireMetric(t, body, "steward_quota_observations{window=\"five_hour\"} 1")
	for _, secret := range []string{"fixture-worker", "claimed-origin", pool.AccountID, "fixture-native", "fixture-acquisition", "fixture-instance"} {
		if strings.Contains(body, secret) {
			t.Fatal("internal origin/identity leaked", secret)
		}
	}
	if state.Provenance.OriginWorkerID != "claimed-origin" {
		t.Fatal("merge mutated the input provenance")
	}
	body = render(s, now.Add(20*time.Second), time.Minute, time.Minute, []domain.QuotaPool{pool})
	requireMetric(t, body, "steward_workers{state=\"fresh\"} 1")
	requireMetric(t, body, "steward_quota_observations{window=\"five_hour\"} 0")
	s.Workers[0].WorkerID = "different-worker"
	body = render(s, now, time.Minute, time.Minute, []domain.QuotaPool{pool})
	requireMetric(t, body, "steward_quota_observations{window=\"five_hour\"} 0")
}

func TestQuotaTiesCountUniqueKeysAndKeepWorstValue(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	key := domain.BucketKey{ProviderInstanceID: "fixture-instance", Window: "five_hour"}
	local := []domain.BucketState{
		{Key: key, ObservedAt: now, UsedPercent: 20},
		{Key: key, ObservedAt: now.Add(-time.Second), UsedPercent: 100},
	}
	workers := []domain.WorkerSnapshot{
		{WorkerID: "a", Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Minute), QuotaObservations: []domain.WorkerQuotaObservation{{Key: key, ObservedAt: now, UsedPercent: 90}}},
		{WorkerID: "b", Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Minute), QuotaObservations: []domain.WorkerQuotaObservation{{Key: key, ObservedAt: now, UsedPercent: 70}}},
	}
	for i := 0; i < 2; i++ {
		body := render(Snapshot{Buckets: local, Workers: workers}, now, time.Minute, time.Minute, nil)
		requireMetric(t, body, "steward_quota_observations{window=\"five_hour\"} 1")
		requireMetric(t, body, "steward_quota_worst_used_ratio{window=\"five_hour\"} 0.9")
		workers[0], workers[1] = workers[1], workers[0]
		local[0], local[1] = local[1], local[0]
	}
}

func TestQuotaInvalidCompetitorsRemainDiagnostic(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	key := domain.BucketKey{Window: "five_hour"}
	body := render(Snapshot{Buckets: []domain.BucketState{
		{Key: key, ObservedAt: now, UsedPercent: math.NaN()},
		{Key: key, ObservedAt: now, UsedPercent: math.Inf(1)},
		{Key: key, ObservedAt: now, UsedPercent: 40},
	}}, now, time.Minute, time.Minute, nil)
	requireMetric(t, body, "steward_quota_observations{window=\"five_hour\"} 1")
	requireMetric(t, body, "steward_quota_worst_used_ratio{window=\"five_hour\"} 0.4")
	requireMetric(t, body, "steward_quota_invalid_observations 1")
}

func TestPartialCollectorWindowsDoNotReconcileAdmission(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	pool, state := collectorFixture(now)
	s := Snapshot{Buckets: []domain.BucketState{state}, Admissions: map[string]int64{"closed": 1}}
	if domain.CollectorBatchComplete(pool, s.Buckets) {
		t.Fatal("fixture must be a partial acquisition")
	}
	body := render(s, now, time.Minute, time.Minute, []domain.QuotaPool{pool})
	requireMetric(t, body, "steward_quota_observations{window=\"five_hour\"} 1")
	requireMetric(t, body, "steward_quota_observations{window=\"seven_day\"} 0")
	requireMetric(t, body, "steward_quota_admissions{state=\"closed\"} 1")
	requireMetric(t, body, "steward_quota_admissions{state=\"open\"} 0")
	if s.Admissions["closed"] != 1 || len(s.Admissions) != 1 {
		t.Fatal("metrics mutated admission")
	}
}

func TestCacheOwnsCoverageAndDoesNotRestampSource(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	pool, state := collectorFixture(now)
	pools := []domain.QuotaPool{pool}
	loader := &fixtureLoader{snapshot: Snapshot{Buckets: []domain.BucketState{state}}}
	cache := NewCache(loader, time.Minute, time.Minute, pools)
	cache.now = func() time.Time { return now }
	pools[0].Collector.MaxObservationAge = time.Second
	pools[0].Collector.Origins[0].WorkerID = "different-worker"
	pools[0].Collector.RequiredWindows[0].Models[0] = "different-model"
	pools[0].Models[0] = "different-model"
	pools[0].ProviderInstanceIDs[0] = "different-instance"
	cache.Refresh(context.Background())
	_, body := scrape(cache)
	requireMetric(t, body, "steward_quota_observations{window=\"five_hour\"} 1")
	now = now.Add(MaxSnapshotAge + time.Second)
	if code, body := scrape(cache); code != 503 || strings.Contains(body, "steward_quota_observations") {
		t.Fatal("quota scope widened exporter freshness", code, body)
	}
	cache.Refresh(context.Background())
	_, body = scrape(cache)
	requireMetric(t, body, "steward_quota_observations{window=\"five_hour\"} 1")
	now = now.Add(5 * time.Minute)
	cache.Refresh(context.Background())
	_, body = scrape(cache)
	requireMetric(t, body, "steward_metrics_snapshot_fresh 1")
	requireMetric(t, body, "steward_quota_observations{window=\"five_hour\"} 0")
	requireMetric(t, body, "steward_quota_stale_observations 1")
	if !loader.snapshot.Buckets[0].ObservedAt.Equal(state.ObservedAt) {
		t.Fatal("refresh restamped acquisition")
	}
}
