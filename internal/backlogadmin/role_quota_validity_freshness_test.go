package backlogadmin

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"math"
	"testing"
	"time"
)

func TestRoleQuotaInvalidityDoesNotEraseObservationPresenceOrTime(t *testing.T) {
	now := time.Date(2026, 10, 13, 20, 0, 0, 0, time.UTC)
	key := domain.BucketKey{ProviderInstanceID: "codex", LimitID: "limit", Window: domain.WindowPrimary}
	pool := domain.QuotaPool{ID: "pool", Provider: "codex", Admission: domain.AdmissionOpen, Buckets: []domain.BucketKey{key}}
	for _, used := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), -1, 101} {
		for _, stale := range []bool{false, true} {
			at := now
			if stale {
				at = now.Add(-2 * time.Hour)
			}
			worker := domain.WorkerSnapshot{QuotaObservations: []domain.WorkerQuotaObservation{{Key: key, ObservedAt: at, UsedPercent: used}}}
			snapshot := BuildRoleQuotaSnapshot(RoleQuotaInput{Now: now, MaxObservationAge: time.Hour, Pools: []domain.QuotaPool{pool}, Workers: []domain.WorkerSnapshot{worker}})
			windows := snapshot.Pools[pool.ID].Windows
			if windows.Complete() || windows.Fresh() {
				t.Fatal("invalid numeric data gained admission evidence")
			}
			problem, bad := windows.Problem()
			if !bad || problem.Code != domain.QuotaWindowUnknown {
				t.Fatal("invalid evidence lost fail-closed unknown status")
			}
			ranked, err := domain.RankRoutes(domain.RouteRankInput{Version: domain.RouteRankingV1, Now: now, Candidates: []domain.RouteRankCandidate{{Route: "codex/model", Pools: []domain.RouteRankPool{snapshot.Pools[pool.ID]}}}})
			if err != nil || ranked[0].Band != "unknown" {
				t.Fatalf("invalid numeric data gained ranking headroom: %v %+v", err, ranked)
			}
			want := QuotaFresh
			if stale {
				want = QuotaStale
			}
			if f := snapshot.Freshness[pool.ID]; f.State != want || !f.ObservedAt.Equal(at) {
				t.Fatalf("invalid evidence lost temporal facts: %+v want%s", f, want)
			}
		}
	}
	// An actual absent window still reports missing even when a present window
	// has invalid data. Unknown validity must not hide structural absence either.
	secondary := domain.BucketKey{ProviderInstanceID: "codex", LimitID: "limit", Window: domain.WindowSecondary}
	pool.Buckets = append(pool.Buckets, secondary)
	snapshot := BuildRoleQuotaSnapshot(RoleQuotaInput{Now: now, MaxObservationAge: time.Hour, Pools: []domain.QuotaPool{pool}, Workers: []domain.WorkerSnapshot{{QuotaObservations: []domain.WorkerQuotaObservation{{Key: key, ObservedAt: now, UsedPercent: math.NaN()}}}}})
	if snapshot.Freshness[pool.ID].State != QuotaMissing {
		t.Fatal("actual missing required window reported present")
	}
}
