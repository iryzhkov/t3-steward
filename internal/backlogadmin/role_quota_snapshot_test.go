package backlogadmin

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"math"
	"strings"
	"testing"
	"time"
)

func TestRoleQuotaSnapshotCoherentFreshnessAndDisabled(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	key := domain.BucketKey{ProviderInstanceID: "codex", LimitID: "test", Window: domain.WindowPrimary}
	for _, mode := range []string{"fresh", "stale", "reset-crossing", "missing", "invalid", "future-window", "future-admission", "missing-admission", "disabled", "closed"} {
		t.Run(mode, func(t *testing.T) {
			pool := domain.QuotaPool{ID: "p", Provider: "codex", Admission: domain.AdmissionOpen, Buckets: []domain.BucketKey{key}}
			worker := domain.WorkerSnapshot{QuotaObservations: []domain.WorkerQuotaObservation{{Key: key, ObservedAt: now, UsedPercent: 10}}}
			admission := domain.QuotaAdmissionRecord{QuotaPoolID: "p", Admission: domain.AdmissionOpen, ObservedAt: now}
			admissions := []domain.QuotaAdmissionRecord{admission}
			switch mode {
			case "stale":
				worker.QuotaObservations[0].ObservedAt = now.Add(-2 * time.Hour)
			case "reset-crossing":
				reset := now.Add(-time.Second)
				worker.QuotaObservations[0].ResetsAt = &reset
				worker.QuotaObservations[0].ObservedAt = now.Add(-time.Minute)
			case "missing":
				worker.QuotaObservations = nil
			case "invalid":
				worker.QuotaObservations[0].UsedPercent = math.NaN()
			case "future-window":
				worker.QuotaObservations[0].ObservedAt = now.Add(time.Minute)
			case "future-admission":
				admissions[0].ObservedAt = now.Add(time.Minute)
			case "missing-admission":
				admissions = nil
			case "disabled":
				pool.ChecksDisabled = true
				pool.Admission = domain.AdmissionClosed
				admissions[0].Admission = domain.AdmissionClosed
			case "closed":
				admissions[0].Admission = domain.AdmissionClosed
			}
			v := newView(sqlite.CoordinatorRecords{QuotaPools: []domain.QuotaPool{pool}}, []domain.WorkerSnapshot{worker}, admissions, RuntimeInfo{MaxQuotaObservationAge: time.Hour}, now)
			snapshot := v.roleQuotaSnapshot()
			if snapshot.Now != now {
				t.Fatal("snapshot clock diverged")
			}
			ranked, err := domain.RankRoutes(domain.RouteRankInput{Version: domain.RouteRankingV1, Now: snapshot.Now, Candidates: []domain.RouteRankCandidate{{Route: "codex/m", Pools: []domain.RouteRankPool{snapshot.Pools["p"]}}}})
			if err != nil {
				t.Fatal(err)
			}
			// Window freshness alone decides headroom: an old or absent
			// admission record does not erase fresh windows, and disabled
			// checks neither gate on retained admission nor hide readings.
			want, freshness := "unknown", QuotaStale
			switch mode {
			case "fresh", "future-admission", "missing-admission", "disabled":
				want, freshness = "healthy", QuotaFresh
			case "closed":
				want, freshness = "gated", QuotaFresh
			case "missing":
				freshness = QuotaMissing
			case "invalid":
				freshness = QuotaFresh
			}
			if ranked[0].Band != want || snapshot.ChecksDisabled["p"] != (mode == "disabled") || snapshot.Freshness["p"].State != freshness {
				t.Fatalf("snapshot=%+v ranked=%+v want=%s/%s", snapshot, ranked, want, freshness)
			}
			if mode == "stale" && (snapshot.Freshness["p"].Age != 2*time.Hour || !strings.Contains(snapshot.Freshness["p"].String(), "p quota stale, observed 2h0m0s ago (maximum age 1h0m0s)")) {
				t.Fatalf("stale quota reported without its age: %s", snapshot.Freshness["p"])
			}
			// Returned evidence cannot change the coordinator snapshot.
			got := snapshot.Pools["p"]
			if len(got.Windows.Windows) > 0 {
				got.Windows.Windows[0].UsedPercent = 55
			}
			if mode != "missing" && len(worker.QuotaObservations) > 0 && mode != "invalid" && worker.QuotaObservations[0].UsedPercent != 10 {
				t.Fatal("snapshot aliases observations")
			}
		})
	}
}
