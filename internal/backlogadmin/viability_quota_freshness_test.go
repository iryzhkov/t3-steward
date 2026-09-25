package backlogadmin

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// S2: the admission record of a pool is rewritten only when its admission
// changes, so its observation time froze at the last transition (twelve days
// old on the fleet coordinator) while workers reported the pool's buckets on
// every exchange, and check called every campaign snapshot-stale. Freshness
// is judged by the newest reading, recorded or reported.
func TestQuotaFreshnessUsesTheWorkersNewestReading(t *testing.T) {
	task := domain.Task{ID: "task-1", Routes: []domain.ProviderRoute{{ProviderInstanceID: "t3-primary", Model: "opus", QuotaPoolID: "pool-1"}}}
	staleRecord := func(v *view) {
		v.runtime.MaxQuotaObservationAge = time.Hour
		v.admissions = []domain.QuotaAdmissionRecord{{
			QuotaPoolID: "pool-1", Revision: 1, Admission: domain.AdmissionOpen,
			ObservedAt: viabilityNow.Add(-48 * time.Hour),
		}}
	}
	stale := func(v view) bool {
		worker := v.viabilityWorkers()[0]
		for _, reason := range v.quotaReasons(task, worker) {
			if reason.Code == ReasonSnapshotStale {
				return true
			}
		}
		return false
	}

	if !stale(viabilityView(t, staleRecord)) {
		t.Fatal("a pool nobody has observed for two days was not reported stale")
	}
	reported := viabilityView(t, func(v *view) {
		staleRecord(v)
		v.workers[0].QuotaObservations = []domain.WorkerQuotaObservation{{
			Key:   domain.BucketKey{ProviderInstanceID: "t3-primary", AccountID: "account", LimitID: "seven_day"},
			Phase: domain.PhaseNormal, UsedPercent: 21, Healthy: true,
			ObservedAt: viabilityNow.Add(-time.Minute),
		}}
	})
	if stale(reported) {
		t.Fatal("a pool a worker reported a minute ago was called stale because its admission record was old")
	}
}
