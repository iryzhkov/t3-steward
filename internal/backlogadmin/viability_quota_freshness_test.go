package backlogadmin

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// S2: the fleet coordinator runs with quota_checks: false. It then neither
// maintains nor enforces admission, and the view drops the retained admission
// records, so a check fell back to the pool record with no observation time at
// all and called every campaign snapshot-stale. A pool whose checks are
// disabled contributes no quota reasons; an enforced pool is judged as before.
func TestQuotaReasonsSkipAPoolWhoseChecksAreDisabled(t *testing.T) {
	task := domain.Task{ID: "task-1", Routes: []domain.ProviderRoute{{ProviderInstanceID: "t3-primary", Model: "opus", QuotaPoolID: "pool-1"}}}
	reasons := func(v view) []ViabilityReason {
		return v.quotaReasons(task, v.viabilityWorkers()[0])
	}
	has := func(list []ViabilityReason, code string) bool {
		for _, reason := range list {
			if reason.Code == code {
				return true
			}
		}
		return false
	}

	enforced := viabilityView(t, func(v *view) {
		v.runtime.MaxQuotaObservationAge = time.Hour
		v.admissions = []domain.QuotaAdmissionRecord{{
			QuotaPoolID: "pool-1", Revision: 1, Admission: domain.AdmissionOpen,
			ObservedAt: viabilityNow.Add(-48 * time.Hour),
		}}
	})
	if !has(reasons(enforced), ReasonSnapshotStale) {
		t.Fatal("an enforced pool nobody has observed for two days was not reported stale")
	}

	disabled := viabilityViewWith(t, func(records *sqlite.CoordinatorRecords) {
		records.QuotaPools[0].ChecksDisabled = true
	}, func(v *view) { v.runtime.MaxQuotaObservationAge = time.Hour })
	if got := reasons(disabled); len(got) != 0 {
		t.Fatalf("a pool whose quota checks are disabled produced quota reasons: %+v", got)
	}
}
