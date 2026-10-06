package backlog

import (
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// F6: a resume permitted because quota checks are disabled was labelled
// "quota pool X recovered", which claims a reading nobody took. Only an
// admission derived from observed readings says recovered; a disabled-checks
// resume says what permitted it and that recovery was not observed.
func TestPlanThrottleResumesLabelsRecoveryOnlyFromObservedReadings(t *testing.T) {
	input := quotaRecoveryFixture()
	records := []domain.ThrottleAttemptRecord{recoveryThrottle(input.ThrottleRecords, "paused")}
	for _, tc := range []struct {
		name      string
		pool      domain.QuotaPool
		admission domain.QuotaAdmissionRecord
		want      string
		recovered bool
	}{
		{
			name:      "observed healthy readings",
			pool:      domain.QuotaPool{ID: "shared", MaxConcurrent: 2},
			admission: domain.QuotaAdmissionRecord{QuotaPoolID: "shared", Revision: 2, Admission: domain.AdmissionOpen, Reason: "all quota buckets are healthy"},
			want:      "quota pool shared recovered: all quota buckets are healthy",
			recovered: true,
		},
		{
			name:      "quota checks disabled",
			pool:      domain.QuotaPool{ID: "shared", MaxConcurrent: 2, ChecksDisabled: true},
			admission: domain.QuotaAdmissionRecord{QuotaPoolID: "shared", Revision: 2, Admission: domain.AdmissionOpen, Reason: "quota checks disabled by coordinator policy"},
			want:      "quota resume permitted for pool shared: quota checks disabled by coordinator policy; recovery not observed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, commands, err := PlanThrottleResumes(records, []domain.QuotaAdmissionRecord{tc.admission}, []domain.QuotaPool{tc.pool}, throttleDeliveryTime.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if len(commands) != 1 || commands[0].Reason != tc.want {
				t.Fatalf("resume commands = %#v, want reason %q", commands, tc.want)
			}
			if strings.Contains(commands[0].Reason, "recovered") != tc.recovered {
				t.Fatalf("reason %q: claims recovery = %v, want %v", commands[0].Reason, !tc.recovered, tc.recovered)
			}
		})
	}
}
