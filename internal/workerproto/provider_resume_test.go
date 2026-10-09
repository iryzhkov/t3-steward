package workerproto

import (
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A resume policy outside the ceilings, or with an unusable closed pool, is
// refused whole before the worker applies it.
func TestValidateSnapshotRequestChecksTheProviderResumePolicy(t *testing.T) {
	valid := &ProviderResumePolicy{MaxResumes: 3, MaxDelaySeconds: 900, ClosedPools: []ClosedQuotaPool{{PoolID: "codex-main", Admission: domain.AdmissionClosed}}}
	if err := ValidateSnapshotRequest(SnapshotRequest{ProviderResume: valid}); err != nil {
		t.Fatal(err)
	}
	tooMany := make([]ClosedQuotaPool, MaxClosedQuotaPools+1)
	for index := range tooMany {
		tooMany[index] = ClosedQuotaPool{PoolID: "pool", Admission: domain.AdmissionClosed}
	}
	for name, policy := range map[string]*ProviderResumePolicy{
		"negative resumes":   {MaxResumes: -1},
		"too many resumes":   {MaxResumes: domain.MaxProviderResumes + 1},
		"delay over ceiling": {MaxResumes: 1, MaxDelaySeconds: 7 * 3600},
		"pool without id":    {MaxResumes: 1, ClosedPools: []ClosedQuotaPool{{Admission: domain.AdmissionClosed}}},
		"too many pools":     {MaxResumes: 1, ClosedPools: tooMany},
	} {
		if err := ValidateSnapshotRequest(SnapshotRequest{ProviderResume: policy}); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
