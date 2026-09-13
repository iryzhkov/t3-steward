package backlog

import (
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// disabledReport grants admission by explicit operator policy, without claiming
// quota was observed healthy. Concurrency remains conservative and fleet-wide.
func (b QuotaBridge) disabledReport(assignments []domain.Assignment) (QuotaBridgeReport, error) {
	pools, _, err := quotaBridgePools(b.Pools)
	if err != nil {
		return QuotaBridgeReport{}, err
	}
	now := time.Now().UTC()
	if b.Now != nil {
		now = b.Now().UTC()
	}
	counts := map[string]int{}
	seen := map[string]bool{}
	for _, a := range assignments {
		if a.ID == "" || seen[a.ID] {
			return QuotaBridgeReport{}, fmt.Errorf("invalid or repeated assignment %q", a.ID)
		}
		seen[a.ID] = true
		if a.State != domain.AssignmentCompleted && a.State != domain.AssignmentReleased {
			counts[a.Route.QuotaPoolID]++
		}
	}
	report := QuotaBridgeReport{ChecksDisabled: true, Pools: pools}
	for i := range report.Pools {
		p := &report.Pools[i]
		p.ChecksDisabled = true
		p.Admission = domain.AdmissionOpen
		p.UpdatedAt = now
		p.ActiveAssignments = counts[p.ID]
		report.Derived = append(report.Derived, QuotaPoolAdmissionSnapshot{
			QuotaPoolID: p.ID, Admission: domain.AdmissionOpen, ObservedAt: now,
			Reason: "quota checks disabled by coordinator policy",
		})
	}
	return report, nil
}
