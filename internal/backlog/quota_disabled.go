package backlog

import (
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// disabledReport grants admission by explicit operator policy, without claiming
// quota was observed healthy. Concurrency remains conservative and fleet-wide.
//
// Occupancy is reconstructed by the same derivation the enabled path uses,
// rather than by a rule of its own. Counting every assignment that was not
// completed or released ignored whether its attempt still holds a provider
// slot, so an attempt parked on an external wait kept the slot the H4 contract
// says it releases: on a one-slot pool a second campaign stayed ready and
// unassigned for the whole park, while explain called the task eligible. An
// operator had to debug that contradiction from scratch.
//
// This path is the one that actually runs. A synthetic provider produces no
// quota observations, so enabling checks closes admission entirely, and every
// disposable test and every quota-free deployment takes this branch. Agreeing
// with the enabled path by construction is the only way the two cannot drift.
func (b QuotaBridge) disabledReport(input QuotaPlanningStateInput) (QuotaBridgeReport, error) {
	pools, _, err := quotaBridgePools(b.Pools)
	if err != nil {
		return QuotaBridgeReport{}, err
	}
	now := time.Now().UTC()
	if b.Now != nil {
		now = b.Now().UTC()
	}
	// The identity check keeps this path's own refusal and its wording, which
	// is stricter than the derivation's about an assignment with no ID.
	seen := map[string]bool{}
	for _, a := range input.Assignments {
		if a.ID == "" || seen[a.ID] {
			return QuotaBridgeReport{}, fmt.Errorf("invalid or repeated assignment %q", a.ID)
		}
		seen[a.ID] = true
	}
	input.QuotaPools = pools
	// Retained throttle records are not consulted while checks are disabled, and
	// the coordinator does not load them on this path at all. Passing them on
	// would make a contradictory record refuse a report that does not depend on
	// it; occupancy is reconstructed from attempts and assignments alone.
	input.ThrottleRecords = nil
	state, err := DeriveQuotaPlanningState(input)
	if err != nil {
		return QuotaBridgeReport{}, fmt.Errorf("reconstruct quota planning state: %w", err)
	}
	report := QuotaBridgeReport{ChecksDisabled: true, Pools: state.QuotaPools}
	for i := range report.Pools {
		p := &report.Pools[i]
		p.ChecksDisabled = true
		p.Admission = domain.AdmissionOpen
		p.UpdatedAt = now
		report.Derived = append(report.Derived, QuotaPoolAdmissionSnapshot{
			QuotaPoolID: p.ID, Admission: domain.AdmissionOpen, ObservedAt: now,
			Reason: "quota checks disabled by coordinator policy",
		})
	}
	return report, nil
}
