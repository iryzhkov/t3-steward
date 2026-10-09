package main

import (
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// campaignTaskRankView composes the existing surplus admission gate before
// preference ranking. Encode that gate in v1's existing closed band while keeping
// its actual constrained/recovering reason; never modify the request snapshot.
func campaignTaskRankView(task backlogadmin.ViabilityTask, snapshot backlogadmin.RoleQuotaSnapshot) routeRankView {
	view := routeRankView{Now: snapshot.Now, Pools: snapshot.Pools, Freshness: snapshot.Freshness}
	if task.Class != domain.TaskClassSurplus {
		return view
	}
	view.Pools = make(map[string]domain.RouteRankPool, len(snapshot.Pools))
	view.PoolGateReasons = map[string]string{}
	for id, pool := range snapshot.Pools {
		if !snapshot.ChecksDisabled[id] && (pool.Admission == domain.AdmissionConstrained || pool.Admission == domain.AdmissionRecovering) {
			view.PoolGateReasons[id] = "admission " + string(pool.Admission) + " for surplus"
			pool.Admission = domain.AdmissionClosed
		}
		view.Pools[id] = pool
	}
	return view
}

// Diversity never moves out of the winning v1 band or promotes an enforced
// unknown/gated pool. ChecksDisabled is explicit operator configuration.
func campaignDiversityUsable(candidate domain.RoleCandidateVerdict, selected policySelection, snapshot backlogadmin.RoleQuotaSnapshot) bool {
	band := ""
	for _, entry := range selected.Candidates {
		if entry.Route == selected.Route {
			band = entry.Band
			break
		}
	}
	return candidate.Band == band && (band == "healthy" || band == "reset-soon" || band == "unknown" && snapshot.ChecksDisabled[candidate.Pool])
}
