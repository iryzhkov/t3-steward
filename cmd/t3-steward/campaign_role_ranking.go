package main

import (
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

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
