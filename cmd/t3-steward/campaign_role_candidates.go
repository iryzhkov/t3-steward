package main

import (
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"strings"
)

func campaignPolicyCandidateReason(candidate policyCandidate, role policyRole, project backlogadmin.Project, worker string, accept func(string) bool, selectionErr error) string {
	if !strings.Contains(selectionErr.Error(), "no eligible candidate") {
		return selectionErr.Error()
	}
	eligible := project
	eligible.Workers = nil
	for _, w := range project.Workers {
		if w.Ready && w.Advertises && (worker == "" || worker == w.Worker) {
			eligible.Workers = append(eligible.Workers, w)
		}
	}
	route, err := deriveTaskRunRoute(candidate.Route, worker, "", eligible)
	if err != nil {
		return "no eligible worker advertises this route: " + err.Error()
	}
	pair := route.Instance + "/" + route.Model
	if accept != nil && !accept(pair) {
		return "route is excluded by the selection predicate"
	}
	var metadata backlogadmin.ProjectRoute
	for _, w := range eligible.Workers {
		for _, r := range w.Routes {
			if r.Instance+"/"+r.Model == pair {
				metadata = r
			}
		}
	}
	tier := ""
	switch metadata.Tier {
	case "executor":
		tier = "standard"
	case "critical":
		tier = "premium"
	case "economy":
		tier = "economy"
	}
	if tier == "" && len(role.Constraints.Tiers) > 0 {
		return "catalog tier is unknown; role requires a known tier"
	}
	if tier != "" && tier != candidate.Tier {
		return fmt.Sprintf("catalog tier %s does not match policy tier %s", tier, candidate.Tier)
	}
	cs := role.Constraints
	if len(cs.ProviderFamilies) > 0 && (metadata.ProviderFamily == "" || !contains(cs.ProviderFamilies, metadata.ProviderFamily)) {
		return fmt.Sprintf("provider family %q does not match role provider_families", metadata.ProviderFamily)
	}
	if len(cs.ExcludeProviderFamilies) > 0 && (metadata.ProviderFamily == "" || contains(cs.ExcludeProviderFamilies, metadata.ProviderFamily)) {
		return fmt.Sprintf("provider family %q is excluded by role exclude_provider_families", metadata.ProviderFamily)
	}
	if len(cs.Tiers) > 0 && !contains(cs.Tiers, tier) {
		return fmt.Sprintf("catalog tier %q is excluded by role tiers", tier)
	}
	return selectionErr.Error()
}
