package main

import (
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"strings"
	"testing"
)

func TestCampaignRoleCandidateVerdictExplainsTierMismatch(t *testing.T) {
	p := &routePolicy{Roles: []policyRole{{Name: "review", Candidates: []policyCandidate{{Route: "a/m", Tier: "premium", Effort: "high"}, {Route: "b/m", Tier: "standard", Effort: "medium"}}}}}
	project := backlogadmin.Project{Name: "p", Workers: []backlogadmin.ProjectWorker{{Worker: "w", Ready: true, Advertises: true, Routes: []backlogadmin.ProjectRoute{{Instance: "a", Model: "m", Tier: "executor"}, {Instance: "b", Model: "m", Tier: "executor"}}}}}
	selected, verdicts, err := selectPolicyRouteDetailed(p, "review", "", "", "", project, nil)
	if err != nil || selected.Route != "b/m" || len(verdicts) != 2 || verdicts[0].Eligible || !strings.Contains(verdicts[0].Reason, "catalog tier standard does not match policy tier premium") {
		t.Fatalf("%+v %+v %v", selected, verdicts, err)
	}
}
