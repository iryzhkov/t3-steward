package main

import (
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"testing"
)

func TestM172LaterCandidateConflictRetainsUsableRoute(t *testing.T) {
	p := rankingPolicy(t)
	project := rankingProject()
	project.Workers = append(project.Workers, backlogadmin.ProjectWorker{
		Worker: "second-worker", Ready: true, Advertises: true,
		Routes: []backlogadmin.ProjectRoute{{Instance: "t3-primary", Model: "opus", Tier: "executor", ProviderFamily: "anthropic", QuotaPool: "pool-claude"}},
	})
	legacy, err := selectPolicyRoute(p, "execute", "", "", "", project, nil)
	if err != nil || legacy.Route != "codex/sol" {
		t.Fatalf("legacy healthy route unexpectedly refused: %+v %v", legacy, err)
	}
	ranked, err := selectPolicyRouteRanked(p, "execute", "", "", "", project, nil, routeRankView{})
	if err == nil && (len(ranked.Candidates) != 2 || ranked.Candidates[1].Eligible || ranked.Candidates[1].Reason == "") {
		t.Fatalf("conflicting candidate must remain visible and ineligible: %+v", ranked.Candidates)
	}
	if err != nil || ranked.Route != legacy.Route {
		t.Fatalf("unknown-quota policy-order fallback must preserve available first route; legacy=%s ranked=%s error=%v", legacy.Route, ranked.Route, err)
	}
}
