package main

import (
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"testing"
	"time"
)

func TestCampaignRoleDiversityAndFallback(t *testing.T) {
	p := &routePolicy{Digest: "digest", Roles: []policyRole{
		{Name: "execute", Candidates: []policyCandidate{{Route: "claude/model", Effort: "medium", Tier: "standard"}}},
		{Name: "review", Candidates: []policyCandidate{{Route: "claude/model", Effort: "medium", Tier: "standard"}, {Route: "codex/model", Effort: "high", Tier: "standard"}}},
	}}
	project := backlogadmin.Project{Name: "p", Workers: []backlogadmin.ProjectWorker{
		{Worker: "w", Ready: true, Advertises: true, Enrolled: true, Routes: []backlogadmin.ProjectRoute{{Instance: "claude", Model: "model", ProviderFamily: "claude", Tier: "executor"}, {Instance: "codex", Model: "model", ProviderFamily: "codex", Tier: "executor"}}},
	}}
	tasks := []backlogadmin.ViabilityTask{{Name: "review", Project: "p", Role: "review", Needs: []string{"producer"}}, {Name: "producer", Project: "p", Role: "execute"}}
	selected, failures := resolveCampaignPolicy(p, tasks, []backlogadmin.Project{project}, nil, time.Unix(1, 0))
	if len(failures) != 0 {
		t.Fatal(failures)
	}
	got := selected["review"]
	if got.Route != "codex/model" || !got.Diversity.CrossProvider || got.PolicyDigest != "digest" || got.Effort != "high" {
		t.Fatalf("%+v", got)
	}
	project.Workers[0].Routes = project.Workers[0].Routes[:1]
	selected, failures = resolveCampaignPolicy(p, tasks, []backlogadmin.Project{project}, nil, time.Unix(1, 0))
	if len(failures) != 0 {
		t.Fatal(failures)
	}
	got = selected["review"]
	if got.Route != "claude/model" || got.Diversity.CrossProvider || got.Diversity.Reason != "fallback: no eligible candidate outside claude" {
		t.Fatalf("%+v", got)
	}
	project.Workers[0].Ready = false
	selected, failures = resolveCampaignPolicy(p, tasks, []backlogadmin.Project{project}, nil, time.Unix(1, 0))
	if len(failures) != 0 || selected["producer"].Route != "claude/model" {
		t.Fatalf("%+v %+v", selected, failures)
	}
}
func TestCampaignRoleRaisedEffortDoesNotSkipCandidate(t *testing.T) {
	p := &routePolicy{Digest: "d", Roles: []policyRole{{Name: "read", Candidates: []policyCandidate{{Route: "a/m", Tier: "standard", Effort: "low"}, {Route: "b/m", Tier: "standard", Effort: "high"}}}}}
	project := backlogadmin.Project{Name: "p", Workers: []backlogadmin.ProjectWorker{{Worker: "w", Ready: true, Advertises: true, Enrolled: true, Routes: []backlogadmin.ProjectRoute{{Instance: "a", Model: "m", Tier: "executor"}, {Instance: "b", Model: "m", Tier: "executor"}}}}}
	selected, failures := resolveCampaignPolicy(p, []backlogadmin.ViabilityTask{{Name: "t", Project: "p", Role: "read", RoleEffort: "high"}}, []backlogadmin.Project{project}, nil, time.Now())
	if len(selected) != 0 || failures["t"].Code != "role-effort-raised" {
		t.Fatalf("%+v %+v", selected, failures)
	}
}
