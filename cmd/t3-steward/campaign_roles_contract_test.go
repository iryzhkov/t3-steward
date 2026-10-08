package main

import (
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCampaignRolePolicyUnavailableAndRoleFreeDoesNotRead(t *testing.T) {
	resolver := coordinatorRoleResolver{PolicyPath: filepath.Join(t.TempDir(), "policy")}
	tasks := []backlogadmin.ViabilityTask{{Name: "t", Project: "p", Role: "execute"}}
	_, failures := resolver.Resolve(context.Background(), tasks, nil, nil)
	if failures["t"].Code != "route-policy-unavailable" || !strings.Contains(failures["t"].Detail, resolver.PolicyPath) {
		t.Fatal(failures)
	}
	if err := os.WriteFile(resolver.PolicyPath, []byte("bad: true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	_, failures = resolver.Resolve(context.Background(), tasks, nil, nil)
	if failures["t"].Code != "route-policy-unavailable" {
		t.Fatal(failures)
	}
	tasks[0].Role = ""
	_, failures = resolver.Resolve(context.Background(), tasks, nil, nil)
	if len(failures) != 0 {
		t.Fatal(failures)
	}
}
func TestCampaignRoleRequestCompatibilityAndUpgrade(t *testing.T) {
	plan := campaign.Plan{Tasks: []campaign.Task{{Name: "explicit", Needs: []string{"producer"}, ReviewType: true}}}
	request, err := campaignViabilityRequest(plan, 0, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(request)
	for _, field := range []string{"role", "roleEffort", "needs", "producers", "reviewType"} {
		if strings.Contains(string(raw), "\""+field+"\"") {
			t.Fatalf("new field in role-free request: %s", raw)
		}
	}
	plan.Tasks[0].Role = "review"
	plan.Tasks = append(plan.Tasks, campaign.Task{Name: "producer"})
	request, err = campaignViabilityRequest(plan, 0, 0, "explicit")
	if err != nil || len(request.Tasks) != 2 {
		t.Fatalf("producer context missing: %+v %v", request, err)
	}
	c := campaignCLI{viability: func(context.Context, backlogadmin.ViabilityRequest) (backlogadmin.ViabilityMatrix, error) {
		return backlogadmin.ViabilityMatrix{}, nil
	}}
	_, err = c.checkViability(context.Background(), plan, campaign.Bundle{}, "")
	if err == nil || !strings.Contains(err.Error(), "upgrade the coordinator") {
		t.Fatal(err)
	}
}
func TestCampaignRoleReviewOutputAndExplicitProducerDiversity(t *testing.T) {
	p := &routePolicy{Digest: "d", Roles: []policyRole{{Name: "read", Candidates: []policyCandidate{{Route: "a/m", Tier: "standard", Effort: "low"}, {Route: "b/m", Tier: "standard", Effort: "medium"}}}}}
	project := backlogadmin.Project{Name: "p", Workers: []backlogadmin.ProjectWorker{{Worker: "w", Ready: true, Advertises: true, Routes: []backlogadmin.ProjectRoute{{Instance: "a", Model: "m", Tier: "executor", ProviderFamily: "claude"}, {Instance: "b", Model: "m", Tier: "executor", ProviderFamily: "codex"}}}}}
	routes := campaignProviderRoutes([]campaign.Route{{Instance: "a", Model: "m"}})
	tasks := []backlogadmin.ViabilityTask{{Name: "producer", Project: "p", Routes: routes}, {Name: "review", Project: "p", Role: "read", ReviewType: true, Producers: []string{"producer"}, RoleEffort: "low"}}
	selected, failures := resolveCampaignPolicy(p, tasks, []backlogadmin.Project{project}, nil, time.Now())
	if len(failures) != 0 || selected["review"].Route != "b/m" || !selected["review"].Diversity.CrossProvider || selected["review"].Effort != "low" {
		t.Fatalf("%+v %+v", selected, failures)
	}
	tasks[1].Role = ""
	tasks[1].Routes = routes
	selected, failures = resolveCampaignPolicy(p, tasks, []backlogadmin.Project{project}, nil, time.Now())
	if len(selected) != 0 || len(failures) != 0 {
		t.Fatalf("explicit route received preference: %+v %+v", selected, failures)
	}
}
