package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/campaign"
)

func TestCampaignLocalRolesAdvisory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "route-policy.yaml")
	policy := "schema: route-policy/v1\nroles:\n  - name: execute\n    candidates:\n      - {route: claudeAgent/exact, effort: high, tier: standard}\n      - {route: codex/exact, effort: medium, tier: standard}\n"
	if err := os.WriteFile(path, []byte(policy), 0600); err != nil {
		t.Fatal(err)
	}
	plan := campaign.Plan{Waves: []campaign.Wave{{Index: 0, Tasks: []string{"work", "review"}}}, Tasks: []campaign.Task{{Name: "work", Role: "execute"}, {Name: "review", Role: "missing"}}}
	var warnings bytes.Buffer
	annotateCampaignLocalRoles(&plan, path, &warnings)
	if len(plan.Tasks[0].LocalPolicyCandidates) != 2 || plan.Tasks[0].LocalPolicyCandidates[0] != "claudeAgent/exact (effort high)" {
		t.Fatalf("candidates = %#v", plan.Tasks[0])
	}
	if !strings.Contains(warnings.String(), "local policy") || !strings.Contains(warnings.String(), "coordinator") || !strings.Contains(warnings.String(), "missing") {
		t.Fatalf("warning = %s", warnings.String())
	}
	if !strings.Contains(campaign.RenderText(plan), "local policy, not live") {
		t.Fatal("missing advisory label")
	}
	// A missing or invalid local policy never becomes a coordinator refusal.
	for _, raw := range []string{"", "malformed: true"} {
		if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		untouched := campaign.Plan{Tasks: []campaign.Task{{Role: "execute"}}}
		annotateCampaignLocalRoles(&untouched, path, &warnings)
		if len(untouched.Tasks[0].LocalPolicyCandidates) != 0 {
			t.Fatal("invalid policy applied")
		}
	}
	untouched := campaign.Plan{Tasks: []campaign.Task{{Role: "execute"}}}
	annotateCampaignLocalRoles(&untouched, path+".missing", nil)
}
