package main

import (
	"bytes"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
)

func TestCampaignShowRoleReceiptText(t *testing.T) {
	var out bytes.Buffer
	selection := &domain.RoleSelection{Role: "review", Route: "codex/model", Effort: "low", PolicyDigest: "digest", Reason: "first eligible", Candidates: []domain.RoleCandidateVerdict{{Route: "claude/model", Reason: "unavailable"}}, Diversity: domain.RoleDiversity{CrossProvider: true, Reason: "cross-provider: codex"}}
	renderWorkflow(&out, &backlogadmin.WorkflowDetail{Tasks: []backlogadmin.TaskDetail{{Task: domain.Task{ID: "t", Name: "review", RoleSelection: selection}}}})
	document := campaignCheck{Matrix: backlogadmin.ViabilityMatrix{Tasks: []backlogadmin.ViabilityTaskResult{{Task: "review", RoleSelection: selection}}}}
	if err := renderCampaignCheck(&out, document); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"roleSelection", "crossProvider", "codex/model", "digest", "unavailable"} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("missing %s: %s", want, raw)
		}
	}
	for _, want := range []string{"role: review", "codex/model", "effort: low", "digest", "claude/model", "unavailable", "cross-provider"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %s: %s", want, out.String())
		}
	}
}
