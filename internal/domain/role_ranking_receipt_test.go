package domain

import (
	"encoding/json"
	"testing"
)

func TestRoleRankingReceiptRoundTripAndClone(t *testing.T) {
	original := RoleSelection{Role: "review", Route: "codex/m", Effort: "medium", PolicyDigest: "policy", Reason: "healthy", Ranking: RouteRankingV1, Candidates: []RoleCandidateVerdict{{Route: "claude/m", Eligible: true, Band: "gated", Pool: "claude", Reason: "closed"}, {Route: "codex/m", Ordinal: 1, Eligible: true, Band: "healthy", Pool: "codex", Reason: "healthy"}}, Diversity: RoleDiversity{ProducerFamilies: []string{"anthropic"}}}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded RoleSelection
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Ranking != RouteRankingV1 || decoded.Candidates[1].Ordinal != 1 || decoded.Candidates[1].Pool != "codex" || decoded.Candidates[0].Band != "gated" {
		t.Fatalf("roundtrip lost ranking: %+v", decoded)
	}
	clone := CloneRoleSelection(decoded)
	clone.Candidates[0].Band = "healthy"
	clone.Diversity.ProducerFamilies[0] = "openai"
	if decoded.Candidates[0].Band != "gated" || decoded.Diversity.ProducerFamilies[0] != "anthropic" {
		t.Fatal("clone aliases ranking receipt")
	}
	task := ApplyRoleSelection(Task{}, decoded)
	task.RoleSelection.Candidates[0].Pool = "changed"
	if decoded.Candidates[0].Pool != "claude" {
		t.Fatal("applied receipt aliases original")
	}
}
