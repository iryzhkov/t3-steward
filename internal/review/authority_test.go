package review

import (
	"strings"
	"testing"
)

func authorityRequirementsSpec() RequirementsSpec {
	return RequirementsSpec{Risk: "routine", CriteriaDigest: strings.Repeat("a", 64), PolicyDigest: strings.Repeat("b", 64), RequiredReviewers: 2, MinProviderFamilies: 2, Members: []MemberRequirement{
		{ID: "one", Role: "independent", Route: "codex/sol", ProviderFamily: "openai", Tier: "executor", Required: true},
		{ID: "two", Role: "independent", Route: "other/model", ProviderFamily: "other", Tier: "executor", Required: true},
	}}
}
func TestAuthorityRequirementsCanonicalAndDefensive(t *testing.T) {
	spec := authorityRequirementsSpec()
	r, err := NewRequirements(spec)
	if err != nil {
		t.Fatal(err)
	}
	digest := r.Digest()
	spec.Members[0].Route = "changed/model"
	out := r.Snapshot()
	out.Members[0].Required = false
	if r.Digest() != digest || r.Snapshot().Members[0].Route != "codex/sol" {
		t.Fatal("mutable freeze")
	}
	reversed := authorityRequirementsSpec()
	reversed.Members[0], reversed.Members[1] = reversed.Members[1], reversed.Members[0]
	reversed.RoundLimit = 2
	r2, err := NewRequirements(reversed)
	if err != nil || r2.Digest() != digest {
		t.Fatalf("noncanonical digest: %v", err)
	}
	risky := authorityRequirementsSpec()
	risky.Risk = "risky"
	risky.Members[0].Tier = "critical"
	rr, err := NewRequirements(risky)
	if err != nil || rr.Snapshot().RoundLimit != 3 {
		t.Fatalf("risky default: %v", err)
	}
}
func TestAuthorityRequirementsRefuseWeakOrInvalidPolicy(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*RequirementsSpec)
	}{
		{"risk", func(s *RequirementsSpec) { s.Risk = "critical" }},
		{"criteria", func(s *RequirementsSpec) { s.CriteriaDigest = "bad" }},
		{"policy", func(s *RequirementsSpec) { s.PolicyDigest = strings.Repeat("B", 64) }},
		{"count", func(s *RequirementsSpec) { s.RequiredReviewers = 1 }},
		{"role", func(s *RequirementsSpec) { s.Members[0].Role = "review" }},
		{"duplicate", func(s *RequirementsSpec) { s.Members[1].ID = s.Members[0].ID }},
		{"diversity", func(s *RequirementsSpec) { s.Members[1].ProviderFamily = "openai" }},
		{"waiver", func(s *RequirementsSpec) { s.MinProviderFamilies = 1 }},
		{"risky-tier", func(s *RequirementsSpec) { s.Risk = "risky" }},
		{"limit", func(s *RequirementsSpec) { s.RoundLimit = 4 }},
		{"route", func(s *RequirementsSpec) { s.Members[0].Route = "bad" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := authorityRequirementsSpec()
			tc.mutate(&s)
			if _, err := NewRequirements(s); err == nil {
				t.Fatal("invalid policy accepted")
			}
		})
	}
}
func TestAuthorityBindingAndCheckpointValidation(t *testing.T) {
	p := ParentBinding{RunID: "run", TaskID: "task", AttemptID: "attempt", ThreadID: "thread", AssignmentID: "assignment", AssignmentEpoch: 1, IssuedRevision: 1, Repository: "repo", BaseCommit: strings.Repeat("a", 40), ExecutorRoute: "codex/sol"}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, commit := range []string{"abc", strings.Repeat("A", 40), "", strings.Repeat("0", 41)} {
		bad := p
		bad.BaseCommit = commit
		if bad.Validate() == nil {
			t.Fatal("bad commit accepted")
		}
	}
	c := Checkpoint{ID: "cp", HeadCommit: strings.Repeat("c", 40), InputDigest: strings.Repeat("d", 64)}
	if c.Validate() != nil {
		t.Fatal("valid checkpoint refused")
	}
	c.ID = "../cp"
	if c.Validate() == nil {
		t.Fatal("unsafe checkpoint accepted")
	}
}
