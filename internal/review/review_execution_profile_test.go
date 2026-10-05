package review

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func executionSpec() RequirementsSpec {
	spec := authorityRequirementsSpec()
	for i := range spec.Members {
		spec.Members[i].Execution = &domain.ReviewExecutionProfile{Effort: "medium", QuotaPoolID: "review-pool", MaxTurns: 9, Resources: domain.ResourceDemand{MinCPUClass: domain.CPUClassMedium, PreferredCPUClass: domain.CPUClassHigh, CPUUnits: 1.5, MemoryMB: 512, ScratchMB: 64}}
	}
	return spec
}
func executionChanges() map[string]func(*domain.ReviewExecutionProfile) {
	return map[string]func(*domain.ReviewExecutionProfile){
		"effort":     func(p *domain.ReviewExecutionProfile) { p.Effort = "high" },
		"pool":       func(p *domain.ReviewExecutionProfile) { p.QuotaPoolID = "another-pool" },
		"turns":      func(p *domain.ReviewExecutionProfile) { p.MaxTurns++ },
		"floor":      func(p *domain.ReviewExecutionProfile) { p.Resources.MinCPUClass = domain.CPUClassLow },
		"preference": func(p *domain.ReviewExecutionProfile) { p.Resources.PreferredCPUClass = domain.CPUClassMedium },
		"cpu":        func(p *domain.ReviewExecutionProfile) { p.Resources.CPUUnits++ },
		"memory":     func(p *domain.ReviewExecutionProfile) { p.Resources.MemoryMB++ },
		"scratch":    func(p *domain.ReviewExecutionProfile) { p.Resources.ScratchMB++ },
	}
}
func TestReviewExecutionProfileEveryFieldDigestAndIsolation(t *testing.T) {
	spec := executionSpec()
	r, err := NewRequirements(spec)
	if err != nil {
		t.Fatal(err)
	}
	original := r.Digest()
	declaration := domain.Task{ReviewRequirements: &domain.TaskReviewRequirements{Version: 2}}
	for _, m := range r.Snapshot().Members {
		declaration.ReviewRequirements.Members = append(declaration.ReviewRequirements.Members, domain.TaskReviewMember{ID: m.ID, Execution: domain.CloneReviewExecution(m.Execution)})
	}
	declaredDigest := domain.TaskDigest(declaration)
	parent := ParentBinding{RunID: "run", TaskID: "task", AttemptID: "attempt", ThreadID: "thread", AssignmentID: "assignment", AssignmentEpoch: 1, IssuedRevision: 1, Repository: "repo", BaseCommit: strings.Repeat("c", 40), ExecutorRoute: "codex/sol"}
	f, err := NewFrozenAuthority(parent, r)
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range executionChanges() {
		t.Run(name, func(t *testing.T) {
			altered := r.Snapshot()
			change(altered.Members[0].Execution)
			next, err := NewRequirements(altered)
			if err != nil || next.Digest() == original {
				t.Fatal("profile field absent from authority digest", err)
			}
			d := declaration
			d.ReviewRequirements = domain.CloneTaskReview(declaration.ReviewRequirements)
			change(d.ReviewRequirements.Members[0].Execution)
			if domain.TaskDigest(d) == declaredDigest {
				t.Fatal("profile field absent from declaration digest")
			}
		})
	}
	spec.Members[0].Execution.Effort = "high"
	snapshot := r.Snapshot()
	snapshot.Members[0].Execution.MaxTurns++
	f.Requirements.Members[0].Execution.Resources.CPUUnits++
	if r.Digest() != original || r.Snapshot().Members[0].Execution.Effort != "medium" || r.Snapshot().Members[0].Execution.MaxTurns != 9 || r.Snapshot().Members[0].Execution.Resources.CPUUnits != 1.5 || r.Snapshot().Members[1].Execution.MaxTurns != 9 {
		t.Fatal("profile pointer alias")
	}
	canon, err := NewFrozenAuthority(parent, r)
	if err != nil {
		t.Fatal(err)
	}
	detached, err := canon.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	detached.Requirements.Members[0].Execution.MaxTurns++
	if canon.Requirements.Members[0].Execution.MaxTurns != 9 {
		t.Fatal("canonical authority aliases")
	}
	mixed := executionSpec()
	mixed.Members[0].Execution = nil
	if _, err := NewRequirements(mixed); err == nil {
		t.Fatal("mixed profiles accepted")
	}
	for _, change := range []func(*domain.ReviewExecutionProfile){
		func(p *domain.ReviewExecutionProfile) { p.Effort = "max" },
		func(p *domain.ReviewExecutionProfile) { p.QuotaPoolID = "" },
		func(p *domain.ReviewExecutionProfile) { p.MaxTurns = 0 },
		func(p *domain.ReviewExecutionProfile) { p.Resources.MinCPUClass = "" },
	} {
		s := executionSpec()
		change(s.Members[0].Execution)
		if _, err := NewRequirements(s); err == nil {
			t.Fatal("malformed frozen profile accepted")
		}
	}
}
func TestReviewExecutionProfileLegacyGolden(t *testing.T) {
	// Exact historical canonical layout, including capitalized keys and field order.
	expected := `{"Risk":"routine","CriteriaDigest":"` + strings.Repeat("a", 64) + `","PolicyDigest":"` + strings.Repeat("b", 64) + `","RequiredReviewers":2,"MinProviderFamilies":2,"RoundLimit":2,"Members":[{"ID":"one","Role":"independent","Route":"codex/sol","ProviderFamily":"openai","Tier":"executor","Required":true},{"ID":"two","Role":"independent","Route":"other/model","ProviderFamily":"other","Tier":"executor","Required":true}]}`
	r, err := NewRequirements(authorityRequirementsSpec())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(r.Snapshot())
	if err != nil || string(raw) != expected {
		t.Fatalf("legacy bytes changed: %s %v", raw, err)
	}
	sum := sha256.Sum256([]byte(expected))
	if r.Digest() != hex.EncodeToString(sum[:]) {
		t.Fatal("legacy digest changed")
	}
}
