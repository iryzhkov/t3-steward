package workerproto

import (
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func packageContextFor(pkg ExecutionPackage, ref domain.ProjectContextReference) *domain.ProjectContext {
	fresh := pkg.CreatedAt.Add(4 * time.Hour)
	return &domain.ProjectContext{
		Version: domain.ProjectContextVersion, Revision: "context-1", Status: domain.ProjectContextAccepted,
		Objective: "complete assigned fixture", Authority: []string{"coordinator:run-1"},
		Budget: "one attempt", Outputs: []string{"findings.md"},
		RequiredReferences: []string{ref.ID}, References: []domain.ProjectContextReference{ref},
		Decisions: []domain.ProjectContextDecision{{
			ID: "accepted-branch", Status: "accepted", Summary: "reuse accepted producer",
			Authority: "review-gate",
		}},
		CheckpointDelta: []string{"producer accepted"}, CodeLocations: []domain.ProjectContextLocation{{
			Path: "internal/workerproto/package.go", Revision: strings.Repeat("a", 40),
		}},
		InputLocations: []domain.ProjectContextLocation{{
			Path: ref.Binding.Path, Revision: ref.Binding.SHA256,
		}},
		Setup: []string{"go version"}, Checks: []string{"go test ./..."},
		CapabilityRefs: []string{ref.ID},
		Freshness:      domain.ProjectContextFreshness{ObservedAt: pkg.CreatedAt, FreshThrough: &fresh},
	}
}

func TestProjectContextPackageProtocolCompatibilityAndExactAuthority(t *testing.T) {
	legacy := validPackage()
	if err := ValidateExecutionPackage(legacy); err != nil {
		t.Fatalf("legacy package compatibility: %v", err)
	}
	pkg := validPackage()
	input := pkg.StaticInputs[0]
	ref := domain.ProjectContextReference{
		ID: "input-ref", Kind: domain.ContextReferenceGit, URI: "https://example.invalid/repo",
		Revision: strings.Repeat("a", 40), Status: domain.ProjectContextPinned, Authority: "coordinator",
		Binding: &domain.ProjectContextArtifactBinding{ArtifactID: input.ID, Path: input.Path, SHA256: input.SHA256},
	}
	pkg.Context = packageContextFor(pkg, ref)
	pkg.RequiredCapabilities = []string{PackageCapabilityProjectContext}
	if _, err := BuildExecutionPackageManifest(pkg); err != nil {
		t.Fatalf("context package: %v", err)
	}

	for name, tt := range map[string]struct {
		mutate func(*ExecutionPackage)
		want   string
	}{
		"missing-capability": {func(p *ExecutionPackage) { p.RequiredCapabilities = nil }, "project context capability"},
		"missing-artifact":   {func(p *ExecutionPackage) { p.Context.References[0].Binding.ArtifactID = "missing" }, "0 package matches"},
		"wrong-digest":       {func(p *ExecutionPackage) { p.Context.References[0].Binding.SHA256 = strings.Repeat("0", 64) }, "path or digest"},
		"stale": {func(p *ExecutionPackage) {
			stale := p.CreatedAt.Add(time.Minute)
			p.Context.Freshness.FreshThrough = &stale
		}, "stale"},
		"self-approval": {func(p *ExecutionPackage) { p.Context.Decisions[0].Authority = "" }, "accepted authoritative"},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := pkg
			copyContext := *pkg.Context
			copyContext.References = append([]domain.ProjectContextReference(nil), pkg.Context.References...)
			binding := *copyContext.References[0].Binding
			copyContext.References[0].Binding = &binding
			copyContext.Decisions = append([]domain.ProjectContextDecision(nil), pkg.Context.Decisions...)
			candidate.Context = &copyContext
			tt.mutate(&candidate)
			err := ValidateExecutionPackage(candidate)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestProjectContextRejectsWrongCrossRunProducerIdentity(t *testing.T) {
	pkg := validPackage()
	dependency := &pkg.Dependencies[0]
	object := dependency.Artifacts[0]
	dependency.Provenance = &DependencyProvenance{
		RunID: "source-run", TaskID: "source-task", AttemptID: "source-attempt",
		SourceArtifacts: map[string]string{object.ID: "source-artifact"},
	}
	ref := domain.ProjectContextReference{
		ID: "execution-ref", Kind: domain.ContextReferenceExecution,
		URI:      "execution:source-run/source-task/source-attempt/source-artifact",
		Revision: object.SHA256, Status: domain.ProjectContextAccepted, Authority: "gate-decision:accepted",
		Acceptance: &domain.ProjectContextAcceptance{GateID: "gate", DecisionID: "accepted", EvidenceSnapshotID: "evidence"},
		Binding: &domain.ProjectContextArtifactBinding{
			ArtifactID: object.ID, Path: object.Path, SHA256: object.SHA256,
			SourceRunID: "source-run", SourceTaskID: "source-task", SourceAttemptID: "source-attempt", SourceArtifactID: "source-artifact",
		},
	}
	pkg.Context = packageContextFor(pkg, ref)
	pkg.RequiredCapabilities = []string{PackageCapabilityProjectContext}
	if err := ValidateExecutionPackage(pkg); err != nil {
		t.Fatal(err)
	}
	pkg.Dependencies[0].Provenance.RunID = "parallel-run"
	if err := ValidateExecutionPackage(pkg); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("wrong producer identity accepted: %v", err)
	}
}
