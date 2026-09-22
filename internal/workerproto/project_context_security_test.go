package workerproto

import (
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestExecutionReferenceRequiresDependencyProvenance(t *testing.T) {
	pkg := validPackage()
	input := pkg.StaticInputs[0]
	ref := domain.ProjectContextReference{
		ID: "execution-ref", Kind: domain.ContextReferenceExecution,
		URI:      "execution:source-run/source-task/source-attempt/source-artifact",
		Revision: input.SHA256, Status: domain.ProjectContextAccepted, Authority: "gate-decision:accepted",
		Acceptance: &domain.ProjectContextAcceptance{GateID: "gate", DecisionID: "accepted", EvidenceSnapshotID: "evidence"},
		Binding:    &domain.ProjectContextArtifactBinding{ArtifactID: input.ID, Path: input.Path, SHA256: input.SHA256},
	}
	pkg.Context = packageContextFor(pkg, ref)
	pkg.RequiredCapabilities = []string{PackageCapabilityProjectContext}
	err := ValidateExecutionPackage(pkg)
	if err == nil || !strings.Contains(err.Error(), "dependency provenance") {
		t.Fatalf("execution reference without full dependency provenance accepted: %v", err)
	}
}

func executionContextPackage() ExecutionPackage {
	pkg := validPackage()
	object := pkg.Dependencies[0].Artifacts[0]
	pkg.Dependencies[0].Provenance = &DependencyProvenance{
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
			SourceRunID: "source-run", SourceTaskID: "source-task",
			SourceAttemptID: "source-attempt", SourceArtifactID: "source-artifact",
		},
	}
	pkg.Context = packageContextFor(pkg, ref)
	pkg.RequiredCapabilities = []string{PackageCapabilityProjectContext}
	return pkg
}

func cloneExecutionContextPackage(pkg ExecutionPackage) ExecutionPackage {
	candidate := pkg
	candidate.Dependencies = append([]DependencyInput(nil), pkg.Dependencies...)
	candidate.Dependencies[0].Artifacts = append([]ArtifactObject(nil), pkg.Dependencies[0].Artifacts...)
	provenance := *pkg.Dependencies[0].Provenance
	provenance.SourceArtifacts = map[string]string{}
	for id, source := range pkg.Dependencies[0].Provenance.SourceArtifacts {
		provenance.SourceArtifacts[id] = source
	}
	candidate.Dependencies[0].Provenance = &provenance
	index := *pkg.Context
	index.References = append([]domain.ProjectContextReference(nil), pkg.Context.References...)
	binding := *pkg.Context.References[0].Binding
	index.References[0].Binding = &binding
	receipt := *pkg.Context.References[0].Acceptance
	index.References[0].Acceptance = &receipt
	candidate.Context = &index
	return candidate
}

func TestExecutionReferenceRequiresExactRetainedSourceIdentity(t *testing.T) {
	base := executionContextPackage()
	if err := ValidateExecutionPackage(base); err != nil {
		t.Fatalf("valid execution provenance: %v", err)
	}
	objectID := base.Dependencies[0].Artifacts[0].ID
	tests := map[string]func(*ExecutionPackage){
		"wrong-run":      func(p *ExecutionPackage) { p.Dependencies[0].Provenance.RunID = "wrong-run" },
		"wrong-task":     func(p *ExecutionPackage) { p.Dependencies[0].Provenance.TaskID = "wrong-task" },
		"wrong-attempt":  func(p *ExecutionPackage) { p.Dependencies[0].Provenance.AttemptID = "wrong-attempt" },
		"wrong-artifact": func(p *ExecutionPackage) { p.Dependencies[0].Provenance.SourceArtifacts[objectID] = "wrong-artifact" },
		"wrong-digest":   func(p *ExecutionPackage) { p.Context.References[0].Binding.SHA256 = strings.Repeat("0", 64) },
		"wrong-uri": func(p *ExecutionPackage) {
			p.Context.References[0].URI = "execution:source-run/source-task/source-attempt/wrong-artifact"
		},
		"partial-source": func(p *ExecutionPackage) { p.Context.References[0].Binding.SourceAttemptID = "" },
		"missing-retained": func(p *ExecutionPackage) {
			p.Dependencies[0].Artifacts = nil
			p.Dependencies[0].Provenance.SourceArtifacts = nil
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := cloneExecutionContextPackage(base)
			mutate(&candidate)
			if err := ValidateExecutionPackage(candidate); err == nil {
				t.Fatal("mismatched execution provenance was accepted")
			}
		})
	}
}
