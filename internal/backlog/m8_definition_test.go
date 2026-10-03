package backlog

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
)

func TestM8UnboundDefinitionArtifactOwnership(t *testing.T) {
	for _, tc := range []struct {
		name, storage string
		kind          domain.ArtifactKind
		accepted      bool
	}{
		{"registered input", "workflows/workflow-1/files/prompt.md", domain.ArtifactInput, true},
		{"foreign definition", "workflows/foreign/files/prompt.md", domain.ArtifactInput, false},
		{"traversal to foreign", "workflows/workflow-1/../foreign/prompt.md", domain.ArtifactInput, false},
		{"unbound output", "workflows/workflow-1/files/output.md", domain.ArtifactOutput, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := executionPackageState{workflow: domain.Workflow{ID: "workflow-1"}, artifacts: map[string]domain.Artifact{
				"artifact-1": {ID: "artifact-1", Kind: tc.kind, StoragePath: tc.storage},
			}}
			_, err := requiredDefinitionArtifact(state, "artifact-1")
			if (err == nil) != tc.accepted {
				t.Fatalf("accepted=%t, error=%v", tc.accepted, err)
			}
		})
	}
}
