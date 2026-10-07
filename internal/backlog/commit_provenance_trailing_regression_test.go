package backlog

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestRecognizedCommitWithTrailingDataCannotBecomeOrdinaryDependency(t *testing.T) {
	dir := t.TempDir()
	raw := []byte(`{"version":"campaign-commit/v1","workflowRunId":"r","taskId":"t","name":"candidate","repository":"repo","base":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","ref":"refs/heads/spoof"} trailing`)
	if err := os.WriteFile(filepath.Join(dir, "candidate"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	err := (WorkspacePreparer{}).resolveDependencyCommits(context.Background(), dir, t.TempDir(), WorkspacePreparation{WorkflowRunID: "r"}, nil)
	if err == nil {
		t.Fatal("malformed recognized commit provenance silently became ordinary input")
	}
}

func TestOrdinaryDependencyWithTrailingDataStaysOrdinary(t *testing.T) {
	for _, raw := range []string{"plain text", `{"version":"other"} trailing`, `{"message":"campaign-commit/v1"} trailing`} {
		if LooksLikeCommitProvenance([]byte(raw)) {
			t.Fatalf("ordinary dependency recognized as commit: %q", raw)
		}
	}
}
