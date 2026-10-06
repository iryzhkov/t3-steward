package backlog

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func workspaceHeadRepository(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	gitRun(t, dir, "init", "-q")
	for _, name := range []string{"main.go", "answer.txt", ".t3/base-commit", ".t3-steward/task.env"} {
		path := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("one\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	gitRun(t, dir, "add", "-A", "-f")
	gitRun(t, dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "-m", "base")
	return dir, gitOutput(t, dir, "rev-parse", "HEAD")
}

func TestCaptureWorkspaceHeadReportsHeadAndTrackedChangesOnly(t *testing.T) {
	ctx := context.Background()
	outputs := []domain.ArtifactDeclaration{{Name: "answer.txt"}}
	dir, head := workspaceHeadRepository(t)
	clean := CaptureWorkspaceHead(ctx, "", dir, outputs)
	if clean.Schema != domain.WorkspaceHeadSchema || clean.Head != head || clean.Dirty || clean.Error != "" {
		t.Fatalf("clean capture = %+v", clean)
	}
	// Declared outputs, the task's .t3 inputs, the identity directory and
	// untracked files are not the reviewed work.
	for _, name := range []string{"answer.txt", ".t3/base-commit", ".t3-steward/task.env", "untracked.txt"} {
		if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(name)), []byte("two\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if ignored := CaptureWorkspaceHead(ctx, "", dir, outputs); ignored.Dirty || ignored.Head != head {
		t.Fatalf("excluded changes made the tree dirty: %+v", ignored)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, dir, "mv", "answer.txt", "moved.txt")
	dirty := CaptureWorkspaceHead(ctx, "", dir, outputs)
	if !dirty.Dirty || dirty.Head != head || !slices.Contains(dirty.DirtyPaths, "main.go") || !slices.Contains(dirty.DirtyPaths, "moved.txt") {
		t.Fatalf("tracked change not reported: %+v", dirty)
	}
}

func TestCaptureWorkspaceHeadReportsAnUnusableWorkspace(t *testing.T) {
	captured := CaptureWorkspaceHead(context.Background(), "", t.TempDir(), nil)
	if captured.Schema != domain.WorkspaceHeadSchema || captured.Head != "" || captured.Error == "" {
		t.Fatalf("capture outside a repository = %+v", captured)
	}
}

func TestWorkspaceHeadEvidenceRoundTrips(t *testing.T) {
	dir, head := workspaceHeadRepository(t)
	captured := CaptureWorkspaceHead(context.Background(), "", dir, nil)
	raw, err := MarshalWorkspaceHead(captured)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseWorkspaceHead(raw)
	if err != nil || parsed.Head != head {
		t.Fatalf("parsed = %+v err=%v", parsed, err)
	}
	if _, err := ParseWorkspaceHead([]byte(`{"schema":"workspace-head/v1","head":"x","extra":1}`)); err == nil {
		t.Fatal("unknown field accepted")
	}
}
