package backlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestCapturePreservedResultDigestsDeclaredWork(t *testing.T) {
	t.Parallel()
	workspace := t.TempDir()
	gitRun(t, workspace, "init", "-q")
	if err := os.WriteFile(filepath.Join(workspace, "fix.go"), []byte("package fix\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, workspace, "add", "fix.go")
	gitRun(t, workspace, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "-m", "fix")
	if err := os.WriteFile(filepath.Join(workspace, "handoff.md"), []byte("done\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(workspace, "reports"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "reports", "a.txt"), []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	outputs := []domain.ArtifactDeclaration{
		{Name: "handoff.md"}, {Name: "reports/a.txt"}, {Name: "never-written.md"},
		{Name: "implementation", Commit: &domain.CommitOutput{}},
		{Name: "missing-branch", Commit: &domain.CommitOutput{Revision: "refs/heads/nope"}},
	}
	ctx := context.Background()
	first, err := CapturePreservedResult(ctx, "", workspace, outputs)
	if err != nil {
		t.Fatal(err)
	}
	if first.Schema != PreservedResultSchema || first.Digest == "" || len(first.Outputs) != 3 || len(first.Commits) != 2 {
		t.Fatalf("captured = %+v", first)
	}
	head := gitOutput(t, workspace, "rev-parse", "HEAD")
	if first.Commits[0] != (PreservedEntry{Name: "implementation", Value: head}) ||
		first.Commits[1] != (PreservedEntry{Name: "missing-branch", Value: preservedUnresolved}) ||
		first.Outputs[2] != (PreservedEntry{Name: "never-written.md", Value: preservedAbsent}) {
		t.Fatalf("entries = %+v", first)
	}
	again, err := CapturePreservedResult(ctx, "", workspace, outputs)
	if err != nil || first.Difference(again) != "" {
		t.Fatalf("an unchanged workspace differs: %q, %v", first.Difference(again), err)
	}
	// Untracked noise that is not declared, such as a verification log,
	// does not change the digest.
	if err := os.WriteFile(filepath.Join(workspace, "test.log"), []byte("ok"), 0o600); err != nil {
		t.Fatal(err)
	}
	if noise, _ := CapturePreservedResult(ctx, "", workspace, outputs); first.Difference(noise) != "" {
		t.Fatalf("undeclared file changed the digest: %s", first.Difference(noise))
	}

	// A changed declared output changes the digest.
	if err := os.WriteFile(filepath.Join(workspace, "reports", "a.txt"), []byte("b"), 0o600); err != nil {
		t.Fatal(err)
	}
	changed, _ := CapturePreservedResult(ctx, "", workspace, outputs)
	if difference := first.Difference(changed); difference == "" {
		t.Fatal("a changed output was not noticed")
	}
	if err := os.WriteFile(filepath.Join(workspace, "reports", "a.txt"), []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A new commit moves the declared commit.
	gitRun(t, workspace, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "--allow-empty", "-m", "later")
	moved, _ := CapturePreservedResult(ctx, "", workspace, outputs)
	if difference := first.Difference(moved); difference == "" {
		t.Fatal("a moved commit was not noticed")
	}
	// A record that was edited no longer matches its own digest.
	tampered := first
	tampered.Outputs = append([]PreservedEntry(nil), first.Outputs...)
	tampered.Outputs[0].Value = "0000"
	if tampered.Difference(first) == "" {
		t.Fatal("a tampered record was trusted")
	}

	if err := os.RemoveAll(workspace); err != nil {
		t.Fatal(err)
	}
	if _, err := CapturePreservedResult(ctx, "", workspace, outputs); !errors.Is(err, ErrPreservedWorkspaceMissing) {
		t.Fatalf("missing workspace = %v", err)
	}
}
