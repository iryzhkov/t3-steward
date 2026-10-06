package backlog

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestExportPublishedBundleReadOnly(t *testing.T) {
	ctx := context.Background()
	repo := newGitFixture(t)
	base := gitOutput(t, repo, "rev-parse", "HEAD")
	writeGitFile(t, repo, "export.txt", "export")
	gitRun(t, repo, "add", "export.txt")
	gitRun(t, repo, "commit", "-m", "export")
	commit := gitOutput(t, repo, "rev-parse", "HEAD")
	store := CampaignRefStore{Root: filepath.Join(t.TempDir(), "refs")}
	provenance, err := store.Publish(ctx, PublishCommitRequest{WorkflowRunID: "run-1", TaskID: "task-1", Name: "implementation", Repository: repo, Base: base, Revision: commit, WorkspaceDir: repo}, nil)
	if err != nil {
		t.Fatal(err)
	}
	path, err := store.ExportPublishedBundle(ctx, provenance, DefaultCommitBundleMaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(filepath.Dir(path))
	gitRun(t, repo, "bundle", "verify", path)
	gitRun(t, repo, "bundle", "list-heads", path)
	equal, err := store.Publish(ctx, PublishCommitRequest{WorkflowRunID: "run-1", TaskID: "task-1", Name: "equal", Repository: repo, Base: commit, Revision: commit, WorkspaceDir: repo}, nil)
	if err != nil {
		t.Fatal(err)
	}
	equalPath, err := store.ExportPublishedBundle(ctx, equal, DefaultCommitBundleMaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(filepath.Dir(equalPath))
	gitRun(t, repo, "bundle", "verify", equalPath)
	bad := provenance
	bad.Commit = base
	if _, err := store.ExportPublishedBundle(ctx, bad, DefaultCommitBundleMaxBytes); err == nil {
		t.Fatal("accepted changed provenance")
	}
	missing := CampaignRefStore{Root: filepath.Join(t.TempDir(), "absent")}
	if _, err := missing.ExportPublishedBundle(ctx, provenance, DefaultCommitBundleMaxBytes); err == nil {
		t.Fatal("accepted missing store")
	}
	if _, err := os.Stat(missing.Root); !os.IsNotExist(err) {
		t.Fatalf("created missing store: %v", err)
	}
	if _, err := store.ExportPublishedBundle(ctx, provenance, 1); err == nil {
		t.Fatal("accepted oversized bundle")
	}
}
