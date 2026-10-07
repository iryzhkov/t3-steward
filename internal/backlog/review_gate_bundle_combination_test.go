package backlog

import (
	"context"
	"path/filepath"
	"testing"
)

// rc.117 combination of M16-0's commit bundles and M16-3's staged commits: a
// consumer on the worker that staged a review-gated commit finds it in its
// own store. Obtain must not ask for a bundle, which the producer never
// retains for a staged commit, and must not publish the staging itself; the
// accepted fetch does that.
func TestObtainFindsACommitStagedOnThisWorker(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	commit := gitOutput(t, repository, "rev-parse", "HEAD")
	writeGitFile(t, repository, "version.txt", "second\n")
	gitRun(t, repository, "commit", "-am", "second")
	changed := gitOutput(t, repository, "rev-parse", "HEAD")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	staged, err := refs.Stage(ctx, PublishCommitRequest{
		WorkflowRunID: "run-1", TaskID: "producer", Name: "change", Repository: "repo",
		WorkspaceDir: repository, Base: commit,
	}, "attempt-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if staged.Commit != changed {
		t.Fatalf("staged %s, want %s", staged.Commit, changed)
	}
	consumer := newGitFixture(t)
	if err := refs.Obtain(ctx, consumer, staged, nil, nil); err != nil {
		t.Fatalf("obtain a commit staged on this worker: %v", err)
	}
	if provenance, err := refs.Resolve("run-1", "producer", "change"); err == nil {
		t.Fatalf("obtain published a staged commit: %+v", provenance)
	}
	// A commit neither published nor staged here still needs its bundle.
	other := staged
	other.Name = "other"
	other.Ref = CampaignRef("run-1", "producer", "other")
	if err := refs.Obtain(ctx, consumer, other, nil, nil); err == nil {
		t.Fatal("obtain accepted a commit this worker holds in no form")
	}
}
