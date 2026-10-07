package backlog

import (
	"context"
	"path/filepath"
	"testing"
)

// rc.117 combination of M16-0's commit bundles and M16-3's staged commits: a
// coordinator that accepts bundles offers the capability to every producer
// of a declared commit, including a review-gated one. Its commit is staged,
// not published, so there is no campaign ref to bundle; the producer must
// still succeed, and must say why no bundle travels with the commit.
func TestReviewGatedProducerOfferedBundlesStagesWithoutABundle(t *testing.T) {
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	writeGitFile(t, repository, "version.txt", "second\n")
	gitRun(t, repository, "commit", "-am", "second")
	head := gitOutput(t, repository, "rev-parse", "HEAD")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	f := newReviewGateFixture(t, true, true)
	finalized, err := (AttemptFinalizer{StorageRoot: t.TempDir(), CampaignRefs: refs, Processes: testProcessRunner{}}).Finalize(context.Background(), AttemptFinalization{
		Task: f.task, Attempt: f.attempt, WorkspaceDir: repository, ExplicitSuccess: true,
		Repository: repository, BaseCommit: base, ReviewGated: true, CommitBundles: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, finalized.StorageDir)
	if !finalized.Completion.VerificationPassed {
		t.Fatalf("review-gated producer offered bundles failed: %+v", finalized.Completion)
	}
	provenance := finalizedCommitProvenance(t, finalized, "change")
	if provenance.Commit != head || provenance.Bundle != nil || provenance.BundleOmitted != BundleOmittedStaged {
		t.Fatalf("provenance = %+v, want commit %s staged without a bundle", provenance, head)
	}
	if _, err := refs.Resolve(f.attempt.WorkflowRunID, f.task.ID, "change"); err == nil {
		t.Fatal("the staged commit was published")
	}
}
