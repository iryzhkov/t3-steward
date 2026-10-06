package backlog

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The index can promise Git that a tracked file has not changed. Those
// promises are the executor's, so an edit behind either flag is still a
// tracked change the review never saw.
func TestCaptureWorkspaceHeadSeesThroughIndexFlags(t *testing.T) {
	for _, flag := range []string{"--assume-unchanged", "--skip-worktree"} {
		t.Run(flag, func(t *testing.T) {
			dir, head := workspaceHeadRepository(t)
			gitRun(t, dir, "update-index", flag, "main.go")
			if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("unreviewed code\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			captured := CaptureWorkspaceHead(context.Background(), "", dir, nil)
			gate := domain.EvaluateReviewCompletionGate(&domain.ReviewRoundHead{HeadCommit: head, Accepted: true, Verdict: "accept"}, &captured, nil)
			if !captured.Dirty || !slices.Contains(captured.DirtyPaths, "main.go") || gate.Passed {
				t.Fatalf("unreviewed tracked edit completed: capture=%+v gate=%+v", captured, gate)
			}
		})
	}
}

// A flagged file whose bytes still match HEAD is not a change, and a deleted
// flagged file is.
func TestCaptureWorkspaceHeadComparesFlaggedFilesByContent(t *testing.T) {
	dir, head := workspaceHeadRepository(t)
	gitRun(t, dir, "update-index", "--skip-worktree", "main.go")
	if captured := CaptureWorkspaceHead(context.Background(), "", dir, nil); captured.Head != head || captured.Dirty {
		t.Fatalf("unchanged flagged file reported dirty: %+v", captured)
	}
	if err := os.Remove(filepath.Join(dir, "main.go")); err != nil {
		t.Fatal(err)
	}
	if captured := CaptureWorkspaceHead(context.Background(), "", dir, nil); !captured.Dirty || !slices.Contains(captured.DirtyPaths, "main.go") {
		t.Fatalf("deleted flagged file not reported: %+v", captured)
	}
}

// A file system monitor configured in the workspace answers Git's question
// about which files changed. An executor that configures one that always
// answers "nothing" must not hide an edit.
func TestCaptureWorkspaceHeadIgnoresAWorkspaceFileSystemMonitor(t *testing.T) {
	dir, head := workspaceHeadRepository(t)
	hook := filepath.Join(t.TempDir(), "fsmonitor")
	if err := os.WriteFile(hook, []byte("#!/bin/sh\nprintf 'token\\0'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	gitRun(t, dir, "config", "core.fsmonitor", hook)
	gitRun(t, dir, "status")
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("unreviewed code\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	captured := CaptureWorkspaceHead(context.Background(), "", dir, nil)
	if captured.Head != head || !captured.Dirty || !slices.Contains(captured.DirtyPaths, "main.go") {
		t.Fatalf("file system monitor hid an edit: %+v", captured)
	}
}

// reviewGatedFinalize finalizes a review-declared attempt whose workspace HEAD
// is the declared commit.
func reviewGatedFinalize(t *testing.T, f *reviewGateFixture, refs CampaignRefStore, repository, base string) FinalizedAttempt {
	t.Helper()
	finalized, err := (AttemptFinalizer{StorageRoot: t.TempDir(), CampaignRefs: refs, Processes: testProcessRunner{}}).Finalize(context.Background(), AttemptFinalization{
		Task: f.task, Attempt: f.attempt, WorkspaceDir: repository, ExplicitSuccess: true,
		Repository: repository, BaseCommit: base, ReviewGated: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, finalized.StorageDir)
	if !finalized.Completion.VerificationPassed {
		t.Fatalf("fixture finalization failed: %+v", finalized.Completion)
	}
	return finalized
}

func finalizedCommitProvenance(t *testing.T, finalized FinalizedAttempt, name string) CommitProvenance {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(finalized.StorageDir, "artifacts", "outputs", name))
	if err != nil {
		t.Fatal(err)
	}
	provenance, err := ParseCommitProvenance(raw)
	if err != nil {
		t.Fatal(err)
	}
	return provenance
}

// The worker cannot know whether the coordinator will accept a
// review-declared result, so its declared commit is only staged. A commit the
// gate rejects never becomes the task's campaign output.
func TestReviewGateRejectedCommitIsNotPublished(t *testing.T) {
	repository := newGitFixture(t)
	accepted := gitOutput(t, repository, "rev-parse", "HEAD")
	gitRun(t, repository, "commit", "--allow-empty", "-m", "unreviewed later head")
	later := gitOutput(t, repository, "rev-parse", "HEAD")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	f := newReviewGateFixture(t, true, true)
	f.openRound(t, "cp-1", accepted, "accept")
	f.finishTurn(t)
	finalized := reviewGatedFinalize(t, f, refs, repository, accepted)
	if staged := finalizedCommitProvenance(t, finalized, "change"); staged.Commit != later || staged.Ref != CampaignRef(f.attempt.WorkflowRunID, f.task.ID, "change") {
		t.Fatalf("staged provenance = %+v", staged)
	}
	attempt := f.collect(t, f.store, cleanWorkspaceHead(later), later)
	if attempt.Progress != domain.ProgressFailed || attempt.ReviewGate == nil || attempt.ReviewGate.Code != domain.ReviewGateHeadChanged {
		t.Fatalf("gate did not reject: %+v", attempt)
	}
	if provenance, err := refs.Resolve(f.attempt.WorkflowRunID, f.task.ID, "change"); err == nil {
		t.Fatalf("rejected unreviewed head is published: %+v", provenance)
	}
	gitDir := filepath.Join(refs.Root, "campaigns.git")
	if out := gitOutput(t, gitDir, "for-each-ref", "refs/campaigns/"); out != "" {
		t.Fatalf("campaign ref published before acceptance: %s", out)
	}
}

// The accepted result's commit becomes the campaign output when a dependent
// task consumes it, and a retry that stages a different commit is not refused
// by an earlier attempt's staging.
func TestReviewGatedCommitIsPublishedWhenTheAcceptedResultIsConsumed(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	gitRun(t, repository, "commit", "--allow-empty", "-m", "rejected head")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	f := newReviewGateFixture(t, true, true)
	rejected := finalizedCommitProvenance(t, reviewGatedFinalize(t, f, refs, repository, base), "change")

	gitRun(t, repository, "commit", "--allow-empty", "-m", "accepted head")
	accepted := gitOutput(t, repository, "rev-parse", "HEAD")
	retry := *f
	retry.attempt.ID, retry.attempt.Number = "attempt-2", 2
	staged := finalizedCommitProvenance(t, reviewGatedFinalize(t, &retry, refs, repository, base), "change")
	if staged.Commit != accepted || rejected.Commit == accepted {
		t.Fatalf("staged %s after %s, want %s", staged.Commit, rejected.Commit, accepted)
	}
	if runs, err := refs.Runs(); err != nil || !slices.Equal(runs, []string{f.attempt.WorkflowRunID}) {
		t.Fatalf("runs holding staged commits = %v %v", runs, err)
	}

	consumer := t.TempDir()
	gitRun(t, consumer, "init", "-q")
	if err := refs.FetchInto(ctx, consumer, staged, nil); err != nil {
		t.Fatal(err)
	}
	if got := gitOutput(t, consumer, "rev-parse", staged.Ref); got != accepted {
		t.Fatalf("consumer resolved %s, want %s", got, accepted)
	}
	if provenance, err := refs.Resolve(f.attempt.WorkflowRunID, f.task.ID, "change"); err != nil || provenance.Commit != accepted {
		t.Fatalf("published provenance = %+v %v, want %s", provenance, err, accepted)
	}
	// The output is now fixed: the rejected staging cannot redefine it.
	if err := refs.FetchInto(ctx, t.TempDir(), rejected, nil); err == nil {
		t.Fatal("a rejected staging redefined the published output")
	}

	if err := refs.ReleaseRun(ctx, f.attempt.WorkflowRunID, nil); err != nil {
		t.Fatal(err)
	}
	if out := gitOutput(t, filepath.Join(refs.Root, "campaigns.git"), "for-each-ref"); strings.TrimSpace(out) != "" {
		t.Fatalf("release left refs behind: %s", out)
	}
	if runs, err := refs.Runs(); err != nil || len(runs) != 0 {
		t.Fatalf("runs after release = %v %v", runs, err)
	}
}

// A run that only staged commits, because every result was rejected, is still
// held and still released.
func TestReleaseRunDropsCommitsThatWereOnlyStaged(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	f := newReviewGateFixture(t, true, true)
	reviewGatedFinalize(t, f, refs, repository, base)
	if runs, err := refs.Runs(); err != nil || !slices.Equal(runs, []string{f.attempt.WorkflowRunID}) {
		t.Fatalf("runs holding staged commits = %v %v", runs, err)
	}
	if err := refs.ReleaseRun(ctx, f.attempt.WorkflowRunID, nil); err != nil {
		t.Fatal(err)
	}
	if out := gitOutput(t, filepath.Join(refs.Root, "campaigns.git"), "for-each-ref"); strings.TrimSpace(out) != "" {
		t.Fatalf("release left staged refs behind: %s", out)
	}
	if runs, err := refs.Runs(); err != nil || len(runs) != 0 {
		t.Fatalf("runs after release = %v %v", runs, err)
	}
}
