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

// A replace ref lets a repository substitute another commit's tree for HEAD's
// while HEAD still names the accepted commit. The executor controls the
// repository's refs, so the comparison must use HEAD's own tree.
func TestCaptureWorkspaceHeadIgnoresReplaceRefs(t *testing.T) {
	dir, head := workspaceHeadRepository(t)
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("unreviewed code\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, dir, "add", "main.go")
	tree := gitOutput(t, dir, "write-tree")
	substitute := gitOutput(t, dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit-tree", tree, "-m", "substitute")
	gitRun(t, dir, "replace", head, substitute)
	// The workspace's index now follows the substituted tree as well.
	gitRun(t, dir, "reset", "-q")
	captured := CaptureWorkspaceHead(context.Background(), "", dir, nil)
	gate := domain.EvaluateReviewCompletionGate(&domain.ReviewRoundHead{HeadCommit: head, Accepted: true, Verdict: "accept"}, &captured, nil)
	if captured.Head != head || !captured.Dirty || !slices.Contains(captured.DirtyPaths, "main.go") || gate.Passed {
		t.Fatalf("replace ref hid an edit: capture=%+v gate=%+v", captured, gate)
	}
}

// core.fileMode=false tells Git to ignore the executable bit, so a tracked
// file made executable after review would pass as clean.
func TestCaptureWorkspaceHeadComparesFileModes(t *testing.T) {
	dir, head := workspaceHeadRepository(t)
	gitRun(t, dir, "config", "core.fileMode", "false")
	if err := os.Chmod(filepath.Join(dir, "main.go"), 0o700); err != nil {
		t.Fatal(err)
	}
	captured := CaptureWorkspaceHead(context.Background(), "", dir, nil)
	if captured.Head != head || !captured.Dirty || !slices.Contains(captured.DirtyPaths, "main.go") {
		t.Fatalf("mode change hidden: %+v", captured)
	}
}

// A promotion that created the campaign ref but failed to write its record
// must be completed by the next accepted fetch; otherwise Resolve never finds
// the output and ReleaseRun, which finds published refs by their records,
// leaves the ref behind forever.
func TestPromotionThatFailedBeforeItsRecordIsCompletedByTheNextFetch(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	staged, err := refs.Stage(ctx, PublishCommitRequest{
		WorkflowRunID: "run-1", TaskID: "task-1", Name: "change", Repository: "repo",
		WorkspaceDir: repository, Base: base,
	}, "attempt-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	// A file where the published records belong makes the record write fail.
	blocker := filepath.Join(refs.Root, "provenance")
	if err := os.WriteFile(blocker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	first := t.TempDir()
	gitRun(t, first, "init", "-q")
	if err := refs.FetchAcceptedInto(ctx, first, staged, nil); err == nil {
		t.Fatal("promotion succeeded without its record")
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	second := t.TempDir()
	gitRun(t, second, "init", "-q")
	if err := refs.FetchAcceptedInto(ctx, second, staged, nil); err != nil {
		t.Fatal(err)
	}
	if provenance, err := refs.Resolve("run-1", "task-1", "change"); err != nil || provenance.Commit != base {
		t.Fatalf("published provenance = %+v %v, want %s", provenance, err, base)
	}
	if err := refs.ReleaseRun(ctx, "run-1", nil); err != nil {
		t.Fatal(err)
	}
	if out := gitOutput(t, filepath.Join(refs.Root, "campaigns.git"), "for-each-ref"); strings.TrimSpace(out) != "" {
		t.Fatalf("release left refs behind: %s", out)
	}
}

// A judge holding a rejected attempt's commit still receives it after a later
// attempt's commit became the campaign output, for example when its worker
// prepares the workspace again after a restart.
func TestInspectionOfARejectedCommitSurvivesALaterPublication(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	gitRun(t, repository, "commit", "--allow-empty", "-m", "rejected")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	request := PublishCommitRequest{WorkflowRunID: "run", TaskID: "producer", Name: "change",
		Repository: "repo", WorkspaceDir: repository, Base: base}
	rejected, err := refs.Stage(ctx, request, "attempt-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	gitRun(t, repository, "commit", "--allow-empty", "-m", "accepted")
	accepted, err := refs.Stage(ctx, request, "attempt-2", nil)
	if err != nil {
		t.Fatal(err)
	}
	consumer := t.TempDir()
	gitRun(t, consumer, "init", "-q")
	if err := refs.FetchAcceptedInto(ctx, consumer, accepted, nil); err != nil {
		t.Fatal(err)
	}
	judge := t.TempDir()
	gitRun(t, judge, "init", "-q")
	if err := refs.FetchInto(ctx, judge, rejected, nil); err != nil {
		t.Fatalf("judge cannot inspect the rejected commit once another is published: %v", err)
	}
	if got := gitOutput(t, judge, "rev-parse", rejected.Ref); got != rejected.Commit {
		t.Fatalf("judge resolved %s, want %s", got, rejected.Commit)
	}
	if provenance, err := refs.Resolve("run", "producer", "change"); err != nil || provenance.Commit != accepted.Commit {
		t.Fatalf("inspection changed the published output: %+v %v", provenance, err)
	}
}
