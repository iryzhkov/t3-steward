package backlog

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A campaign ref can be published once and never redefined, so publishing one
// for an attempt that has already failed poisons every retry: the retry builds
// a different commit, the ref refuses to move, and the refusal about the ref
// replaces the failure that actually happened. The commit therefore waits for
// an attempt that is otherwise a success.
func TestNoCommitIsPublishedForAnAttemptThatAlreadyFailed(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	finalizer := AttemptFinalizer{
		StorageRoot: t.TempDir(), CampaignRefs: refs, Processes: testProcessRunner{},
	}
	task := domain.Task{
		ID: "task-implement", WorkflowID: "workflow-1", Name: "implement",
		Outputs:      []domain.ArtifactDeclaration{{Name: "handoff", Commit: &domain.CommitOutput{}}},
		Verification: []string{"exit 3"},
	}
	attempt := domain.Attempt{
		ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1,
	}

	failed, err := finalizer.Finalize(ctx, AttemptFinalization{
		Task: task, Attempt: attempt, WorkspaceDir: repository,
		ExplicitSuccess: true, Repository: repository, BaseCommit: base,
	})
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if failed.StorageDir != "" {
		cleanupImmutable(t, failed.StorageDir)
	}
	if failed.Completion.VerificationPassed {
		t.Fatal("a failed verification passed")
	}
	// The cause survives, and it comes first: the note about the commit is an
	// addition to the reason, never a replacement for it.
	if !strings.HasPrefix(failed.Completion.Failure, "verification command failed (3): exit 3") {
		t.Fatalf("failure = %q", failed.Completion.Failure)
	}
	if !strings.Contains(failed.Completion.Failure, `declared commit "handoff" not published`) {
		t.Fatalf("failure does not say the commit waited: %q", failed.Completion.Failure)
	}
	if runs, err := refs.Runs(); err != nil || len(runs) != 0 {
		t.Fatalf("a failed attempt published %v, %v", runs, err)
	}

	// The retry commits something different, as a retry does, and is accepted:
	// the first attempt left no ref to collide with.
	writeGitFile(t, repository, "implementation.txt", "second try\n")
	gitRun(t, repository, "add", "implementation.txt")
	gitRun(t, repository, "commit", "-m", "retry")
	retryTask := task
	retryTask.Verification = []string{"true"}
	retry := attempt
	retry.ID = "attempt-2"
	finalized, err := finalizer.Finalize(ctx, AttemptFinalization{
		Task: retryTask, Attempt: retry, WorkspaceDir: repository,
		ExplicitSuccess: true, Repository: repository, BaseCommit: base,
	})
	if err != nil {
		t.Fatalf("finalize retry: %v", err)
	}
	cleanupImmutable(t, finalized.StorageDir)
	if !finalized.Completion.VerificationPassed || finalized.Completion.Failure != "" {
		t.Fatalf("the retry inherited the first attempt's ref: %+v", finalized.Completion)
	}
	provenance, err := refs.Resolve("run-1", "task-implement", "handoff")
	if err != nil {
		t.Fatalf("resolve the retry's commit: %v", err)
	}
	if provenance.Commit != gitOutput(t, repository, "rev-parse", "HEAD") {
		t.Fatalf("the published commit is not the retry's: %+v", provenance)
	}
}

// A missing declared output is the same shape: the attempt has failed, so the
// commit is not published and the retry is free to publish its own.
func TestNoCommitIsPublishedWhenADeclaredOutputIsMissing(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	finalizer := AttemptFinalizer{
		StorageRoot: t.TempDir(), CampaignRefs: refs, Processes: testProcessRunner{},
	}
	task := domain.Task{
		ID: "task-implement", WorkflowID: "workflow-1", Name: "implement",
		Outputs: []domain.ArtifactDeclaration{
			{Name: "report.md"},
			{Name: "handoff", Commit: &domain.CommitOutput{}},
		},
	}
	result, err := finalizer.Finalize(ctx, AttemptFinalization{
		Task: task,
		Attempt: domain.Attempt{
			ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1,
		},
		WorkspaceDir: repository, ExplicitSuccess: true,
		Repository: repository, BaseCommit: base,
	})
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if result.StorageDir != "" {
		cleanupImmutable(t, result.StorageDir)
	}
	if !strings.HasPrefix(result.Completion.Failure, "missing declared output: report.md") {
		t.Fatalf("failure = %q", result.Completion.Failure)
	}
	if runs, err := refs.Runs(); err != nil || len(runs) != 0 {
		t.Fatalf("an attempt with a missing output published %v, %v", runs, err)
	}
}
