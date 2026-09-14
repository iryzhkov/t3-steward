package backlog

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// ReleaseRun deletes the whole provenance directory of a run, so the list it
// acts on has to be the one the lock protects. Listing before the lock let a
// publication land in between: its record was destroyed with the directory and
// its ref, never listed, was left behind forever.
func TestReleaseRunActsOnTheListItReadsUnderTheLock(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	first, err := refs.Publish(ctx, PublishCommitRequest{
		WorkflowRunID: "run-1", TaskID: "task-1", Name: "first",
		Repository: repository, WorkspaceDir: repository, Base: base,
	}, nil)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Hold the lock a publication would hold, so the release blocks where a
	// concurrent publication would have it block.
	lock, err := acquireFileLock(ctx, refs.Root, "campaign-refs")
	if err != nil {
		t.Fatal(err)
	}
	released := make(chan error, 1)
	go func() { released <- refs.ReleaseRun(ctx, "run-1", nil) }()
	// The release must be waiting for the lock, not deciding without it. This
	// both asserts the lock discipline and puts the goroutine past its unlocked
	// look, so what follows is the interleaving the defect needed.
	select {
	case err := <-released:
		t.Fatalf("release finished while a publication held the lock: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	// What the blocked publication commits while the release waits: a second
	// commit, its ref and its record. The release has already looked once and
	// must not act on what it saw then.
	writeGitFile(t, repository, "version.txt", "second\n")
	gitRun(t, repository, "add", "version.txt")
	gitRun(t, repository, "commit", "-m", "second")
	secondCommit := gitOutput(t, repository, "rev-parse", "HEAD")
	secondRef := CampaignRef("run-1", "task-1", "second")
	gitRun(t, repository, "push", "--", filepath.Join(refs.Root, "campaigns.git"), secondCommit+":"+secondRef)
	if err := refs.writeProvenance(CommitProvenance{
		Version: CampaignCommitRecordVersion, WorkflowRunID: "run-1", TaskID: "task-1",
		Name: "second", Repository: repository, Base: first.Commit, Commit: secondCommit,
		Ref: secondRef, CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-released; err != nil {
		t.Fatalf("release run: %v", err)
	}

	if count := campaignRefCount(t, refs); count != 0 {
		t.Fatalf("%d ref(s) survived the release; a publication that landed while it waited was orphaned", count)
	}
	if runs, err := refs.Runs(); err != nil || len(runs) != 0 {
		t.Fatalf("runs after release = %v, %v", runs, err)
	}
	if _, err := os.Stat(filepath.Join(refs.Root, "provenance", "run-1")); !os.IsNotExist(err) {
		t.Fatalf("provenance directory survived: %v", err)
	}
}

// A run the coordinator has no record of is in neither half of the lifetime,
// so walking the records would pin its refs forever. The coordinator converges
// on what its store holds, which is the rule its workers already follow.
func TestCoordinatorReleasesRefsOfARunItHasForgotten(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	for _, runID := range []string{"run-forgotten", "run-live"} {
		if _, err := refs.Publish(ctx, PublishCommitRequest{
			WorkflowRunID: runID, TaskID: "task-1", Name: "handoff",
			Repository: repository, WorkspaceDir: repository, Base: base,
		}, nil); err != nil {
			t.Fatalf("publish %s: %v", runID, err)
		}
	}
	// The coordinator knows about run-live only: run-forgotten's records are
	// gone, pruned or lost with a restore.
	live, liveTask, liveArtifact := campaignRunRecords("run-live", "task-1", "handoff")
	records := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{live},
		Tasks:        []domain.Task{liveTask},
		Artifacts:    []domain.Artifact{liveArtifact},
	}
	reconciler := &CampaignRefReleaseReconciler{
		Records: func(context.Context) (sqlite.CoordinatorRecords, error) { return records, nil },
		Refs:    refs,
	}
	report := reconciler.Tick(ctx)
	if len(report.Errors) != 0 || strings.Join(report.Released, ",") != "run-forgotten" {
		t.Fatalf("released %v, errors %v", report.Released, report.Errors)
	}
	runs, err := refs.Runs()
	if err != nil || strings.Join(runs, ",") != "run-live" {
		t.Fatalf("runs after the pass = %v, %v", runs, err)
	}
	// A store it cannot list is not an excuse to release nothing quietly.
	blind := &countingReleaser{listErr: os.ErrPermission}
	blindReconciler := &CampaignRefReleaseReconciler{
		Records: func(context.Context) (sqlite.CoordinatorRecords, error) { return records, nil },
		Refs:    blind,
	}
	if report := blindReconciler.Tick(ctx); len(report.Errors) != 1 || blind.calls != 0 {
		t.Fatalf("unlistable store: errors %v, releases %d", report.Errors, blind.calls)
	}
}
