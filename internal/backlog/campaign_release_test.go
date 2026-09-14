package backlog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// The store exists to keep a commit reachable for a campaign's lifetime, so the
// property that matters is that the lifetime ends: several campaigns run to
// completion one after another and the store holds the refs of at most the
// campaign that is still running, never the sum of all of them.
func TestCampaignRefReleaseBoundsTheStore(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	var records sqlite.CoordinatorRecords
	reconciler := &CampaignRefReleaseReconciler{
		Records: func(context.Context) (sqlite.CoordinatorRecords, error) { return records, nil },
		Refs:    refs,
	}

	peak := 0
	for campaign := 1; campaign <= 5; campaign++ {
		runID := fmt.Sprintf("run-%d", campaign)
		taskID := fmt.Sprintf("task-%d", campaign)
		writeGitFile(t, repository, "version.txt", fmt.Sprintf("campaign %d\n", campaign))
		gitRun(t, repository, "add", "version.txt")
		gitRun(t, repository, "commit", "-m", runID)
		base := gitOutput(t, repository, "rev-parse", "HEAD~1")
		if _, err := refs.Publish(ctx, PublishCommitRequest{
			WorkflowRunID: runID, TaskID: taskID, Name: "handoff",
			Repository: repository, WorkspaceDir: repository, Base: base,
		}, nil); err != nil {
			t.Fatalf("publish campaign %d: %v", campaign, err)
		}
		records.WorkflowRuns = append(records.WorkflowRuns, domain.WorkflowRun{ID: runID, WorkflowID: "workflow-1"})
		records.Tasks = append(records.Tasks, domain.Task{
			ID: taskID, WorkflowID: "workflow-1", RunID: runID, Name: "producer",
			Outputs: []domain.ArtifactDeclaration{{Name: "handoff", Commit: &domain.CommitOutput{}}},
		})
		if live := campaignRefCount(t, refs); live > peak {
			peak = live
		}

		// The campaign finishes: its sink settles and the next boundary runs.
		records.WorkflowRuns[campaign-1].Sink = &domain.SinkTask{
			Name: domain.SinkTaskName, Progress: domain.ProgressSucceeded,
		}
		report := reconciler.Tick(ctx)
		if len(report.Errors) != 0 {
			t.Fatalf("release errors after campaign %d: %v", campaign, report.Errors)
		}
		if strings.Join(report.Released, ",") != runID {
			t.Fatalf("released %v after campaign %d, want %s", report.Released, campaign, runID)
		}
		if live := campaignRefCount(t, refs); live != 0 {
			t.Fatalf("campaign %d left %d ref(s) pinned", campaign, live)
		}
	}
	if peak != 1 {
		t.Fatalf("the store held %d refs at once; the test no longer proves a bound", peak)
	}
	if entries, err := os.ReadDir(filepath.Join(refs.Root, "provenance")); err != nil || len(entries) != 0 {
		t.Fatalf("provenance records after five campaigns = %v, %v", entries, err)
	}
}

// A rerun consumes an ancestor's declared commit as a carried input, and the
// carried record names the source run's campaign ref. Releasing the source
// while the new run still needs it would take the commit away from a run that
// has not started yet, so the source is held until the rerun settles too.
func TestCampaignRefReleaseHoldsARerunsCarriedCommit(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	if _, err := refs.Publish(ctx, PublishCommitRequest{
		WorkflowRunID: "run-1", TaskID: "task-implement", Name: "implementation",
		Repository: repository, WorkspaceDir: repository, Base: base,
	}, nil); err != nil {
		t.Fatalf("publish: %v", err)
	}

	settled := &domain.SinkTask{Name: domain.SinkTaskName, Progress: domain.ProgressFailed}
	records := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{
			{ID: "run-1", WorkflowID: "workflow-1", Sink: settled},
			{ID: "run:rerun:key", WorkflowID: "workflow-1"},
		},
		Tasks: []domain.Task{
			{
				ID: "task-implement", WorkflowID: "workflow-1", RunID: "run-1", Name: "implement",
				Outputs: []domain.ArtifactDeclaration{{Name: "implementation", Commit: &domain.CommitOutput{}}},
			},
			{ID: "task-review", WorkflowID: "workflow-1", RunID: "run-1", Name: "review"},
			{
				ID: "task:rerun:key:0", WorkflowID: "workflow-1", RunID: "run:rerun:key", Name: "review",
				CarriedInputs: []domain.CarriedInput{{
					Producer: "implement", ProducerTaskID: "task-implement",
					Name: "implementation", ArtifactID: "input:rerun:key:0",
				}},
			},
		},
	}
	reconciler := &CampaignRefReleaseReconciler{
		Records: func(context.Context) (sqlite.CoordinatorRecords, error) { return records, nil },
		Refs:    refs,
	}
	if report := reconciler.Tick(ctx); len(report.Released) != 0 || len(report.Errors) != 0 {
		t.Fatalf("released %v while the rerun still needs the commit (errors %v)", report.Released, report.Errors)
	}
	if _, err := refs.Resolve("run-1", "task-implement", "implementation"); err != nil {
		t.Fatalf("the carried commit was released out from under the rerun: %v", err)
	}

	// The rerun settles, and now nothing needs the source run's commit.
	records.WorkflowRuns[1].Sink = &domain.SinkTask{Name: domain.SinkTaskName, Progress: domain.ProgressSucceeded}
	report := reconciler.Tick(ctx)
	if len(report.Errors) != 0 || strings.Join(report.Released, ",") != "run-1,run:rerun:key" {
		t.Fatalf("released %v, errors %v", report.Released, report.Errors)
	}
	if campaignRefCount(t, refs) != 0 {
		t.Fatal("a settled campaign still pins its commit")
	}
	// Releasing again changes nothing and reports nothing: the reconciler is
	// safe to run on every boundary, and a restart that forgets what it already
	// released reaches the store's own idempotent no-op.
	if report := reconciler.Tick(ctx); len(report.Released) != 0 || len(report.Errors) != 0 {
		t.Fatalf("second pass released %v, errors %v", report.Released, report.Errors)
	}
	fresh := &CampaignRefReleaseReconciler{
		Records: func(context.Context) (sqlite.CoordinatorRecords, error) { return records, nil },
		Refs:    refs,
	}
	if report := fresh.Tick(ctx); len(report.Errors) != 0 {
		t.Fatalf("a restarted reconciler failed to release nothing: %v", report.Errors)
	}
}

// A run that declared no commit is not a Git operation at all: releasing it
// must not create the bare repository, because the store exists only for runs
// that actually pinned something.
func TestCampaignRefReleaseOfARunWithoutCommitsTouchesNothing(t *testing.T) {
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	if err := refs.ReleaseRun(context.Background(), "run-1", nil); err != nil {
		t.Fatalf("release a run that declared nothing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(refs.Root, "campaigns.git")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("releasing a run without commits created the store: %v", err)
	}
}

// A failed release is an operational error: it is reported and the run it
// belongs to is untouched, so the next boundary tries again.
func TestCampaignRefReleaseFailureIsReportedAndRetried(t *testing.T) {
	records := sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{
		ID: "run-1", WorkflowID: "workflow-1",
		Sink: &domain.SinkTask{Name: domain.SinkTaskName, Progress: domain.ProgressSucceeded},
	}}}
	broken := &countingReleaser{err: errors.New("git refused")}
	reconciler := &CampaignRefReleaseReconciler{
		Records: func(context.Context) (sqlite.CoordinatorRecords, error) { return records, nil },
		Refs:    broken,
	}
	for pass := 1; pass <= 2; pass++ {
		report := reconciler.Tick(context.Background())
		if len(report.Released) != 0 || len(report.Errors) != 1 ||
			!strings.Contains(report.Errors[0].Error(), "run-1") {
			t.Fatalf("pass %d reported %v / %v", pass, report.Released, report.Errors)
		}
	}
	if broken.calls != 2 {
		t.Fatalf("a failed release was attempted %d times, want one per boundary", broken.calls)
	}
}

type countingReleaser struct {
	calls int
	err   error
}

func (r *countingReleaser) ReleaseRun(_ context.Context, _ string, _ io.Writer) error {
	r.calls++
	return r.err
}

// campaignRefCount reports how many campaign refs the store currently pins.
func campaignRefCount(t *testing.T, refs CampaignRefStore) int {
	t.Helper()
	gitDir := filepath.Join(refs.Root, "campaigns.git")
	if _, err := os.Stat(gitDir); errors.Is(err, os.ErrNotExist) {
		return 0
	}
	raw, err := exec.Command("git", "--git-dir", gitDir, "for-each-ref", "--format=%(refname)", "refs/campaigns").Output()
	if err != nil {
		t.Fatalf("list campaign refs: %v", err)
	}
	listed := strings.TrimSpace(string(raw))
	if listed == "" {
		return 0
	}
	return len(strings.Split(listed, "\n"))
}
