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

// campaignRunRecords is one settled campaign whose single task declared one
// commit, together with the provenance record that names it.
func campaignRunRecords(runID, taskID, name string) (domain.WorkflowRun, domain.Task, domain.Artifact) {
	run := domain.WorkflowRun{ID: runID, WorkflowID: "workflow-1"}
	task := domain.Task{
		ID: taskID, WorkflowID: "workflow-1", RunID: runID, Name: "producer",
		Outputs: []domain.ArtifactDeclaration{{Name: name, Commit: &domain.CommitOutput{}}},
	}
	artifact := domain.Artifact{
		ID: "output-" + taskID, WorkflowRunID: runID, TaskID: taskID,
		AttemptID: "attempt-" + taskID, Kind: domain.ArtifactOutput, Name: name,
		MediaType: "application/json", Producer: "worker:test",
	}
	return run, task, artifact
}

func settle(run *domain.WorkflowRun) {
	run.Sink = &domain.SinkTask{Name: domain.SinkTaskName, Progress: domain.ProgressSucceeded}
}

// The store exists to keep a commit reachable for as long as anything can ask
// for it, and the thing that can ask is the provenance record. So the refs of a
// settled campaign survive while its record is retained and go when retention
// removes it, and across many completed campaigns the store holds the refs of
// the campaigns whose records still exist rather than the sum of all of them.
func TestCampaignRefReleaseFollowsArtifactRetention(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	var records sqlite.CoordinatorRecords
	reconciler := &CampaignRefReleaseReconciler{
		Records: func(context.Context) (sqlite.CoordinatorRecords, error) { return records, nil },
		Refs:    refs,
	}
	// Retention removes a campaign's record one campaign after it finished, so
	// at most two campaigns can be holding refs at any moment.
	prune := func(runID string) {
		kept := records.Artifacts[:0]
		for _, artifact := range records.Artifacts {
			if artifact.WorkflowRunID != runID {
				kept = append(kept, artifact)
			}
		}
		records.Artifacts = kept
	}

	peak := 0
	for campaign := 1; campaign <= 5; campaign++ {
		runID := fmt.Sprintf("run-%d", campaign)
		writeGitFile(t, repository, "version.txt", fmt.Sprintf("campaign %d\n", campaign))
		gitRun(t, repository, "add", "version.txt")
		gitRun(t, repository, "commit", "-m", runID)
		base := gitOutput(t, repository, "rev-parse", "HEAD~1")
		taskID := fmt.Sprintf("task-%d", campaign)
		if _, err := refs.Publish(ctx, PublishCommitRequest{
			WorkflowRunID: runID, TaskID: taskID, Name: "handoff",
			Repository: repository, WorkspaceDir: repository, Base: base,
		}, nil); err != nil {
			t.Fatalf("publish campaign %d: %v", campaign, err)
		}
		run, task, artifact := campaignRunRecords(runID, taskID, "handoff")
		records.WorkflowRuns = append(records.WorkflowRuns, run)
		records.Tasks = append(records.Tasks, task)
		records.Artifacts = append(records.Artifacts, artifact)

		// A live campaign is never released: its commit is the handoff its own
		// successor is about to fetch.
		if report := reconciler.Tick(ctx); len(report.Released) != 0 {
			t.Fatalf("campaign %d was released while it was still running: %v", campaign, report.Released)
		}
		settle(&records.WorkflowRuns[campaign-1])
		// Settling is not the boundary. The record still exists, so a rerun
		// authored now would still find the commit.
		if report := reconciler.Tick(ctx); len(report.Released) != 0 {
			t.Fatalf("campaign %d was released while its provenance record was retained: %v", campaign, report.Released)
		}
		if live := campaignRefCount(t, refs); live > peak {
			peak = live
		}
		if campaign == 1 {
			continue
		}
		previous := fmt.Sprintf("run-%d", campaign-1)
		prune(previous)
		report := reconciler.Tick(ctx)
		if len(report.Errors) != 0 || strings.Join(report.Released, ",") != previous {
			t.Fatalf("after pruning %s: released %v, errors %v", previous, report.Released, report.Errors)
		}
	}
	prune("run-5")
	if report := reconciler.Tick(ctx); len(report.Errors) != 0 || strings.Join(report.Released, ",") != "run-5" {
		t.Fatalf("final release = %v, errors %v", report.Released, report.Errors)
	}
	if live := campaignRefCount(t, refs); live != 0 {
		t.Fatalf("%d ref(s) outlived every provenance record", live)
	}
	if peak > 2 {
		t.Fatalf("the store held %d refs at once under a two-campaign retention window", peak)
	}
	if entries, err := os.ReadDir(filepath.Join(refs.Root, "provenance")); err != nil || len(entries) != 0 {
		t.Fatalf("provenance records after five campaigns = %v, %v", entries, err)
	}
}

// The two lists are complements over the runs that declared a commit, because
// the worker statement releases exactly what the coordinator did not retain.
// A run that declared none is in neither: there is nothing to keep and nothing
// to delete.
func TestCampaignRefLifetimeSplitsOnlyTheRunsThatDeclaredCommits(t *testing.T) {
	live, liveTask, liveArtifact := campaignRunRecords("run-live", "task-live", "handoff")
	held, heldTask, heldArtifact := campaignRunRecords("run-held", "task-held", "handoff")
	settle(&held)
	gone, goneTask, _ := campaignRunRecords("run-gone", "task-gone", "handoff")
	settle(&gone)
	plain := domain.WorkflowRun{ID: "run-plain", WorkflowID: "workflow-1"}
	settle(&plain)
	plainTask := domain.Task{
		ID: "task-plain", WorkflowID: "workflow-1", RunID: "run-plain", Name: "producer",
		Outputs: []domain.ArtifactDeclaration{{Name: "report.md"}},
	}
	records := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{live, held, gone, plain},
		Tasks:        []domain.Task{liveTask, heldTask, goneTask, plainTask},
		Artifacts:    []domain.Artifact{liveArtifact, heldArtifact},
	}
	retained, releasable := CampaignRefLifetime(records)
	if strings.Join(retained, ",") != "run-held,run-live" {
		t.Fatalf("retained = %v", retained)
	}
	if strings.Join(releasable, ",") != "run-gone" {
		t.Fatalf("releasable = %v", releasable)
	}
}

// An input artifact of another run that happens to share the name is not this
// run's provenance record: a rerun's carried reference belongs to the new run,
// and what holds the source is the pin on it, not a name collision.
func TestCampaignRefLifetimeCountsOnlyTheRunsOwnOutput(t *testing.T) {
	run, task, artifact := campaignRunRecords("run-1", "task-1", "handoff")
	settle(&run)
	carried := artifact
	carried.ID = "input:rerun:key:0"
	carried.WorkflowRunID = "run:rerun:key"
	carried.TaskID = "task:rerun:key:0"
	carried.Kind = domain.ArtifactInput
	records := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{run},
		Tasks:        []domain.Task{task},
		Artifacts:    []domain.Artifact{carried},
	}
	_, releasable := CampaignRefLifetime(records)
	if strings.Join(releasable, ",") != "run-1" {
		t.Fatalf("releasable = %v", releasable)
	}
}

// Releasing twice is a no-op, and a reconciler that restarted and forgot what
// it released reaches the store's own idempotent early return.
func TestCampaignRefReleaseIsIdempotent(t *testing.T) {
	ctx := context.Background()
	repository := newGitFixture(t)
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	refs := CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	if _, err := refs.Publish(ctx, PublishCommitRequest{
		WorkflowRunID: "run-1", TaskID: "task-1", Name: "handoff",
		Repository: repository, WorkspaceDir: repository, Base: base,
	}, nil); err != nil {
		t.Fatalf("publish: %v", err)
	}
	run, task, _ := campaignRunRecords("run-1", "task-1", "handoff")
	settle(&run)
	records := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{run}, Tasks: []domain.Task{task},
	}
	load := func(context.Context) (sqlite.CoordinatorRecords, error) { return records, nil }
	reconciler := &CampaignRefReleaseReconciler{Records: load, Refs: refs}
	if report := reconciler.Tick(ctx); len(report.Errors) != 0 || strings.Join(report.Released, ",") != "run-1" {
		t.Fatalf("first pass = %v, errors %v", report.Released, report.Errors)
	}
	if report := reconciler.Tick(ctx); len(report.Released) != 0 || len(report.Errors) != 0 {
		t.Fatalf("second pass = %v, errors %v", report.Released, report.Errors)
	}
	restarted := &CampaignRefReleaseReconciler{Records: load, Refs: refs}
	if report := restarted.Tick(ctx); len(report.Errors) != 0 {
		t.Fatalf("a restarted reconciler failed to release nothing: %v", report.Errors)
	}
	if campaignRefCount(t, refs) != 0 {
		t.Fatal("a released campaign still pins its commit")
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
	run, task, _ := campaignRunRecords("run-1", "task-1", "handoff")
	settle(&run)
	records := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{run}, Tasks: []domain.Task{task},
	}
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
