package backlogadmin

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

var commitCampaignTime = time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC)

// commitCampaign is the shape the declared-commit work exists for: implement
// produces a commit, review consumes it, the run finished with review failed.
// The commit is published into a real campaign ref store and its provenance
// record is the retained artifact, exactly as a worker would leave them.
type commitCampaign struct {
	service    *Service
	store      *sqlite.Store
	refs       backlog.CampaignRefStore
	artifacts  backlog.CoordinatorArtifactStore
	provenance backlog.CommitProvenance
}

func newCommitCampaign(t *testing.T) commitCampaign {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	artifactRoot := filepath.Join(root, "artifacts")
	repository := newCommitRepository(t)
	refs := backlog.CampaignRefStore{Root: filepath.Join(root, "campaign-refs")}
	provenance, err := refs.Publish(ctx, backlog.PublishCommitRequest{
		WorkflowRunID: "run", TaskID: "implement", Name: "implementation",
		Repository:   repository,
		WorkspaceDir: repository,
		Base:         gitFixtureOutput(t, repository, "rev-parse", "HEAD"),
		CreatedAt:    commitCampaignTime,
	}, nil)
	if err != nil {
		t.Fatalf("publish declared commit: %v", err)
	}
	record, err := backlog.MarshalCommitProvenance(provenance)
	if err != nil {
		t.Fatal(err)
	}

	store, err := sqlite.OpenMigrated(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	records := sqlite.CoordinatorRecords{Workflows: []domain.Workflow{{
		ID: "workflow", Name: "campaign", TaskIDs: []string{"implement", "review"},
	}}}
	progress := map[string]domain.ProgressState{
		"implement": domain.ProgressSucceeded, "review": domain.ProgressFailed,
	}
	for _, name := range []string{"implement", "review"} {
		task := domain.Task{
			ID: name, WorkflowID: "workflow", Name: name, Class: domain.TaskClassSurplus,
			MaxTurns: 1, PromptArtifactID: "prompt-" + name,
			Routes: []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "gpt"}},
		}
		if name == "implement" {
			task.Outputs = []domain.ArtifactDeclaration{{
				Name: "implementation", MediaType: "application/json",
				Commit: &domain.CommitOutput{},
			}}
		} else {
			task.Needs = []string{"implement"}
			task.DependencyInputs = map[string][]string{"implement": {"implementation"}}
		}
		prompt, err := backlog.PrepareGraphInput(artifactRoot, task.PromptArtifactID, "run", name, "prompt "+name, commitCampaignTime)
		if err != nil {
			t.Fatal(err)
		}
		records.Tasks = append(records.Tasks, task)
		records.Artifacts = append(records.Artifacts, prompt)
		records.Attempts = append(records.Attempts, domain.Attempt{
			ID: "attempt-" + name, WorkflowRunID: "run", TaskID: name, Number: 1, Revision: 1,
			Progress: progress[name], Control: domain.ControlStopped, UpdatedAt: commitCampaignTime,
		})
	}
	// The retained artifact of a declared commit is its provenance record.
	commitRecord, err := backlog.PrepareGraphInput(artifactRoot, "output-implement", "run", "implement", string(record), commitCampaignTime)
	if err != nil {
		t.Fatal(err)
	}
	commitRecord.Kind = domain.ArtifactOutput
	commitRecord.Name = "implementation"
	commitRecord.MediaType = "application/json"
	commitRecord.AttemptID = "attempt-implement"
	commitRecord.Producer = "worker:test"
	records.Artifacts = append(records.Artifacts, commitRecord)

	run, err := domain.BindRunSink(domain.WorkflowRun{
		ID: "run", WorkflowID: "workflow", GraphRevision: 1, Revision: 1,
		Progress: domain.ProgressFailed, CreatedAt: commitCampaignTime, UpdatedAt: commitCampaignTime,
	}, records.Tasks)
	if err != nil {
		t.Fatal(err)
	}
	completed := commitCampaignTime.Add(time.Hour)
	run.CompletedAt = &completed
	run.Sink.Progress = domain.ProgressFailed
	run.Sink.CompletedAt = &completed
	records.WorkflowRuns = append(records.WorkflowRuns, run)
	if err = store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}

	service, err := New(store, graphAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return commitCampaignTime.Add(2 * time.Hour) })
	service.SetGraphAmendmentSupport(artifactRoot, func(domain.Workflow, domain.Task) error { return nil })
	artifacts := backlog.CoordinatorArtifactStore{Root: artifactRoot, Catalog: store}
	service.SetArtifactOpener(func(ctx context.Context, id string) (domain.Artifact, io.ReadCloser, error) {
		return artifacts.Open(ctx, id)
	})
	return commitCampaign{service: service, store: store, refs: refs, artifacts: artifacts, provenance: provenance}
}

func (c commitCampaign) lifetime(t *testing.T) (retained, releasable []string) {
	t.Helper()
	records, err := c.store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return backlog.CampaignRefLifetime(records)
}

// resolves reports whether the published commit can still be fetched by the
// reference the provenance record names, which is the only thing a successor
// ever does with it.
func (c commitCampaign) resolves(t *testing.T) bool {
	t.Helper()
	consumer := t.TempDir()
	gitFixtureRun(t, consumer, "init", "--initial-branch=main")
	return c.refs.FetchInto(context.Background(), consumer, c.provenance, io.Discard) == nil
}

// A rerun can only be authored once the source run has finished, so the commits
// of a finished run must outlive settlement. They live as long as the provenance
// record does, and a rerun pins that record, so a rerun authored after the run
// settled still resolves the commit it carried.
func TestARerunAuthoredAfterSettlementStillResolvesTheCarriedCommit(t *testing.T) {
	ctx := context.Background()
	campaign := newCommitCampaign(t)

	retained, releasable := campaign.lifetime(t)
	if strings.Join(retained, ",") != "run" || len(releasable) != 0 {
		t.Fatalf("a settled run with a retained record: retained %v, releasable %v", retained, releasable)
	}

	// The rerun is authored now, after settlement, which is the only time it is
	// allowed. It carries implement's declared commit into the new run.
	result, err := campaign.service.AmendGraph(ctx, Principal{ID: "operator"}, domain.GraphAmendment{
		ID: "rerun-1", RunID: "run", Operation: "rerun", TaskID: "review",
		ExpectedRevision: 1, Reason: "review failed on a stale checkout",
	})
	if err != nil {
		t.Fatalf("author the rerun: %v", err)
	}
	carried := result.Graph.Tasks[0].CarriedInputs
	if len(carried) != 1 || carried[0].Name != "implementation" || carried[0].ProducerTaskID != "implement" {
		t.Fatalf("carried inputs = %+v", carried)
	}
	if !campaign.resolves(t) {
		t.Fatal("the commit the rerun carried is no longer reachable")
	}

	// Retention runs and finds everything old enough to prune. The rerun pins
	// its source, so the record it carried survives: whether the prune refuses
	// or skips it, what matters is that the record and the commit are still
	// there for the run that needs them.
	if _, err := campaign.store.PruneArtifacts(ctx, commitCampaignTime.Add(24*time.Hour), nil); err != nil {
		t.Logf("prune refused while the source was pinned: %v", err)
	}
	retained, releasable = campaign.lifetime(t)
	if strings.Join(retained, ",") != "run" || len(releasable) != 0 {
		t.Fatalf("a pinned source: retained %v, releasable %v", retained, releasable)
	}
	if !campaign.resolves(t) {
		t.Fatal("retention took the commit away from a live rerun")
	}
}

// With nothing holding it, retention removes the provenance record and the same
// rule releases the ref. Afterwards the commit is gone, which is the bound: a
// campaign nobody can ask about stops costing anything.
func TestRetentionReleasesTheCampaignRefWithItsProvenanceRecord(t *testing.T) {
	ctx := context.Background()
	campaign := newCommitCampaign(t)
	if !campaign.resolves(t) {
		t.Fatal("the published commit does not resolve before retention")
	}
	if _, err := campaign.store.PruneArtifacts(ctx, commitCampaignTime.Add(24*time.Hour), nil); err != nil {
		t.Fatalf("prune artifacts: %v", err)
	}
	retained, releasable := campaign.lifetime(t)
	if len(retained) != 0 || strings.Join(releasable, ",") != "run" {
		t.Fatalf("after retention: retained %v, releasable %v", retained, releasable)
	}
	reconciler := &backlog.CampaignRefReleaseReconciler{
		Records: campaign.store.LoadCoordinatorRecords, Refs: campaign.refs,
	}
	report := reconciler.Tick(ctx)
	if len(report.Errors) != 0 || strings.Join(report.Released, ",") != "run" {
		t.Fatalf("release = %v, errors %v", report.Released, report.Errors)
	}
	if campaign.resolves(t) {
		t.Fatal("a released campaign commit is still reachable")
	}
}

func newCommitRepository(t *testing.T) string {
	t.Helper()
	repository := t.TempDir()
	gitFixtureRun(t, repository, "init", "--initial-branch=main")
	gitFixtureRun(t, repository, "config", "user.name", "Test User")
	gitFixtureRun(t, repository, "config", "user.email", "test@example.test")
	if err := os.WriteFile(filepath.Join(repository, "version.txt"), []byte("first\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitFixtureRun(t, repository, "add", "version.txt")
	gitFixtureRun(t, repository, "commit", "-m", "first")
	return repository
}

func gitFixtureRun(t *testing.T, dir string, args ...string) {
	t.Helper()
	gitFixtureOutput(t, dir, args...)
}

func gitFixtureOutput(t *testing.T, dir string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = dir
	command.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	raw, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, raw)
	}
	return strings.TrimSpace(string(raw))
}
