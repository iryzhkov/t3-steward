package backlogadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// stagedRecord is the provenance record a review-declared producer's worker
// leaves for its staged commit: the published record's fields and the
// attempt that staged it. It is written as JSON so the test reads the same
// on a build whose record type has no such field.
func stagedRecord(t *testing.T, provenance backlog.CommitProvenance, attemptID string) []byte {
	t.Helper()
	raw, err := backlog.MarshalCommitProvenance(provenance)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	fields["stagedAttempt"] = attemptID
	if raw, err = json.Marshal(fields); err != nil {
		t.Fatal(err)
	}
	return raw
}

// rc.116 already had review-declared tasks, and they published their commits
// directly, so their records name no staging. Such a commit is a published
// campaign output and exports as one after the coordinator upgrades.
func TestExportCommitOfAPublishedReviewDeclaredCommit(t *testing.T) {
	c := newCommitCampaign(t)
	ctx := context.Background()
	records, err := c.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range records.Tasks {
		if task.ID == "implement" {
			task.ReviewRequirements = &domain.TaskReviewRequirements{Version: 1, Risk: "routine", RequiredReviewers: 1, MinProviderFamilies: 1, RoundLimit: 1}
			if err := c.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Tasks: []domain.Task{task}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	c.service.SetCommitBundleOpener(func(ctx context.Context, p backlog.CommitProvenance, _ domain.Artifact) (io.ReadCloser, error) {
		path, err := c.refs.ExportPublishedBundle(ctx, p, backlog.DefaultCommitBundleMaxBytes)
		if err != nil {
			return nil, err
		}
		t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(path)) })
		return os.Open(path)
	})
	result, err := c.service.ExportCommit(ctx, Principal{ID: "operator"}, CommitExportRequest{RunID: "run", TaskID: "implement", Name: "implementation", Branch: "review"})
	if err != nil {
		t.Fatalf("export a published commit of a review-declared task: %v", err)
	}
	_ = result.Content.Close()
}

// A rerun that carries a review-declared producer's commit carries its staged
// record, so placement requires what holding staged work needs, as it does for
// a consumer in the producer's own run.
func TestARerunCarryingAReviewedCommitRequiresAcceptedDependencies(t *testing.T) {
	c := newCommitCampaign(t)
	ctx := context.Background()
	records, err := c.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range records.Tasks {
		if task.ID == "implement" {
			task.ReviewRequirements = &domain.TaskReviewRequirements{Version: 1, Risk: "routine", RequiredReviewers: 1, MinProviderFamilies: 1, RoundLimit: 1}
			if err := c.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Tasks: []domain.Task{task}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	result, err := c.service.AmendGraph(ctx, Principal{ID: "operator"}, domain.GraphAmendment{
		ID: "rerun-1", RunID: "run", Operation: "rerun", TaskID: "review",
		ExpectedRevision: 1, Reason: "review failed on a stale checkout",
	})
	if err != nil {
		t.Fatalf("author the rerun: %v", err)
	}
	got := result.Graph.Tasks[0].Placement.Capabilities
	for _, want := range []string{workerproto.PackageCapabilityCommitBundle, workerproto.PackageCapabilityAcceptedDependencies} {
		if !slices.Contains(got, want) {
			t.Fatalf("placement capabilities %v lack %q", got, want)
		}
	}
}

// H3's operator export of a review-gated leaf, whose staged commit no
// consumer ever promotes: the coordinator falls back to the producing worker,
// which exports the staging, and only once the coordinator accepted the
// attempt that staged it. A failed attempt's staged commit, or a record that
// names another attempt's staging, is refused before the worker is asked.
func TestExportCommitOfAReviewedLeafNeedsItsAcceptedAttempt(t *testing.T) {
	c := newCommitCampaign(t)
	ctx := context.Background()
	// The producing worker staged the commit and never published it.
	worker := backlog.CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	staged, err := worker.Stage(ctx, backlog.PublishCommitRequest{
		WorkflowRunID: "run", TaskID: "implement", Name: "implementation",
		Repository: c.provenance.Repository, WorkspaceDir: c.provenance.Repository,
		Base: c.provenance.Base, CreatedAt: commitCampaignTime,
	}, "attempt-implement", nil)
	if err != nil {
		t.Fatal(err)
	}
	records, err := c.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var origin domain.Artifact
	for _, a := range records.Artifacts {
		if a.Name == "implementation" && a.Kind == domain.ArtifactOutput {
			origin = a
		}
	}
	record := stagedRecord(t, staged, "attempt-implement")
	c.service.SetArtifactOpener(func(context.Context, string) (domain.Artifact, io.ReadCloser, error) {
		return origin, io.NopCloser(bytes.NewReader(record)), nil
	})
	asked := 0
	c.service.SetCommitBundleOpener(func(ctx context.Context, p backlog.CommitProvenance, _ domain.Artifact) (io.ReadCloser, error) {
		asked++
		path, err := worker.ExportPublishedBundle(ctx, p, backlog.DefaultCommitBundleMaxBytes)
		if err != nil {
			return nil, err
		}
		t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(path)) })
		return os.Open(path)
	})
	request := CommitExportRequest{RunID: "run", TaskID: "implement", Name: "implementation", Branch: "review"}

	result, err := c.service.ExportCommit(ctx, Principal{ID: "operator"}, request)
	if err != nil {
		t.Fatalf("export the accepted staged commit of a reviewed leaf: %v", err)
	}
	raw, err := io.ReadAll(result.Content)
	if closeErr := result.Content.Close(); err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	exported := filepath.Join(t.TempDir(), "result.bundle")
	if err := os.WriteFile(exported, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if heads := gitFixtureOutput(t, c.provenance.Repository, "bundle", "list-heads", exported); heads != staged.Commit+" refs/heads/review" || asked != 1 {
		t.Fatalf("heads=%q asked=%d", heads, asked)
	}

	// Another attempt's staging is not this attempt's accepted work.
	record = stagedRecord(t, staged, "attempt-other")
	if _, err := c.service.ExportCommit(ctx, Principal{ID: "operator"}, request); err == nil || asked != 1 {
		t.Fatalf("exported another attempt's staging: %v, asked=%d", err, asked)
	}

	// The review gate refused the attempt.
	record = stagedRecord(t, staged, "attempt-implement")
	for index := range records.Attempts {
		if records.Attempts[index].ID == "attempt-implement" {
			attempt := records.Attempts[index]
			attempt.Progress = domain.ProgressFailed
			attempt.Revision++
			if err := c.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := c.service.ExportCommit(ctx, Principal{ID: "operator"}, request); err == nil || !strings.Contains(err.Error(), "accepted") || asked != 1 {
		t.Fatalf("exported a staged commit the coordinator never accepted: %v, asked=%d", err, asked)
	}
}
