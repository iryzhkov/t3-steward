package workerruntime

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// A review-declared producer's commit is only staged on this worker. Preparing
// a consumer publishes it as the producer's campaign output only when the
// package marks the dependency accepted; a review judge given the result
// without that mark still receives the commit, and publishes nothing.
func TestLocalDriverPublishesAStagedDependencyCommitOnlyWhenAccepted(t *testing.T) {
	ctx := context.Background()
	repository, commit := makeGitRepository(t)
	refs := backlog.CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
	staged, err := refs.Stage(ctx, backlog.PublishCommitRequest{
		WorkflowRunID: "run-1", TaskID: "producer", Name: "change", Repository: "https://example.com/steward.git",
		WorkspaceDir: repository, Base: commit,
	}, "producer-attempt", nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := backlog.MarshalCommitProvenance(staged)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := backlog.NewProjectCatalog(
		[]backlog.ProjectDefinition{{Name: "steward", Repository: "https://example.com/steward.git", DefaultRef: commit, T3ProjectTemplate: "development", SetupProfile: "go"}},
		[]backlog.SetupProfile{{Name: "go", Commands: []string{"true"}, Timeout: time.Minute}},
	)
	if err != nil {
		t.Fatal(err)
	}
	prepare := func(t *testing.T, accepted bool) {
		t.Helper()
		pkg := testPackage()
		pkg.Environment.Repository = "https://example.com/steward.git"
		pkg.Environment.Ref = commit
		reference := testArtifact("commit-1", "dependencies/producer/change", string(raw))
		pkg.Dependencies = []workerproto.DependencyInput{{TaskID: "producer", Artifacts: []workerproto.ArtifactObject{reference}}}
		if accepted {
			pkg.Dependencies[0].AcceptedCommits = []string{"change"}
			pkg.RequiredCapabilities = []string{workerproto.PackageCapabilityAcceptedDependencies}
		}
		root := t.TempDir()
		driver, err := NewLocalDriver(LocalDriver{
			Config:  LocalDriverConfig{CatalogRevision: "catalog-1", ArtifactRoot: filepath.Join(root, "artifacts"), RunsRoot: filepath.Join(root, "runs")},
			Catalog: catalog,
			Workspace: backlog.WorkspacePreparer{
				Cache: staticRepositoryCache{path: repository}, Processes: successfulProcessRunner{}, CampaignRefs: refs,
			},
			Finalizer: backlog.AttemptFinalizer{Processes: successfulProcessRunner{}},
			Source:    mapArtifactSource{pkg.Prompt.ID: []byte("prompt"), reference.ID: raw},
			Publisher: &recordingPublisher{}, T3: &recordingT3{}, Now: func() time.Time { return runtimeTestNow },
		})
		if err != nil {
			t.Fatal(err)
		}
		workspace, err := driver.Prepare(ctx, pkg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = driver.Cleanup(context.Background(), pkg, workspace) })
	}

	prepare(t, false)
	if provenance, err := refs.Resolve("run-1", "producer", "change"); err == nil {
		t.Fatalf("an unaccepted dependency published the staged commit: %+v", provenance)
	}
	prepare(t, true)
	if provenance, err := refs.Resolve("run-1", "producer", "change"); err != nil || provenance.Commit != commit {
		t.Fatalf("accepted dependency published %+v %v, want %s", provenance, err, commit)
	}
}
