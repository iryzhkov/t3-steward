package workerruntime

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// Review round 2, finding 1, on the worker: the package is the only trusted
// source of a dependency's declared commit outputs. A record in any other
// dependency file publishes nothing, even one naming its own producer and its
// own file; a record naming another producer publishes nothing even from a
// coordinator that does not mark commit outputs, where every file is a
// candidate and the record is refused; and the marked commit output is still
// resolved.
func TestLocalDriverResolvesCommitRecordsOnlyFromMarkedOutputs(t *testing.T) {
	ctx := context.Background()
	repository, commit := makeGitRepository(t)
	const repositoryURL = "https://example.com/steward.git"
	scratch := backlog.CampaignRefStore{Root: filepath.Join(t.TempDir(), "scratch-refs")}
	published, err := scratch.Publish(ctx, backlog.PublishCommitRequest{
		WorkflowRunID: "run-1", TaskID: "producer", Name: "change", Repository: repositoryURL,
		WorkspaceDir: repository, Base: commit,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	record := func(task, name string) []byte {
		t.Helper()
		provenance := published
		provenance.TaskID, provenance.Name, provenance.Ref = task, name, backlog.CampaignRef("run-1", task, name)
		raw, err := backlog.MarshalCommitProvenance(provenance)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	catalog, err := backlog.NewProjectCatalog(
		[]backlog.ProjectDefinition{{Name: "steward", Repository: repositoryURL, DefaultRef: commit, T3ProjectTemplate: "development", SetupProfile: "go"}},
		[]backlog.SetupProfile{{Name: "go", Commands: []string{"true"}, Timeout: time.Minute}},
	)
	if err != nil {
		t.Fatal(err)
	}
	prepare := func(t *testing.T, marked bool) (backlog.CampaignRefStore, error) {
		t.Helper()
		refs := backlog.CampaignRefStore{Root: filepath.Join(t.TempDir(), "campaign-refs")}
		change, notes, poison := record("producer", "change"), record("producer", "notes"), record("victim", "change")
		pkg := testPackage()
		pkg.Environment.Repository = repositoryURL
		pkg.Environment.Ref = commit
		objects := []workerproto.ArtifactObject{
			testArtifact("change-1", "dependencies/producer/change", string(change)),
			testArtifact("notes-1", "dependencies/producer/notes", string(notes)),
			testArtifact("poison-1", "dependencies/attacker/change", string(poison)),
		}
		pkg.Dependencies = []workerproto.DependencyInput{
			{TaskID: "attacker", Artifacts: objects[2:]},
			{TaskID: "producer", Artifacts: objects[:2]},
		}
		if marked {
			pkg.Dependencies[1].CommitOutputs = []string{"change"}
			pkg.RequiredCapabilities = []string{workerproto.PackageCapabilityCommitOutputs}
		}
		root := t.TempDir()
		driver, err := NewLocalDriver(LocalDriver{
			Config:  LocalDriverConfig{CatalogRevision: "catalog-1", ArtifactRoot: filepath.Join(root, "artifacts"), RunsRoot: filepath.Join(root, "runs")},
			Catalog: catalog,
			Workspace: backlog.WorkspacePreparer{
				Cache: staticRepositoryCache{path: repository}, Processes: successfulProcessRunner{}, CampaignRefs: refs,
			},
			Finalizer: backlog.AttemptFinalizer{Processes: successfulProcessRunner{}},
			Source: mapArtifactSource{pkg.Prompt.ID: []byte("prompt"),
				objects[0].ID: change, objects[1].ID: notes, objects[2].ID: poison},
			Publisher: &recordingPublisher{}, T3: &recordingT3{}, Now: func() time.Time { return runtimeTestNow },
		})
		if err != nil {
			t.Fatal(err)
		}
		workspace, err := driver.Prepare(ctx, pkg)
		if err == nil {
			t.Cleanup(func() { _ = driver.Cleanup(context.Background(), pkg, workspace) })
		}
		return refs, err
	}

	legacy, err := prepare(t, false)
	if err == nil || !strings.Contains(err.Error(), `but its record names task "victim"`) {
		t.Fatalf("unmarked package: a record naming another producer was not refused: %v", err)
	}
	if provenance, err := legacy.Resolve("run-1", "victim", "change"); err == nil {
		t.Fatalf("unmarked package: another producer's output published the victim's ref: %+v", provenance)
	}

	refs, err := prepare(t, true)
	if err != nil {
		t.Fatal(err)
	}
	if provenance, err := refs.Resolve("run-1", "victim", "change"); err == nil {
		t.Fatalf("another producer's output published the victim's ref: %+v", provenance)
	}
	if provenance, err := refs.Resolve("run-1", "producer", "notes"); err == nil {
		t.Fatalf("an unmarked output published a campaign ref: %+v", provenance)
	}
	if provenance, err := refs.Resolve("run-1", "producer", "change"); err != nil || provenance.Commit != commit {
		t.Fatalf("declared commit output resolved %+v %v", provenance, err)
	}
}
