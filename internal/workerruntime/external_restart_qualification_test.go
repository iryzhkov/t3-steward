package workerruntime

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func qualificationArtifact(id, run, task, attempt, name, path string, body []byte) domain.Artifact {
	sum := sha256.Sum256(body)
	return domain.Artifact{ID: id, WorkflowRunID: run, TaskID: task, AttemptID: attempt, Kind: domain.ArtifactInput, Name: name, MediaType: "text/plain", Size: int64(len(body)), SHA256: fmt.Sprintf("%x", sum), StoragePath: path, Producer: "coordinator", CreatedAt: runtimeTestNow}
}

func TestRestartedExternalInputsBuildAndMaterializeExactMultiOutputProvenance(t *testing.T) {
	ctx := context.Background()
	repository, commit := makeGitRepository(t)
	workflow := domain.Workflow{ID: "workflow", Version: 1, Name: "workflow", Project: "steward", Environment: domain.ExecutionEnvironment{Type: backlog.EnvironmentGit, Scope: backlog.EnvironmentScopeTask}, Class: domain.TaskClassRequired, TaskIDs: []string{"consumer"}}
	run := domain.WorkflowRun{ID: "target-run", WorkflowID: workflow.ID, Progress: domain.ProgressActive, Revision: 1, CreatedAt: runtimeTestNow, UpdatedAt: runtimeTestNow}
	task := domain.Task{ID: "consumer", WorkflowID: workflow.ID, Name: "consumer", Class: domain.TaskClassRequired, PromptArtifactID: "prompt", MaxTurns: 1}
	attempt := domain.Attempt{ID: "attempt", WorkflowRunID: run.ID, TaskID: task.ID, Number: 1, Progress: domain.ProgressReady, Control: domain.ControlUnassigned, Revision: 1, AssignmentID: "assignment", UpdatedAt: runtimeTestNow}
	assignment := domain.Assignment{ID: "assignment", AttemptID: attempt.ID, WorkerID: "worker", WorkerEpoch: "worker-epoch", Route: domain.ProviderRoute{WorkerID: "worker", ProviderInstanceID: "codex", Model: "gpt", QuotaPoolID: "quota"}, State: domain.AssignmentOffered, Epoch: 1, LeaseToken: "lease", LeaseExpiresAt: runtimeTestNow.Add(time.Hour), DispatchToken: "dispatch", ThreadID: "thread", CreatedAt: runtimeTestNow, UpdatedAt: runtimeTestNow}
	bodies := map[string][]byte{"carried-a-result": []byte("a-result"), "carried-a-meta": []byte("a-meta"), "carried-b-result": []byte("b-result"), "carried-b-meta": []byte("b-meta")}
	artifacts := []domain.Artifact{qualificationArtifact("prompt", run.ID, task.ID, attempt.ID, "tasks/consumer.md", "objects/prompt", []byte("prompt"))}
	for _, source := range []string{"a", "b"} {
		namespace := "build--" + source
		for _, output := range []string{"result.txt", "meta.json"} {
			id := "carried-" + source + "-" + map[string]string{"result.txt": "result", "meta.json": "meta"}[output]
			original := "source-" + source + "-" + output
			task.CarriedInputs = append(task.CarriedInputs, domain.CarriedInput{Name: output, ArtifactID: id, Producer: "build", ProducerTaskID: "shared-task", ProducerNamespace: namespace, SourceRunID: "run-" + source, SourceAttemptID: "attempt-" + source, SourceArtifactID: original})
			artifacts = append(artifacts, qualificationArtifact(id, run.ID, task.ID, attempt.ID, output, "objects/"+id, bodies[id]))
		}
	}
	assignment.TaskDigest = domain.TaskDigest(task)
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Workflows: []domain.Workflow{workflow}, WorkflowRuns: []domain.WorkflowRun{run}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}, Artifacts: artifacts}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	catalog, err := backlog.NewProjectCatalog([]backlog.ProjectDefinition{{Name: "steward", Repository: "https://example.com/steward.git", DefaultRef: commit, T3ProjectTemplate: "development", SetupProfile: "go"}}, []backlog.SetupProfile{{Name: "go", Commands: []string{"true"}, Timeout: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	builder := backlog.CoordinatorOfferBuilder{Store: store, Catalog: catalog, CatalogRevision: "catalog", CoordinatorID: "coordinator", CoordinatorEpoch: 1, VerificationTimeout: time.Minute, MaxArtifactBytes: 1 << 20, MaxTotalBytes: 2 << 20}
	offer, err := builder.BuildAssignmentOffer(ctx, assignment, runtimeTestNow.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(offer.Package.Package.Dependencies) != 2 {
		t.Fatalf("dependencies=%+v", offer.Package.Package.Dependencies)
	}
	for _, dep := range offer.Package.Package.Dependencies {
		if dep.Provenance == nil || len(dep.Provenance.SourceArtifacts) != 2 {
			t.Fatalf("provenance=%+v", dep)
		}
	}
	source := mapArtifactSource{"prompt": []byte("prompt")}
	for id, body := range bodies {
		source[id] = body
	}
	driver, err := NewLocalDriver(LocalDriver{Config: LocalDriverConfig{CatalogRevision: "catalog", ArtifactRoot: filepath.Join(t.TempDir(), "artifacts"), RunsRoot: filepath.Join(t.TempDir(), "runs")}, Catalog: catalog, Workspace: backlog.WorkspacePreparer{Cache: staticRepositoryCache{path: repository}, Processes: successfulProcessRunner{}}, Finalizer: backlog.AttemptFinalizer{Processes: successfulProcessRunner{}, Now: func() time.Time { return runtimeTestNow }, NewID: func(string) string { return "verification" }}, Source: source, Publisher: &recordingPublisher{}, T3: &recordingT3{}, Now: func() time.Time { return runtimeTestNow }})
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := driver.Prepare(ctx, offer.Package.Package)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutable := range []string{workspace, filepath.Join(workspace, ".t3"), filepath.Join(workspace, ".t3", "dependencies")} {
		_ = os.Chmod(mutable, 0700)
	}
	t.Cleanup(func() {
		_ = filepath.Walk(workspace, func(path string, info os.FileInfo, err error) error {
			if err == nil {
				_ = os.Chmod(path, 0700)
			}
			return nil
		})
	})
	for _, dep := range offer.Package.Package.Dependencies {
		for _, artifact := range dep.Artifacts {
			got, err := os.ReadFile(filepath.Join(workspace, ".t3", filepath.FromSlash(artifact.Path)))
			_ = os.Chmod(filepath.Dir(filepath.Join(workspace, ".t3", filepath.FromSlash(artifact.Path))), 0700)
			if err != nil || string(got) != string(bodies[artifact.ID]) {
				t.Fatalf("%s=%q err=%v", artifact.Path, got, err)
			}
		}
	}
}
