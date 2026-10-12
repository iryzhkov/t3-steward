package workerruntime

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// Exercise a tick-scoped offer all the way through LocalDriver's object cache
// and preparation. The project remote never receives the producer's commit.
func TestTerminalSourceOfferTransportsUnpublishedCommit(t *testing.T) {
	ctx := context.Background()
	now := runtimeTestNow
	repository, base := makeGitRepository(t)
	const repositoryURL = "https://example.com/steward.git"
	producerRoot := t.TempDir()
	producerWorkspace := filepath.Join(producerRoot, "workspace")
	gitIn(t, producerRoot, "clone", repository, producerWorkspace)
	gitIn(t, producerWorkspace, "config", "user.name", "Test")
	gitIn(t, producerWorkspace, "config", "user.email", "test@example.com")
	if err := os.WriteFile(filepath.Join(producerWorkspace, "implementation.txt"), []byte("unpublished\n"), 0600); err != nil {
		t.Fatal(err)
	}
	gitIn(t, producerWorkspace, "add", ".")
	gitIn(t, producerWorkspace, "commit", "-m", "implementation")
	head := gitIn(t, producerWorkspace, "rev-parse", "HEAD")
	if gitIn(t, repository, "rev-parse", "HEAD") != base {
		t.Fatal("project remote changed")
	}
	sourceTask := domain.Task{ID: "source", WorkflowID: "workflow-source", Name: "implement",
		Class: domain.TaskClassRequired, Outputs: []domain.ArtifactDeclaration{{Name: "implementation", Commit: &domain.CommitOutput{}}}}
	sourceAttempt := domain.Attempt{ID: "attempt-source", WorkflowRunID: "run-source", TaskID: sourceTask.ID,
		Number: 2, Revision: 1, Progress: domain.ProgressSucceeded, Control: domain.ControlStopped,
		AssignmentID: "assignment-source", UpdatedAt: now}
	storage := t.TempDir()
	producerRefs := backlog.CampaignRefStore{Root: filepath.Join(producerRoot, "refs")}
	finalizer := backlog.AttemptFinalizer{StorageRoot: storage, CampaignRefs: producerRefs, Processes: successfulProcessRunner{}}
	finalized, err := finalizer.Finalize(ctx, backlog.AttemptFinalization{Task: sourceTask, Attempt: sourceAttempt,
		WorkspaceDir: producerWorkspace, ExplicitSuccess: true, Repository: repositoryURL, BaseCommit: base, CommitBundles: true})
	if err != nil || !finalized.Completion.VerificationPassed {
		t.Fatalf("producer: %+v, %v", finalized.Completion, err)
	}
	t.Cleanup(func() { _ = removeReadOnlyTree(finalized.StorageDir) })
	var provenance, bundle domain.Artifact
	bytes := mapArtifactSource{}
	for _, artifact := range finalized.Artifacts {
		raw, err := os.ReadFile(filepath.Join(storage, artifact.StoragePath))
		if err != nil {
			t.Fatal(err)
		}
		bytes[artifact.ID] = raw
		if artifact.Kind == domain.ArtifactOutput {
			provenance = artifact
		}
		if artifact.Kind == domain.ArtifactGitState {
			bundle = artifact
		}
	}
	if provenance.ID == "" || bundle.ID == "" {
		t.Fatal("producer retained no record or bundle")
	}
	pkg := testPackage()
	catalog, err := backlog.NewProjectCatalog(
		[]backlog.ProjectDefinition{{Name: "steward", Repository: repositoryURL, DefaultRef: base, T3ProjectTemplate: "development", SetupProfile: "go"}},
		[]backlog.SetupProfile{{Name: "go", Commands: []string{"true"}, Timeout: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	task := domain.Task{ID: pkg.Identity.TaskID, RunID: pkg.Identity.WorkflowRunID, WorkflowID: pkg.Identity.WorkflowID,
		Name: "consumer", Class: domain.TaskClassRequired, MaxTurns: 4, PromptArtifactID: pkg.Prompt.ID,
		CarriedInputs: []domain.CarriedInput{{Producer: sourceTask.Name, ProducerNamespace: "external-implement", ProducerTaskID: sourceTask.ID,
			SourceRunID: sourceAttempt.WorkflowRunID, SourceAttemptID: sourceAttempt.ID, SourceArtifactID: provenance.ID,
			Name: provenance.Name, ArtifactID: "carried-record"}}}
	carried := provenance
	carried.ID, carried.WorkflowRunID, carried.TaskID, carried.AttemptID, carried.Kind = "carried-record", task.RunID, task.ID, "", domain.ArtifactInput
	bytes[carried.ID] = bytes[provenance.ID]
	prompt := domain.Artifact{ID: pkg.Prompt.ID, WorkflowRunID: task.RunID, TaskID: task.ID, Kind: domain.ArtifactInput,
		Name: "consumer.md", MediaType: pkg.Prompt.MediaType, Size: pkg.Prompt.Size, SHA256: pkg.Prompt.SHA256, StoragePath: "prompt", CreatedAt: now}
	bytes[prompt.ID] = []byte("prompt")
	assignment := domain.Assignment{ID: pkg.Identity.AssignmentID, AttemptID: pkg.Identity.AttemptID,
		WorkerID: pkg.WorkerID, WorkerEpoch: pkg.WorkerEpoch, Route: pkg.Route, State: domain.AssignmentOffered, Epoch: 1,
		DispatchToken: pkg.Identity.DispatchToken, ThreadID: pkg.Identity.ThreadID, LeaseToken: "lease",
		CreatedAt: now, UpdatedAt: now, LeaseExpiresAt: now.Add(time.Hour)}
	sourceAssignment := assignment
	sourceAssignment.ID, sourceAssignment.AttemptID, sourceAssignment.WorkerID = sourceAttempt.AssignmentID, sourceAttempt.ID, "producer-worker"
	sourceAssignment.DispatchToken, sourceAssignment.ThreadID, sourceAssignment.LeaseToken = "source-dispatch", "source-thread", "source-lease"
	records := sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{ID: task.WorkflowID, Version: 2, Name: "consumer", Project: "steward",
			Environment: domain.ExecutionEnvironment{Type: backlog.EnvironmentGit, Scope: backlog.EnvironmentScopeTask}, TaskIDs: []string{task.ID}, CreatedAt: now},
			{ID: sourceTask.WorkflowID, Version: 2, Name: "source", TaskIDs: []string{sourceTask.ID}, CreatedAt: now}},
		WorkflowRuns: []domain.WorkflowRun{{ID: task.RunID, WorkflowID: task.WorkflowID, Progress: domain.ProgressActive, Revision: 1, CreatedAt: now, UpdatedAt: now},
			{ID: sourceAttempt.WorkflowRunID, WorkflowID: sourceTask.WorkflowID, Progress: domain.ProgressSucceeded,
				Sink: &domain.SinkTask{Progress: domain.ProgressSucceeded}, Revision: 1, CreatedAt: now, UpdatedAt: now}},
		Tasks: []domain.Task{task, sourceTask},
		Attempts: []domain.Attempt{sourceAttempt, {ID: assignment.AttemptID, WorkflowRunID: task.RunID, TaskID: task.ID,
			Number: 1, Revision: 1, Progress: domain.ProgressReady, Control: domain.ControlUnassigned, AssignmentID: assignment.ID, UpdatedAt: now}},
		Assignments: []domain.Assignment{assignment, sourceAssignment}, Artifacts: []domain.Artifact{prompt, carried, provenance, bundle},
	}
	for _, attempt := range []string{"attempt-older", "attempt-newer"} {
		decoy := bundle
		decoy.ID, decoy.AttemptID = attempt+"-bundle", attempt
		records.Artifacts = append(records.Artifacts, decoy)
	}
	store, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "coordinator.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	ctx, scope := sqlite.WithCoordinatorRecordsSnapshot(ctx)
	defer scope.Finish()
	before, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(before.Artifacts, func(a domain.Artifact) bool { return a.ID == bundle.ID }) {
		t.Fatal("source bundle already in snapshot")
	}
	builder := backlog.CoordinatorOfferBuilder{Store: store, Catalog: catalog, CatalogRevision: "catalog-1",
		CoordinatorID: "coordinator", CoordinatorEpoch: 1, VerificationTimeout: time.Minute, MaxArtifactBytes: 1 << 20, MaxTotalBytes: 2 << 20,
		WorkerCapabilities: map[string][]string{pkg.WorkerID: {workerproto.PackageCapabilityCommitBundle, workerproto.PackageCapabilityCommitOutputs}}}
	offer, err := builder.BuildAssignmentOffer(ctx, assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	pkg = offer.Package.Package
	if len(pkg.CommitBundles) != 1 || pkg.CommitBundles[0].Bundle == nil || pkg.CommitBundles[0].Bundle.ID != bundle.ID {
		t.Fatalf("delivery = %+v", pkg.CommitBundles)
	}
	consumerRoot := t.TempDir()
	consumerRefs := backlog.CampaignRefStore{Root: filepath.Join(consumerRoot, "refs")}
	driver, err := NewLocalDriver(LocalDriver{
		Config:  LocalDriverConfig{CatalogRevision: "catalog-1", ArtifactRoot: filepath.Join(consumerRoot, "artifacts"), RunsRoot: filepath.Join(consumerRoot, "runs")},
		Catalog: catalog, Source: bytes, Publisher: &recordingPublisher{}, T3: &recordingT3{},
		Workspace: backlog.WorkspacePreparer{Cache: staticRepositoryCache{path: repository}, Processes: successfulProcessRunner{}, CampaignRefs: consumerRefs},
		Finalizer: backlog.AttemptFinalizer{Processes: successfulProcessRunner{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := driver.Prepare(ctx, pkg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Cleanup(context.Background(), pkg, workspace) })
	ref := backlog.CampaignRef(sourceAttempt.WorkflowRunID, sourceTask.ID, provenance.Name)
	if got := gitIn(t, workspace, "rev-parse", ref+"^{commit}"); got != head {
		t.Fatalf("resolved %s, want %s", got, head)
	}
	if resolved, err := consumerRefs.Resolve(sourceAttempt.WorkflowRunID, sourceTask.ID, provenance.Name); err != nil || resolved.Commit != head {
		t.Fatalf("import = %+v, %v", resolved, err)
	}
	if gitIn(t, repository, "rev-parse", "HEAD") != base {
		t.Fatal("consumer published the commit to the remote")
	}
}
