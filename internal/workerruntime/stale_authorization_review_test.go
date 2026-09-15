package workerruntime

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestRevokedModelCannotDispatchFromPersistedOldSnapshot(t *testing.T) {
	ctx := context.Background()
	now := runtimeTestNow
	repository, commit := makeGitRepository(t)
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	epoch, err := store.AcquireCoordinator(ctx, "coordinator")
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.BacklogV2.Mode = "coordinator"
	cfg.BacklogV2.Coordinator.ID = "coordinator"
	cfg.BacklogV2.Workers = map[string]config.V2Worker{"normandy": {Address: "qualification.invalid", Epoch: "worker-1", AcceptBacklog: true, Credential: "secretref:f02-protocol/normandy", Providers: map[string]config.V2Provider{"codex": {Models: []string{"gpt"}, QuotaPool: "pool"}}}}
	cfg.BacklogV2.Projects = map[string]config.V2Project{"steward": {Repository: "https://example.com/steward.git", DefaultRef: commit, T3Project: "development", SetupProfile: "go", Workers: []string{"normandy"}}}
	cfg.BacklogV2.SetupProfiles = map[string]config.V2SetupProfile{"go": {Commands: []string{"true"}, Timeout: config.Duration(time.Minute)}}
	before, err := BuildWorkerBinding(cfg.BacklogV2, "normandy", now)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := domain.WorkerSnapshot{WorkerID: "normandy", WorkerEpoch: "worker-1", CoordinatorEpoch: epoch, Sequence: 1, Connected: true, Inventory: before.Inventory, ObservedAt: now, ValidUntil: now.Add(time.Hour)}
	if err := store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	prompt := testArtifact("prompt", "task.md", "prompt")
	cost := 9.0
	records := sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "workflow", Version: 1, Name: "workflow", Project: "steward", Environment: domain.ExecutionEnvironment{Type: "git", Scope: "task"}, Class: domain.TaskClassRequired, TaskIDs: []string{"task"}, CreatedAt: now}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run", WorkflowID: "workflow", Progress: domain.ProgressActive, Revision: 1, CreatedAt: now, UpdatedAt: now}},
		Tasks:        []domain.Task{{ID: "task", WorkflowID: "workflow", Name: "task", Class: domain.TaskClassRequired, PromptArtifactID: "prompt", Routes: []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "gpt", QuotaPoolID: "pool"}}, Importance: 5, Difficulty: 3, EstimatedCost: &cost, MaxTurns: 2}},
		Attempts:     []domain.Attempt{{ID: "attempt", WorkflowRunID: "run", TaskID: "task", Number: 1, Progress: domain.ProgressReady, Control: domain.ControlUnassigned, Revision: 1, UpdatedAt: now}},
		Artifacts:    []domain.Artifact{{ID: "prompt", WorkflowRunID: "run", TaskID: "task", Kind: domain.ArtifactInput, Name: "task.md", MediaType: prompt.MediaType, Size: prompt.Size, SHA256: prompt.SHA256, StoragePath: "fixture", CreatedAt: now}},
	}
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	fleet := config.CoordinatorFleet{CoordinatorID: "coordinator", Workers: map[string]config.CoordinatorFleetWorker{"normandy": {WorkerID: "normandy", CPUClass: "high", ExecutorSlots: 1, ProviderInstances: []string{"codex"}, DesiredModels: map[string][]string{"codex": {}}, QuotaPools: []string{"pool"}}},
		Projects: map[string]config.CoordinatorFleetProject{"steward": {Repository: "https://example.com/steward.git", DefaultRef: commit, SetupProfile: "go", EligibleWorkers: []string{"normandy"}}}}
	if err := cfg.ApplyCoordinatorFleet(fleet); err != nil {
		t.Fatal(err)
	}
	after, err := BuildWorkerBinding(cfg.BacklogV2, "normandy", now)
	if err != nil {
		t.Fatal(err)
	}
	if after.CatalogRevision == before.CatalogRevision || after.Inventory.Providers[0].Available {
		t.Fatal("revocation did not change current catalog")
	}
	snapshots, err := store.LoadWorkerSnapshots(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := AuthorizedPlanningSnapshots(cfg.BacklogV2, snapshots, now); len(got) != 0 {
		t.Fatal("stale persisted snapshot authorized new placement")
	}
	snapshot.Inventory = after.Inventory
	if got := AuthorizedPlanningSnapshots(cfg.BacklogV2, []domain.WorkerSnapshot{snapshot}, now); len(got) != 1 {
		t.Fatal("current catalog snapshot withheld")
	}
	input, err := backlog.BuildCoordinatorPlanInput(backlog.CoordinatorPlanningStateInput{
		Now: now, CoordinatorEpoch: epoch, Workflows: records.Workflows, WorkflowRuns: records.WorkflowRuns, Tasks: records.Tasks, Attempts: records.Attempts, WorkerSnapshots: snapshots,
		QuotaPools:           []domain.QuotaPool{{ID: "pool", Provider: "codex", ProviderInstanceIDs: []string{"codex"}, MaxConcurrent: 2, Admission: domain.AdmissionOpen}},
		QuotaWindows:         []backlog.QuotaWindowBudget{{QuotaPoolID: "pool", WindowID: "primary", ObservedAt: now, Admission: domain.AdmissionOpen, Capacity: 100, ResetsAt: now.Add(time.Hour)}},
		MaxWorkerSnapshotAge: time.Hour, MaxQuotaObservationAge: time.Hour, DeadlineRiskWindow: time.Hour, CheckpointMargin: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	planner := backlog.FleetCoordinator{Store: store}
	report, err := planner.PlanAndCommit(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Assignments) != 1 {
		t.Fatalf("stale-snapshot fixture must plan one assignment, got %d", len(report.Assignments))
	}
	assignment := report.Assignments[0]
	t.Logf("old snapshot planned revoked route %+v", assignment.Route)
	builder := backlog.CoordinatorOfferBuilder{Store: store, Catalog: after.Catalog, CatalogRevision: after.CatalogRevision, CoordinatorID: "coordinator", CoordinatorEpoch: epoch, VerificationTimeout: time.Minute, MaxArtifactBytes: 1 << 20, MaxTotalBytes: 1 << 20}
	// ReconcileWorker grants this lease before packaging an offered assignment.
	assignment.LeaseExpiresAt = now.Add(2 * time.Minute)
	builder.Authorization = &after.Inventory
	if _, err := builder.BuildAssignmentOffer(ctx, assignment, now.Add(time.Minute)); err == nil || !strings.Contains(err.Error(), "no longer authorized") {
		t.Fatalf("current builder must refuse revoked model: %v", err)
	}
	// A package from a previous coordinator still encounters independent worker authorization.
	builder.Authorization = &before.Inventory
	offer, err := builder.BuildAssignmentOffer(ctx, assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("authorized positive-control offer failed: %v", err)
	}
	pkg := offer.Package.Package
	t.Logf("package stamps current catalog=%s on revoked model=%s", pkg.Environment.CatalogRevision, pkg.Route.Model)
	root := t.TempDir()
	control := &recordingT3{}
	driver, err := NewLocalDriver(LocalDriver{Config: LocalDriverConfig{Authorization: &after.Inventory, CatalogRevision: after.CatalogRevision, ArtifactRoot: filepath.Join(root, "artifacts"), RunsRoot: filepath.Join(root, "runs")},
		Catalog: after.Catalog, Workspace: backlog.WorkspacePreparer{Cache: staticRepositoryCache{path: repository}, Processes: successfulProcessRunner{}},
		Finalizer: backlog.AttemptFinalizer{Processes: successfulProcessRunner{}}, Source: mapArtifactSource{"prompt": []byte("prompt")}, Publisher: &recordingPublisher{}, T3: control, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	journal, err := OpenJournal(filepath.Join(root, "journal"), "normandy", "worker-1", epoch)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := New(Config{WorkerID: "normandy", WorkerEpoch: "worker-1", CoordinatorID: "coordinator", CoordinatorEpoch: epoch, SnapshotTTL: time.Minute, LeaseDuration: 2 * time.Minute, MaxPackageBytes: 1 << 20, Inventory: after.Inventory, Now: func() time.Time { return now }}, journal, driver)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := runtime.AcceptOffers(ctx, workerproto.AssignmentOffers{Offers: []workerproto.AssignmentOffer{offer}})
	if err != nil || len(claims.Claims) == 0 {
		t.Fatalf("fixture offer not claimed: %v", err)
	}
	if _, err := driver.Prepare(ctx, pkg); err == nil || !strings.Contains(err.Error(), "no longer authorized") {
		t.Fatalf("prepare must refuse revoked route: %v", err)
	}
	if err := driver.CreateThread(ctx, pkg, ""); err == nil || !strings.Contains(err.Error(), "no longer authorized") {
		t.Fatalf("dispatch must refuse revoked route: %v", err)
	}
	if len(control.created) != 0 {
		t.Fatalf("REVOKED MODEL DISPATCHED: %d T3 create calls for %v", len(control.created), control.created[0].ModelSelection)
	}
}
