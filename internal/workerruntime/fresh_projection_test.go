package workerruntime

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestProjectedFreshProjectBuildsExecutionPackage(t *testing.T) {
	cfg := config.Config{}
	cfg.BacklogV2.Mode = "coordinator"
	cfg.BacklogV2.Coordinator.ID = "coordinator"
	cfg.BacklogV2.QuotaPools = map[string]config.V2QuotaPool{"codex-main": {Provider: "codex", MaxConcurrent: 1}}
	cfg.BacklogV2.Workers = map[string]config.V2Worker{"worker": {
		Address: "worker", Epoch: "worker-epoch", Credential: "secretref:f02-protocol/worker", AcceptBacklog: true,
		CPUClass: config.CPUClassLow, Executors: config.V2Executors{Slots: 1},
		Providers: map[string]config.V2Provider{"codex": {Models: []string{"gpt"}, QuotaPool: "codex-main"}},
	}}
	fleet := config.CoordinatorFleet{Kind: "steward-coordinator-catalog-input", SchemaVersion: 1, CoordinatorID: "coordinator",
		Workers: map[string]config.CoordinatorFleetWorker{"worker": {
			WorkerID: "worker", CPUClass: config.CPUClassLow, ExecutorSlots: 1,
			ProviderInstances: []string{"codex"}, DesiredModels: map[string][]string{"codex": {"gpt"}},
			QuotaPools: []string{"codex-main"},
		}},
		Projects: map[string]config.CoordinatorFleetProject{"scratch": {Type: backlog.EnvironmentFresh, EligibleWorkers: []string{"worker"}}},
	}
	if err := cfg.ApplyCoordinatorFleet(fleet); err != nil {
		t.Fatal(err)
	}
	binding, err := BuildWorkerBinding(cfg.BacklogV2, "worker", runtimeTestNow)
	if err != nil {
		t.Fatal(err)
	}

	workflow := domain.Workflow{ID: "workflow", Version: 2, Name: "fresh", Project: "scratch",
		Environment: domain.ExecutionEnvironment{Type: backlog.EnvironmentFresh, Scope: backlog.EnvironmentScopeTask},
		Class:       domain.TaskClassRequired, TaskIDs: []string{"task"}}
	run := domain.WorkflowRun{ID: "run", WorkflowID: workflow.ID, Progress: domain.ProgressActive, Revision: 1, CreatedAt: runtimeTestNow, UpdatedAt: runtimeTestNow}
	task := domain.Task{ID: "task", WorkflowID: workflow.ID, Name: "task", Class: domain.TaskClassRequired, PromptArtifactID: "prompt", MaxTurns: 1}
	attempt := domain.Attempt{ID: "attempt", WorkflowRunID: run.ID, TaskID: task.ID, Number: 1, Progress: domain.ProgressReady, Control: domain.ControlUnassigned, Revision: 1, AssignmentID: "assignment", UpdatedAt: runtimeTestNow}
	assignment := domain.Assignment{ID: "assignment", AttemptID: attempt.ID, WorkerID: "worker", WorkerEpoch: "worker-epoch",
		Route: domain.ProviderRoute{WorkerID: "worker", ProviderInstanceID: "codex", Model: "gpt", QuotaPoolID: "codex-main"},
		State: domain.AssignmentOffered, Epoch: 1, LeaseToken: "lease", LeaseExpiresAt: runtimeTestNow.Add(time.Hour), DispatchToken: "dispatch", ThreadID: "thread", CreatedAt: runtimeTestNow, UpdatedAt: runtimeTestNow}
	assignment.TaskDigest = domain.TaskDigest(task)
	prompt := qualificationArtifact("prompt", run.ID, task.ID, attempt.ID, "tasks/task.md", "objects/prompt", []byte("prompt"))
	store, err := sqlite.OpenMigrated(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{workflow}, WorkflowRuns: []domain.WorkflowRun{run}, Tasks: []domain.Task{task},
		Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}, Artifacts: []domain.Artifact{prompt},
	}); err != nil {
		t.Fatal(err)
	}
	builder := backlog.CoordinatorOfferBuilder{Store: store, Catalog: binding.Catalog, CatalogRevision: binding.CatalogRevision,
		CoordinatorID: "coordinator", CoordinatorEpoch: 1, VerificationTimeout: time.Minute, MaxArtifactBytes: 1 << 20, MaxTotalBytes: 2 << 20}
	offer, err := builder.BuildAssignmentOffer(context.Background(), assignment, runtimeTestNow.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	environment := offer.Package.Package.Environment
	if environment.Type != backlog.EnvironmentFresh || environment.Repository != "" || environment.Ref != "" ||
		environment.SetupProfile != "steward-fresh-empty" || offer.Package.Package.Limits.PrepareTimeout != time.Minute {
		t.Fatalf("fresh execution package environment = %+v", environment)
	}
}
