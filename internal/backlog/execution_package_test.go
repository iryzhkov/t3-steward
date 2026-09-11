package backlog

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type packageRecordStore struct {
	records sqlite.CoordinatorRecords
}

func (s packageRecordStore) LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error) {
	return s.records, nil
}

func TestCoordinatorOfferBuilderAssemblesReplayStablePackage(t *testing.T) {
	now := time.Date(2026, 9, 10, 22, 0, 0, 0, time.UTC)
	records, assignment := packageBuilderFixture(now)
	builder := packageBuilder(t, records)

	first, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	second, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.Package, second.Package) {
		t.Fatal("package content changed across replay time")
	}
	pkg := first.Package.Package
	if pkg.CreatedAt != assignment.CreatedAt || pkg.Identity.AssignmentID != assignment.ID ||
		pkg.Identity.ThreadID != assignment.ThreadID || pkg.Environment.CatalogRevision != "catalog-1" {
		t.Fatalf("package identity = %+v", pkg)
	}
	if pkg.Prompt.Path != "prompt/tasks/consumer.md" ||
		len(pkg.StaticInputs) != 1 || pkg.StaticInputs[0].Path != "inputs/context.txt" {
		t.Fatalf("package inputs = prompt:%+v static:%+v", pkg.Prompt, pkg.StaticInputs)
	}
	if len(pkg.Dependencies) != 1 || pkg.Dependencies[0].TaskID != "task-producer" ||
		len(pkg.Dependencies[0].Artifacts) != 1 ||
		pkg.Dependencies[0].Artifacts[0].Path != "dependencies/producer/reports/result.txt" {
		t.Fatalf("package dependencies = %+v", pkg.Dependencies)
	}
	if pkg.Limits.MaxTurns != 4 || pkg.Limits.PrepareTimeout != 5*time.Minute ||
		pkg.Limits.VerificationTimeout != 2*time.Minute {
		t.Fatalf("package limits = %+v", pkg.Limits)
	}
	if err := workerproto.ValidateExecutionPackageManifest(first.Package, 1<<20); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorOfferBuilderFailsClosedOnBrokenDurableLinks(t *testing.T) {
	now := time.Date(2026, 9, 10, 22, 0, 0, 0, time.UTC)
	tests := map[string]func(*sqlite.CoordinatorRecords){
		"missing-prompt": func(records *sqlite.CoordinatorRecords) {
			records.Artifacts = records.Artifacts[1:]
		},
		"wrong-run": func(records *sqlite.CoordinatorRecords) {
			records.Artifacts[0].WorkflowRunID = "run-other"
		},
		"missing-dependency-output": func(records *sqlite.CoordinatorRecords) {
			records.Artifacts = records.Artifacts[:2]
		},
		"detached-attempt": func(records *sqlite.CoordinatorRecords) {
			records.Attempts[0].AssignmentID = ""
		},
		"changed-assignment": func(records *sqlite.CoordinatorRecords) {
			records.Assignments[0].DispatchToken = "dispatch-other"
		},
		"wrong-prompt-owner": func(records *sqlite.CoordinatorRecords) {
			records.Artifacts[0].TaskID = "task-producer"
		},
		"wrong-input-kind": func(records *sqlite.CoordinatorRecords) {
			records.Artifacts[1].Kind = domain.ArtifactOutput
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			records, assignment := packageBuilderFixture(now)
			mutate(&records)
			_, err := packageBuilder(t, records).BuildAssignmentOffer(
				context.Background(), assignment, now.Add(time.Minute),
			)
			if err == nil {
				t.Fatal("broken durable state produced an offer")
			}
		})
	}
}

func packageBuilder(t *testing.T, records sqlite.CoordinatorRecords) CoordinatorOfferBuilder {
	t.Helper()
	catalog, err := NewProjectCatalog(
		[]ProjectDefinition{{
			Name: "steward", Repository: "ssh://git/steward", DefaultRef: "main",
			T3ProjectTemplate: "development", SetupProfile: "go",
			ResourceLocks: []string{"repository"}, RequiredCredentials: []string{"github-token"},
		}},
		[]SetupProfile{{Name: "go", Commands: []string{"go test ./..."}, Timeout: 5 * time.Minute}},
	)
	if err != nil {
		t.Fatal(err)
	}
	return CoordinatorOfferBuilder{
		Store: packageRecordStore{records: records}, Catalog: catalog,
		CatalogRevision: "catalog-1", CoordinatorID: "coordinator", CoordinatorEpoch: 7,
		VerificationTimeout: 2 * time.Minute, MaxArtifactBytes: 1 << 20, MaxTotalBytes: 2 << 20,
	}
}

func packageBuilderFixture(now time.Time) (sqlite.CoordinatorRecords, domain.Assignment) {
	workflow := domain.Workflow{
		ID: "workflow-1", Version: 2, Name: "workflow", Project: "steward",
		Environment: domain.ExecutionEnvironment{Type: EnvironmentGit, Scope: EnvironmentScopeTask},
		Class:       domain.TaskClassRequired, TaskIDs: []string{"task-producer", "task-consumer"}, CreatedAt: now,
	}
	run := domain.WorkflowRun{
		ID: "run-1", WorkflowID: workflow.ID, Progress: domain.ProgressActive,
		Revision: 2, CreatedAt: now, UpdatedAt: now,
	}
	producer := domain.Task{ID: "task-producer", WorkflowID: workflow.ID, Name: "producer", Class: domain.TaskClassRequired}
	consumer := domain.Task{
		ID: "task-consumer", WorkflowID: workflow.ID, Name: "consumer", Class: domain.TaskClassRequired,
		Needs: []string{"producer"}, PromptArtifactID: "prompt-1", InputArtifactIDs: []string{"input-1"},
		DependencyInputs: map[string][]string{"producer": {"reports/result.txt"}},
		Outputs:          []domain.ArtifactDeclaration{{Name: "out.txt", MediaType: "text/plain"}},
		Verification:     []string{"go test ./..."}, MaxTurns: 4,
	}
	attempt := domain.Attempt{
		ID: "attempt-1", WorkflowRunID: run.ID, TaskID: consumer.ID, Number: 1,
		Progress: domain.ProgressReady, Control: domain.ControlUnassigned,
		Revision: 2, AssignmentID: "assignment-1", UpdatedAt: now,
	}
	assignment := domain.Assignment{
		ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "normandy", WorkerEpoch: "worker-1",
		Route: domain.ProviderRoute{
			WorkerID: "normandy", ProviderInstanceID: "codex", Model: "gpt-5.6-sol",
			QuotaPoolID: "openai", Options: map[string]string{"effort": "high"},
		},
		State: domain.AssignmentOffered, Epoch: 1, LeaseToken: "lease-1",
		LeaseExpiresAt: now.Add(time.Hour), DispatchToken: "dispatch-1", ThreadID: "thread-1",
		CreatedAt: now, UpdatedAt: now,
	}
	artifact := func(id, taskID string, kind domain.ArtifactKind, name string) domain.Artifact {
		return domain.Artifact{
			ID: id, WorkflowRunID: run.ID, TaskID: taskID, Kind: kind, Name: name,
			MediaType: "text/plain", Size: 10, SHA256: strings.Repeat("a", 64),
			StoragePath: "objects/" + id, Producer: "coordinator", CreatedAt: now,
		}
	}
	return sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{workflow}, WorkflowRuns: []domain.WorkflowRun{run},
		Tasks: []domain.Task{producer, consumer}, Attempts: []domain.Attempt{attempt},
		Assignments: []domain.Assignment{assignment},
		Artifacts: []domain.Artifact{
			artifact("prompt-1", consumer.ID, domain.ArtifactInput, "tasks/consumer.md"),
			artifact("input-1", "", domain.ArtifactInput, "context.txt"),
			artifact("output-1", producer.ID, domain.ArtifactOutput, "reports/result.txt"),
		},
	}, assignment
}
