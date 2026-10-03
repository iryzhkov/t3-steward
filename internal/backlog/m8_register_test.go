package backlog

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestM8RegisterOnlyCreatesDefinitionWithoutRun(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	storage := filepath.Join(t.TempDir(), "storage")
	t.Cleanup(func() { _ = removeIngestedTree(storage) })
	service := &SubmissionService{StorageRoot: storage, Store: store, MaxBytes: 1 << 20, MaxFiles: 100}
	request := DirectorySubmission{IdempotencyKey: "register-example", BundleDir: validBundle(t)}
	// The baseline ignores this new option, creating a dispatchable run.
	if err := json.Unmarshal([]byte(`{"RegisterOnly":true}`), &request); err != nil {
		t.Fatal(err)
	}
	result, err := service.SubmitDirectory(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.WorkflowRuns) != 0 || len(records.Attempts) != 0 || result.Record.RunID != "" {
		t.Fatalf("registration started work: %d runs, %d attempts, run=%q", len(records.WorkflowRuns), len(records.Attempts), result.Record.RunID)
	}
	if len(records.Workflows) != 1 || len(records.Tasks) == 0 || len(records.Artifacts) == 0 {
		t.Fatalf("definition not retained: %+v", records)
	}
	for _, task := range records.Tasks {
		if task.RunID != "" {
			t.Fatalf("task bound to nonexistent run: %+v", task)
		}
	}
	replay, err := service.SubmitDirectory(ctx, request)
	if err != nil || !replay.Replay || replay.Record.WorkflowID != result.Record.WorkflowID {
		t.Fatalf("replay = %+v, %v", replay, err)
	}
	ordinary := DirectorySubmission{IdempotencyKey: request.IdempotencyKey, BundleDir: request.BundleDir}
	if _, err := service.SubmitDirectory(ctx, ordinary); !errors.Is(err, domain.ErrSubmissionConflict) {
		t.Fatalf("mode change was not refused: %v", err)
	}
	// The registered definition is usable by the existing schedule path.
	now := time.Now().UTC()
	_, err = (&ScheduleDefinitionService{Store: store}).Put(ctx, ScheduleDefinitionRequest{
		RequestID: "schedule-example", ID: "nightly", Name: "Nightly", WorkflowID: result.Record.WorkflowID,
		Expression: "0 2 * * *", Timezone: "UTC", Reason: "test", Actor: "operator", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	fired, err := store.CommitScheduleTrigger(ctx, domain.ScheduleTriggerRequest{
		ScheduleID: "nightly", TriggerID: "trigger-test", WorkflowRunID: "scheduled-test",
		NominalAt: now, ObservedAt: now, Source: domain.ScheduleTriggerManual,
	})
	if err != nil {
		t.Fatal(err)
	}
	if fired.WorkflowRun == nil {
		t.Fatalf("trigger created no run: %+v", fired)
	}
	records, err = store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.WorkflowRuns) != 1 || len(records.Attempts) != len(records.Tasks) {
		t.Fatalf("scheduled definition cannot execute: %d runs, %d attempts, %d tasks", len(records.WorkflowRuns), len(records.Attempts), len(records.Tasks))
	}
	// Build an actual worker offer: creating the run alone does not prove dispatch.
	_, assignment := packageBuilderFixture(now)
	assignment.AttemptID = records.Attempts[0].ID
	records.Attempts[0].AssignmentID = assignment.ID
	records.Assignments = []domain.Assignment{assignment}
	builder := packageBuilder(t, records)
	catalog, err := NewProjectCatalog(
		[]ProjectDefinition{{Name: "t3-steward", Repository: "ssh://git/steward", DefaultRef: "main", T3ProjectTemplate: "development", SetupProfile: "go"}},
		[]SetupProfile{{Name: "go", Commands: []string{"go test ./..."}, Timeout: 5 * time.Minute}},
	)
	if err != nil {
		t.Fatal(err)
	}
	builder.Catalog = catalog
	offer, err := builder.BuildAssignmentOffer(ctx, assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("registered schedule cannot dispatch: %v", err)
	}
	if offer.Package.Package.Prompt.Path != "prompt/prompts/inspect.md" {
		t.Fatalf("prompt = %+v", offer.Package.Package.Prompt)
	}

	writeBundleFile(t, request.BundleDir, "prompts/inspect.md", "changed")
	if _, err := service.SubmitDirectory(ctx, request); !errors.Is(err, domain.ErrSubmissionConflict) {
		t.Fatalf("changed content accepted: %v", err)
	}
}

func TestM8RegisterOnlyRefusesSupervision(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	root := validBundle(t)
	writeBundleFile(t, root, "workflow.yaml", supervisedIngestionManifest)
	writeBundleFile(t, root, "prompts/implement.md", "implement")
	writeBundleFile(t, root, "prompts/overseer.md", "supervise")
	service := &SubmissionService{StorageRoot: t.TempDir(), Store: store, MaxBytes: 1 << 20, MaxFiles: 100}
	request := DirectorySubmission{IdempotencyKey: "supervised-register", BundleDir: root}
	_ = json.Unmarshal([]byte(`{"RegisterOnly":true}`), &request)
	_, err = service.SubmitDirectory(ctx, request)
	if err == nil || !strings.Contains(err.Error(), "supervised") || !strings.Contains(err.Error(), "t3-steward campaign help supervision") {
		t.Fatalf("refusal = %v", err)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.WorkflowRuns) != 0 || len(records.Workflows) != 0 {
		t.Fatal("refusal left records")
	}
}
