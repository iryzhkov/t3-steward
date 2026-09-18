package backlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestSubmissionServicePreservesLegacySingleTaskCompatibility(t *testing.T) {
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	storage := filepath.Join(t.TempDir(), "storage")
	t.Cleanup(func() { _ = removeIngestedTree(storage) })
	service := &SubmissionService{
		StorageRoot: storage, Store: store, MaxBytes: 1 << 20, MaxFiles: 16,
	}
	gated := false
	task := Task{
		Project: "t3-steward development", Host: "normandy", Title: "Continue",
		Importance: 5, Difficulty: 4, Model: "gpt-5.6-sol", Instance: "codex",
		Options: map[string]string{"effort": "high"}, MaxTurns: 12,
		Gate: &gated, Prompt: "Continue the production-binding stage.",
	}
	request := SingleTaskSubmission{IdempotencyKey: "legacy-request", Task: task}
	first, err := service.SubmitSingleTask(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := service.SubmitSingleTask(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Replay || replay.Record.WorkflowID != first.Record.WorkflowID {
		t.Fatalf("legacy replay = %#v, first = %#v", replay, first)
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Workflows) != 1 || len(records.Tasks) != 1 || len(records.Attempts) != 1 {
		t.Fatalf("legacy records = %#v", records)
	}
	workflow, submitted := records.Workflows[0], records.Tasks[0]
	if workflow.Class != domain.TaskClassRequired || workflow.Project != task.Project ||
		submitted.Importance != task.Importance || submitted.Difficulty != task.Difficulty ||
		submitted.MaxTurns != task.MaxTurns || len(submitted.Routes) != 1 ||
		submitted.Routes[0].WorkerID != task.Host ||
		submitted.Routes[0].ProviderInstanceID != task.Instance ||
		submitted.Routes[0].Model != task.Model ||
		submitted.Routes[0].Options["effort"] != "high" ||
		len(submitted.Verification) != 1 || submitted.Verification[0] != "true" {
		t.Fatalf("legacy workflow/task = %#v %#v", workflow, submitted)
	}
	var promptPath string
	for _, artifact := range records.Artifacts {
		if artifact.ID == submitted.PromptArtifactID {
			promptPath = filepath.Join(storage, filepath.FromSlash(artifact.StoragePath))
		}
	}
	raw, err := os.ReadFile(promptPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != task.Prompt {
		t.Fatalf("prompt = %q", raw)
	}
}

// TestSubmissionServiceRefusesALegacyTaskWithoutARoute is the legacy side of
// the no-route rule: a task file that names no instance and model is refused
// at intake, permanently and as a content conflict, so the legacy source
// quarantines it once rather than creating a run the planner cannot place.
func TestSubmissionServiceRefusesALegacyTaskWithoutARoute(t *testing.T) {
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	storage := filepath.Join(t.TempDir(), "storage")
	t.Cleanup(func() { _ = removeIngestedTree(storage) })
	service := &SubmissionService{
		StorageRoot: storage, Store: store, MaxBytes: 1 << 20, MaxFiles: 16,
	}
	for name, task := range map[string]Task{
		"neither":       {Project: "steward", Title: "Continue", Prompt: "do the work"},
		"model only":    {Project: "steward", Title: "Continue", Prompt: "do the work", Model: "claude-haiku-4-5"},
		"instance only": {Project: "steward", Title: "Continue", Prompt: "do the work", Instance: "claudeAgent"},
	} {
		_, err := service.SubmitSingleTask(context.Background(), SingleTaskSubmission{IdempotencyKey: "legacy-" + name, Task: task})
		if err == nil {
			t.Fatalf("%s: a route-less legacy task was accepted", name)
		}
		if !errors.Is(err, ErrPermanentIntake) || !errors.Is(err, domain.ErrSubmissionConflict) {
			t.Fatalf("%s: the refusal is not a permanent content conflict: %v", name, err)
		}
		if !strings.Contains(err.Error(), "no-route") {
			t.Fatalf("%s: the refusal does not name no-route: %v", name, err)
		}
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Workflows) != 0 {
		t.Fatalf("a refused legacy task created %d workflow(s)", len(records.Workflows))
	}
}
