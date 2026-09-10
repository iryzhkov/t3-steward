package backlog

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type ingestionStore struct {
	records sqlite.CoordinatorRecords
	err     error
	calls   int
}

func (s *ingestionStore) SaveCoordinatorRecords(_ context.Context, records sqlite.CoordinatorRecords) error {
	s.calls++
	s.records = records
	return s.err
}

func TestBundleIngesterCopiesImmutableFilesAndPersistsRecords(t *testing.T) {
	bundle := validBundle(t)
	writeBundleFile(t, bundle, "prompts/implement.md", "implement prompt")
	writeBundleFile(t, bundle, "inputs/plan.md", "plan")
	writeBundleFile(t, bundle, "inputs/data.json", "{\"ok\":true}")
	rewriteBundleManifest(t, bundle, `
version: 2
name: bundle
class: required
environment: {project: t3-steward, ref: feature/backlog-orchestrator}
inputs: [inputs/*.md, inputs/*.json]
routes:
  - {host: normandy, instance: codex, model: gpt-5.6-sol, quota_pool: openai}
tasks:
  inspect:
    prompt_file: prompts/inspect.md
    outputs: [findings.md]
  implement:
    prompt_file: prompts/implement.md
    needs: [inspect]
    inputs_from: {inspect: [findings.md]}
    verify: [go test ./...]
`)

	stateDir := t.TempDir()
	store, err := sqlite.OpenMigrated(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 9, 10, 10, 30, 0, 0, time.FixedZone("PDT", -7*60*60))
	next := 0
	ingester := BundleIngester{
		StorageRoot: filepath.Join(t.TempDir(), "artifacts"),
		Store:       store,
		Now:         func() time.Time { return now },
		NewID: func() string {
			next++
			return fmt.Sprintf("%02d", next)
		},
	}
	t.Cleanup(func() { _ = removeIngestedTree(ingester.StorageRoot) })
	got, err := ingester.Ingest(context.Background(), bundle)
	if err != nil {
		t.Fatalf("ingest bundle: %v", err)
	}
	if got.WorkflowID != "workflow-01" || got.RunID != "run-02" {
		t.Fatalf("submission IDs = %q, %q", got.WorkflowID, got.RunID)
	}

	loaded, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatalf("load records: %v", err)
	}
	if !reflect.DeepEqual(loaded, got.Records) {
		t.Fatalf("persisted records differ:\n got: %#v\nwant: %#v", loaded, got.Records)
	}
	if len(loaded.Workflows) != 1 || len(loaded.WorkflowRuns) != 1 || len(loaded.Tasks) != 2 || len(loaded.Attempts) != 2 || len(loaded.Artifacts) != 5 {
		t.Fatalf("unexpected record counts: %#v", loaded)
	}
	workflow := loaded.Workflows[0]
	if workflow.Environment.Type != EnvironmentGit || workflow.Environment.Scope != EnvironmentScopeTask ||
		workflow.Environment.Ref != "feature/backlog-orchestrator" {
		t.Fatalf("workflow environment = %#v", workflow.Environment)
	}
	if !workflow.CreatedAt.Equal(now.UTC()) || loaded.WorkflowRuns[0].Progress != domain.ProgressQueued {
		t.Fatalf("workflow timestamps or state not normalized: %#v %#v", workflow, loaded.WorkflowRuns[0])
	}

	tasks := map[string]domain.Task{}
	for _, task := range loaded.Tasks {
		tasks[task.Name] = task
	}
	if tasks["inspect"].PromptArtifactID == "" || tasks["implement"].PromptArtifactID == "" {
		t.Fatalf("prompt artifacts not assigned: %#v", tasks)
	}
	if len(tasks["implement"].InputArtifactIDs) != 2 ||
		!reflect.DeepEqual(tasks["implement"].Needs, []string{"inspect"}) ||
		!reflect.DeepEqual(tasks["implement"].DependencyInputs, map[string][]string{"inspect": {"findings.md"}}) {
		t.Fatalf("implement task conversion = %#v", tasks["implement"])
	}
	attemptState := map[string]domain.ProgressState{}
	for _, attempt := range loaded.Attempts {
		for _, task := range loaded.Tasks {
			if task.ID == attempt.TaskID {
				attemptState[task.Name] = attempt.Progress
			}
		}
		if attempt.Control != domain.ControlUnassigned || attempt.Number != 1 {
			t.Fatalf("initial attempt = %#v", attempt)
		}
	}
	if attemptState["inspect"] != domain.ProgressReady || attemptState["implement"] != domain.ProgressBlocked {
		t.Fatalf("initial attempt states = %#v", attemptState)
	}

	for _, artifact := range loaded.Artifacts {
		path := filepath.Join(ingester.StorageRoot, filepath.FromSlash(artifact.StoragePath))
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read copied artifact %q: %v", artifact.Name, err)
		}
		sum := fmt.Sprintf("%x", sha256.Sum256(raw))
		if artifact.Size != int64(len(raw)) || artifact.SHA256 != sum {
			t.Errorf("metadata for %q = size %d sha %q, want %d %q", artifact.Name, artifact.Size, artifact.SHA256, len(raw), sum)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o444 {
			t.Errorf("artifact %q mode = %o, want 444", artifact.Name, info.Mode().Perm())
		}
	}
	info, err := os.Stat(got.StorageDir)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o555 {
		t.Fatalf("bundle directory mode = %o, want 555", info.Mode().Perm())
	}
}

func TestBundleIngesterRollsBackInvalidBundle(t *testing.T) {
	bundle := validBundle(t)
	if err := os.Remove(filepath.Join(bundle, "prompts", "inspect.md")); err != nil {
		t.Fatal(err)
	}
	store := &ingestionStore{}
	storage := filepath.Join(t.TempDir(), "storage")
	_, err := (BundleIngester{StorageRoot: storage, Store: store}).Ingest(context.Background(), bundle)
	if err == nil || !strings.Contains(err.Error(), "prompt_file") {
		t.Fatalf("invalid bundle error = %v", err)
	}
	if store.calls != 0 {
		t.Fatalf("store called %d times for invalid bundle", store.calls)
	}
	if _, statErr := os.Stat(storage); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("storage created for invalid bundle: %v", statErr)
	}
}

func TestBundleIngesterRollsBackFilesOnPersistenceFailure(t *testing.T) {
	bundle := validBundle(t)
	store := &ingestionStore{err: errors.New("database unavailable")}
	storage := filepath.Join(t.TempDir(), "storage")
	_, err := (BundleIngester{
		StorageRoot: storage,
		Store:       store,
		NewID:       func() string { return "fixed" },
	}).Ingest(context.Background(), bundle)
	if err == nil || !strings.Contains(err.Error(), "persist metadata: database unavailable") {
		t.Fatalf("persistence error = %v", err)
	}
	if store.calls != 1 || len(store.records.Workflows) != 1 {
		t.Fatalf("store calls or records = %d, %#v", store.calls, store.records)
	}
	entries, readErr := os.ReadDir(filepath.Join(storage, "workflows"))
	if readErr != nil {
		t.Fatalf("read storage after rollback: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("files survived persistence rollback: %#v", entries)
	}
}
