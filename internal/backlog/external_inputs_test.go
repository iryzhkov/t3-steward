package backlog

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type externalInputStore struct{ records sqlite.CoordinatorRecords }

func (s *externalInputStore) SaveCoordinatorRecords(context.Context, sqlite.CoordinatorRecords) error {
	return nil
}
func (s *externalInputStore) LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error) {
	return s.records, nil
}

func externalInputFixture(progress domain.ProgressState) (*externalInputStore, Manifest, sqlite.CoordinatorRecords) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	producer := domain.Task{
		ID: "producer-id", WorkflowID: "source-workflow", Name: "producer",
		Outputs: []domain.ArtifactDeclaration{{Name: "report.txt", MediaType: "text/plain"}},
	}
	run := domain.WorkflowRun{
		ID: "source-run", WorkflowID: "source-workflow", Progress: domain.ProgressSucceeded,
		Revision: 4, CreatedAt: now, UpdatedAt: now,
	}
	attempt := domain.Attempt{
		ID: "producer-attempt", WorkflowRunID: run.ID, TaskID: producer.ID,
		Number: 1, Revision: 2, Progress: progress, Control: domain.ControlStopped,
	}
	source := &externalInputStore{records: sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{run}, Tasks: []domain.Task{producer},
		Attempts: []domain.Attempt{attempt},
		Artifacts: []domain.Artifact{{
			ID: "source-output", WorkflowRunID: run.ID, TaskID: producer.ID,
			AttemptID: attempt.ID, Kind: domain.ArtifactOutput, Name: "report.txt",
			MediaType: "text/plain", Size: 7, SHA256: "abc", StoragePath: "source/report.txt",
			Producer: "worker:source", CreatedAt: now,
		}},
	}}
	manifest := Manifest{Tasks: map[string]ManifestTask{
		"consumer": {
			Needs:      []string{"source-run/producer"},
			InputsFrom: map[string][]string{"source-run/producer": {"report.txt"}},
		},
	}}
	target := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: "target-run", WorkflowID: "target-workflow"}},
		Tasks: []domain.Task{{
			ID: "consumer-id", WorkflowID: "target-workflow", Name: "consumer",
			ExternalNeeds:    []domain.NodeRef{{RunID: "source-run", TaskID: "producer"}},
			DependencyInputs: map[string][]string{"source-run/producer": {"report.txt"}},
		}},
	}
	return source, manifest, target
}

func TestRetainExternalInputsPinsExactSuccessfulArtifact(t *testing.T) {
	store, manifest, target := externalInputFixture(domain.ProgressSucceeded)
	ingester := BundleIngester{Store: store, NewTypedID: func(string) string { return "retained-input" }}
	if err := ingester.retainExternalInputs(context.Background(), manifest, &target); err != nil {
		t.Fatal(err)
	}
	task := target.Tasks[0]
	if len(task.CarriedInputs) != 1 || task.CarriedInputs[0].ArtifactID != "retained-input" ||
		task.CarriedInputs[0].Producer != "producer" || task.CarriedInputs[0].Name != "report.txt" ||
		task.CarriedInputs[0].ProducerNamespace == "" ||
		task.CarriedInputs[0].SourceRunID != "source-run" ||
		task.CarriedInputs[0].SourceAttemptID != "producer-attempt" ||
		task.CarriedInputs[0].SourceArtifactID != "source-output" {
		t.Fatalf("carried inputs = %+v", task.CarriedInputs)
	}
	if len(task.DependencyInputs) != 0 {
		t.Fatalf("external dependency remained local: %+v", task.DependencyInputs)
	}
	if len(target.Artifacts) != 1 {
		t.Fatalf("retained artifacts = %d", len(target.Artifacts))
	}
	retained := target.Artifacts[0]
	if retained.WorkflowRunID != "target-run" || retained.TaskID != "consumer-id" ||
		retained.AttemptID != "" || retained.Kind != domain.ArtifactInput ||
		retained.SHA256 != "abc" || retained.StoragePath != "source/report.txt" {
		t.Fatalf("retained custody = %+v", retained)
	}
	dependencies, err := packageDependencies(task, target.Tasks,
		map[string]domain.Artifact{retained.ID: retained}, "target-run")
	if err != nil {
		t.Fatal(err)
	}
	if len(dependencies) != 1 || len(dependencies[0].Artifacts) != 1 ||
		dependencies[0].Artifacts[0].SHA256 != "abc" ||
		dependencies[0].Provenance == nil ||
		dependencies[0].Provenance.RunID != "source-run" ||
		dependencies[0].Provenance.AttemptID != "producer-attempt" ||
		dependencies[0].Provenance.ArtifactID != "source-output" {
		t.Fatalf("packaged dependency = %+v", dependencies)
	}
}

func TestRetainExternalCampaignCommitKeepsExactProvenance(t *testing.T) {
	store, manifest, target := externalInputFixture(domain.ProgressSucceeded)
	store.records.Tasks[0].Outputs = []domain.ArtifactDeclaration{{
		Name: "implementation", MediaType: "application/json",
		Commit: &domain.CommitOutput{Revision: "HEAD"},
	}}
	source := &store.records.Artifacts[0]
	source.Name = "implementation"
	source.MediaType = "application/json"
	source.SHA256 = "commit-record-digest"
	source.StoragePath = "objects/commit-record"
	manifest.Tasks["consumer"].InputsFrom["source-run/producer"] = []string{"implementation"}
	target.Tasks[0].DependencyInputs["source-run/producer"] = []string{"implementation"}
	ingester := BundleIngester{Store: store, NewTypedID: func(string) string { return "retained-commit" }}
	if err := ingester.retainExternalInputs(context.Background(), manifest, &target); err != nil {
		t.Fatal(err)
	}
	dependencies, err := packageDependencies(target.Tasks[0], target.Tasks, mapArtifacts(target.Artifacts), "target-run")
	if err != nil {
		t.Fatal(err)
	}
	if len(dependencies) != 1 || dependencies[0].Provenance == nil ||
		dependencies[0].Provenance.ArtifactID != "source-output" ||
		dependencies[0].Artifacts[0].SHA256 != "commit-record-digest" ||
		dependencies[0].Artifacts[0].MediaType != "application/json" {
		t.Fatalf("packaged campaign commit = %+v", dependencies)
	}
}

func TestRetainExternalInputsRefusesFailedOrMismatchedProducer(t *testing.T) {
	store, manifest, target := externalInputFixture(domain.ProgressFailed)
	ingester := BundleIngester{Store: store, NewTypedID: func(string) string { return "unused" }}
	if err := ingester.retainExternalInputs(context.Background(), manifest, &target); err == nil {
		t.Fatal("failed producer was accepted")
	}
	store, manifest, target = externalInputFixture(domain.ProgressSucceeded)
	ingester.Store = store
	ingester.StorageRoot = t.TempDir()
	if err := ingester.retainExternalInputs(context.Background(), manifest, &target); err == nil {
		t.Fatal("missing retained artifact content was accepted")
	}
	store, manifest, target = externalInputFixture(domain.ProgressSucceeded)
	ingester.StorageRoot = ""
	manifest.Tasks["consumer"].InputsFrom["source-run/producer"] = []string{"missing.txt"}
	ingester.Store = store
	if err := ingester.retainExternalInputs(context.Background(), manifest, &target); err == nil {
		t.Fatal("undeclared external output was accepted")
	}
}

func TestExternalInputsKeepDistinctNamespacesAndProvenanceAcrossRestart(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	source := &externalInputStore{}
	manifest := Manifest{Tasks: map[string]ManifestTask{"consumer": {
		InputsFrom: map[string][]string{
			"run-a/build": {"result.txt"},
			"run-b/build": {"result.txt"},
		},
	}}}
	target := sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: "target-run", WorkflowID: "target-workflow"}},
		Tasks:        []domain.Task{{ID: "consumer-id", WorkflowID: "target-workflow", Name: "consumer"}},
	}
	for _, suffix := range []string{"a", "b"} {
		runID, taskID, attemptID, artifactID := "run-"+suffix, "task-"+suffix, "attempt-"+suffix, "artifact-"+suffix
		source.records.WorkflowRuns = append(source.records.WorkflowRuns, domain.WorkflowRun{
			ID: runID, WorkflowID: "workflow-" + suffix, Progress: domain.ProgressSucceeded,
		})
		source.records.Tasks = append(source.records.Tasks, domain.Task{
			ID: taskID, WorkflowID: "workflow-" + suffix, Name: "build",
			Outputs: []domain.ArtifactDeclaration{{Name: "result.txt", MediaType: "text/plain"}},
		})
		source.records.Attempts = append(source.records.Attempts, domain.Attempt{
			ID: attemptID, WorkflowRunID: runID, TaskID: taskID, Number: 1,
			Revision: 1, Progress: domain.ProgressSucceeded, Control: domain.ControlStopped,
		})
		source.records.Artifacts = append(source.records.Artifacts, domain.Artifact{
			ID: artifactID, WorkflowRunID: runID, TaskID: taskID, AttemptID: attemptID,
			Kind: domain.ArtifactOutput, Name: "result.txt", MediaType: "text/plain",
			Size: 1, SHA256: "digest-" + suffix, StoragePath: "objects/" + suffix,
			CreatedAt: now,
		})
	}
	next := 0
	ingester := BundleIngester{Store: source, NewTypedID: func(string) string {
		next++
		return fmt.Sprintf("input-%d", next)
	}}
	if err := ingester.retainExternalInputs(context.Background(), manifest, &target); err != nil {
		t.Fatal(err)
	}
	dependencies, err := packageDependencies(target.Tasks[0], target.Tasks, mapArtifacts(target.Artifacts), "target-run")
	if err != nil {
		t.Fatal(err)
	}
	if len(dependencies) != 2 || dependencies[0].Artifacts[0].Path == dependencies[1].Artifacts[0].Path {
		t.Fatalf("external package paths collide: %+v", dependencies)
	}
	for _, dependency := range dependencies {
		if dependency.Provenance == nil || dependency.Provenance.RunID == "" ||
			dependency.Provenance.AttemptID == "" || dependency.Provenance.ArtifactID == "" {
			t.Fatalf("missing package provenance: %+v", dependency)
		}
	}
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCoordinatorRecords(context.Background(), target); err != nil {
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
	reopened, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := domain.TasksForRun(reopened.WorkflowRuns[0], reopened.Tasks)[0].CarriedInputs
	if len(got) != 2 || got[0].SourceRunID == "" || got[1].SourceRunID == "" {
		t.Fatalf("reopened provenance = %+v", got)
	}
}

func mapArtifacts(values []domain.Artifact) map[string]domain.Artifact {
	result := make(map[string]domain.Artifact, len(values))
	for _, artifact := range values {
		result[artifact.ID] = artifact
	}
	return result
}

func TestManifestExternalInputsValidateShapeWithoutGuessingOutputs(t *testing.T) {
	task := ManifestTask{
		Needs:      []string{"source-run/producer"},
		InputsFrom: map[string][]string{"source-run/producer": {"report.txt"}},
		Class:      domain.TaskClassSurplus, PromptFile: "prompt.md", Importance: 1,
		Difficulty: 1, MaxTurns: 1,
		Routes: []ManifestRoute{{Instance: "codex", Model: "gpt"}},
	}
	if err := validateManifestTask("consumer", task, map[string]ManifestTask{"consumer": task}); err != nil {
		t.Fatal(err)
	}
	task.InputsFrom = map[string][]string{"source-run/__sink": {"report.txt"}}
	task.Needs = []string{"source-run/__sink"}
	if err := validateManifestTask("consumer", task, map[string]ManifestTask{"consumer": task}); err == nil {
		t.Fatal("sink output input was accepted")
	}
}
