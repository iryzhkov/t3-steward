package backlog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A rerun carries an ancestor's output into the new run by reference. The
// promise that makes the carried file usable at all is that it arrives where
// the in-run dependency put it, because the task's prompt was written against
// that path.
func TestMaterializeDependenciesPlacesACarriedInputWhereTheDependencyWas(t *testing.T) {
	storage := t.TempDir()
	source := storedOutputArtifact(t, storage, "producer-id", "attempt-p", "reports/one.txt", "one")

	// What the source run delivered: an ordinary dependency input.
	dependencyWorkspace := t.TempDir()
	consumer := domain.Task{
		ID: "consumer-id", Name: "consumer", Needs: []string{"producer"},
		DependencyInputs: map[string][]string{"producer": {"reports/one.txt"}},
	}
	before, err := MaterializeDependencies(dependencyWorkspace, storage, "run-1", consumer,
		[]domain.Task{{ID: "producer-id", Name: "producer"}, consumer}, []domain.Artifact{source})
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, filepath.Join(dependencyWorkspace, ".t3", "dependencies"))

	// What the rerun delivers: a reference artifact of the new run, same
	// content address, carried rather than produced here.
	reference := source
	reference.ID = "input:rerun:key:0"
	reference.WorkflowRunID = "run-2"
	reference.TaskID = "task:rerun:key:0"
	reference.AttemptID = ""
	reference.Kind = domain.ArtifactInput
	rerun := domain.Task{
		ID: "task:rerun:key:0", Name: "consumer",
		CarriedInputs: []domain.CarriedInput{{
			Producer: "producer", ProducerTaskID: "producer-id",
			Name: "reports/one.txt", ArtifactID: reference.ID,
		}},
	}
	carriedWorkspace := t.TempDir()
	after, err := MaterializeDependencies(carriedWorkspace, storage, "run-2", rerun,
		[]domain.Task{rerun}, []domain.Artifact{reference})
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, filepath.Join(carriedWorkspace, ".t3", "dependencies"))
	if strings.Join(after, "|") != strings.Join(before, "|") {
		t.Fatalf("carried paths = %v, want the source run's %v", after, before)
	}
	raw, err := os.ReadFile(filepath.Join(carriedWorkspace, filepath.FromSlash(after[0])))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "one" {
		t.Fatalf("carried content = %q", raw)
	}
}

// A carried input that names an artifact of another run is refused. The
// reference must be owned by the run that reads it, which is what keeps every
// custody check downstream honest.
func TestMaterializeDependenciesRefusesACarriedInputOutsideTheRun(t *testing.T) {
	storage := t.TempDir()
	source := storedOutputArtifact(t, storage, "producer-id", "attempt-p", "reports/one.txt", "one")
	task := domain.Task{
		ID: "task-1", Name: "consumer",
		CarriedInputs: []domain.CarriedInput{{
			Producer: "producer", ProducerTaskID: "producer-id",
			Name: "reports/one.txt", ArtifactID: source.ID,
		}},
	}
	if _, err := MaterializeDependencies(t.TempDir(), storage, "run-2", task,
		[]domain.Task{task}, []domain.Artifact{source}); err == nil {
		t.Fatal("a carried input from another run was materialized")
	}
	missing := task
	missing.CarriedInputs[0].ArtifactID = "absent"
	if _, err := MaterializeDependencies(t.TempDir(), storage, "run-1", missing,
		[]domain.Task{missing}, []domain.Artifact{source}); err == nil {
		t.Fatal("a carried input with no artifact was materialized")
	}
}
