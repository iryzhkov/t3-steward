package backlog

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"os"
	"path/filepath"
	"testing"
)

func TestCarriedGateKeepsReportAndLogPaths(t *testing.T) {
	storage := t.TempDir()
	workspace := t.TempDir()
	task := domain.Task{ID: "consumer", Name: "consumer"}
	var artifacts []domain.Artifact
	byID := map[string]domain.Artifact{}
	for _, name := range []string{"gate", "gate/log.txt"} {
		physicalName := name
		if name == "gate" {
			physicalName = "gate/report.json"
		}
		source := storedOutputArtifact(t, storage, "producer", "attempt", physicalName, "evidence")
		source.Name = name
		source.ID = "reference-" + name
		source.WorkflowRunID = "rerun"
		source.Kind = domain.ArtifactInput
		source.TaskID = task.ID
		source.AttemptID = ""
		artifacts = append(artifacts, source)
		byID[source.ID] = source
		task.CarriedInputs = append(task.CarriedInputs, domain.CarriedInput{Producer: "producer", ProducerTaskID: "producer", Name: name, ArtifactID: source.ID, SourceKind: domain.ArtifactGate})
	}
	paths, err := MaterializeDependencies(workspace, storage, "rerun", task, []domain.Task{task}, artifacts)
	if err != nil {
		t.Fatal(err)
	}
	cleanupImmutable(t, filepath.Join(workspace, ".t3", "dependencies"))
	if len(paths) != 2 {
		t.Fatalf("paths %+v", paths)
	}
	for _, name := range []string{"gate/report.json", "gate/log.txt"} {
		if _, err := os.Stat(filepath.Join(workspace, ".t3", "dependencies", "producer", name)); err != nil {
			t.Fatal(err)
		}
	}
	inputs, err := packageCarriedInputs(task, byID)
	if err != nil {
		t.Fatal(err)
	}
	if inputs[0].Artifacts[0].Path != "dependencies/producer/gate/report.json" || inputs[0].Artifacts[1].Path != "dependencies/producer/gate/log.txt" {
		t.Fatalf("carried %+v", inputs)
	}
	legacy := domain.CarriedInput{Name: "gate"}
	if gateCarriedDependencyPath(legacy) != "gate" {
		t.Fatal("ordinary gate-named output moved")
	}
}
