package backlog

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
	"time"
)

func TestGateDependencyPackage(t *testing.T) {
	now := time.Now()
	records, _ := packageBuilderFixture(now)
	var task domain.Task
	for _, candidate := range records.Tasks {
		if candidate.ID == "task-consumer" {
			task = candidate
		}
	}
	task.DependencyInputs = map[string][]string{"producer": {"gate", "gate/log.txt"}}
	artifacts := map[string]domain.Artifact{}
	for _, a := range records.Artifacts {
		if a.Name == "reports/result.txt" {
			for _, name := range []string{"gate", "gate/log.txt"} {
				copy := a
				copy.ID = name
				copy.Name = name
				copy.Kind = domain.ArtifactGate
				artifacts[name] = copy
			}
		}
	}
	inputs, err := packageDependencies(task, records.Tasks, artifacts, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(inputs) != 1 || len(inputs[0].Artifacts) != 2 || inputs[0].Artifacts[0].Path != "dependencies/producer/gate/report.json" || inputs[0].Artifacts[1].Path != "dependencies/producer/gate/log.txt" {
		t.Fatalf("gate dependencies %+v", inputs)
	}
}
