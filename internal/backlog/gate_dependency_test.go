package backlog

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
	"time"
)

func TestCoordinatorOfferBuilderGateRetrySelectsSuccessfulAttempt(t *testing.T) {
	now := time.Now().UTC()
	records, assignment := packageBuilderFixture(now)
	for i := range records.Tasks {
		if records.Tasks[i].Name == "producer" {
			records.Tasks[i].Gate = &domain.TaskGate{Commands: []string{"true"}, Timeout: time.Minute}
		}
		if records.Tasks[i].Name == "consumer" {
			records.Tasks[i].DependencyInputs = map[string][]string{"producer": {"reports/result.txt", "gate", "gate/log.txt"}}
		}
	}
	records.Attempts = append(records.Attempts,
		domain.Attempt{ID: "producer-failed", WorkflowRunID: "run-1", TaskID: "task-producer", Number: 1, Progress: domain.ProgressFailed},
		domain.Attempt{ID: "producer-success", WorkflowRunID: "run-1", TaskID: "task-producer", Number: 2, Progress: domain.ProgressSucceeded})
	var output domain.Artifact
	for i := range records.Artifacts {
		if records.Artifacts[i].Kind == domain.ArtifactOutput {
			records.Artifacts[i].AttemptID = "producer-success"
			output = records.Artifacts[i]
		}
	}
	old := output
	old.ID = "old-output"
	old.AttemptID = "producer-failed"
	old.CreatedAt = now.Add(time.Hour)
	records.Artifacts = append(records.Artifacts, old)
	for _, attempt := range []string{"producer-failed", "producer-success"} {
		for _, name := range []string{"gate", "gate/log.txt"} {
			a := output
			a.ID = attempt + "-" + name
			a.Name = name
			a.Kind = domain.ArtifactGate
			a.AttemptID = attempt
			if name == "gate" {
				a.MediaType = "application/json"
			}
			records.Artifacts = append(records.Artifacts, a)
		}
	}
	offer, err := packageBuilder(t, records).BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("gate retry stranded dependent: %v", err)
	}
	for _, dependency := range offer.Package.Package.Dependencies {
		for _, a := range dependency.Artifacts {
			if strings.Contains(a.ID, "failed") || a.ID == "old-output" {
				t.Fatalf("dependent received failed attempt: %+v", a)
			}
		}
	}
}

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
