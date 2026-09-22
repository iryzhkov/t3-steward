package backlog

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestProjectContextColdStartCustodySurvivesCrashRestartAndProducerArchive(t *testing.T) {
	ctx := context.Background()
	source, manifest, target := externalInputFixture(domain.ProgressSucceeded)
	digest := strings.Repeat("a", 64)
	source.records.Artifacts[0].SHA256 = digest

	now := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	fresh := now.Add(24 * time.Hour)
	index := &domain.ProjectContext{
		Version: domain.ProjectContextVersion, Revision: "context-1", Status: domain.ProjectContextAccepted,
		Objective: "complete from retained accepted evidence", Authority: []string{"coordinator:target-run"},
		Budget: "one attempt", Outputs: []string{"result.txt"},
		RequiredReferences: []string{"accepted-producer"},
		References: []domain.ProjectContextReference{{
			ID: "accepted-producer", Kind: domain.ContextReferenceExecution,
			URI:      "execution:source-run/producer-id/producer-attempt/source-output",
			Revision: digest, Status: domain.ProjectContextAccepted, Authority: "review-gate",
			Topics: []string{"accepted", "checkpoint"},
		}},
		Decisions: []domain.ProjectContextDecision{{
			ID: "reuse-branch", Status: "accepted", Summary: "reuse unchanged accepted producer",
			Authority: "review-gate", Topics: []string{"resume"},
		}},
		CheckpointDelta: []string{"producer accepted; consumer remains"},
		CodeLocations:   []domain.ProjectContextLocation{{Path: "internal/backlog/external_inputs.go", Revision: strings.Repeat("b", 40)}},
		InputLocations:  []domain.ProjectContextLocation{{Path: "report.txt", Revision: digest}},
		Setup:           []string{"go version"}, Checks: []string{"go test ./..."},
		CapabilityRefs: []string{"accepted-producer"},
		Freshness:      domain.ProjectContextFreshness{ObservedAt: now, FreshThrough: &fresh},
	}
	manifestTask := manifest.Tasks["consumer"]
	manifestTask.Context = index
	manifest.Tasks["consumer"] = manifestTask
	target.Tasks[0].Context = index
	target.Attempts = []domain.Attempt{{
		ID: "consumer-attempt", WorkflowRunID: "target-run", TaskID: "consumer-id",
		Number: 1, Progress: domain.ProgressBlocked, Control: domain.ControlUnassigned,
	}}
	// A real coordinator store contains both runs until retention archives the
	// producer. Keeping them here also proves the successor stays blocked before
	// the target-run reference is accepted.
	target.WorkflowRuns = append(target.WorkflowRuns, source.records.WorkflowRuns...)
	target.Tasks = append(target.Tasks, source.records.Tasks...)
	target.Attempts = append(target.Attempts, source.records.Attempts...)
	target.Artifacts = append(target.Artifacts, source.records.Artifacts...)

	dbPath := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlite.OpenMigrated(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCoordinatorRecords(ctx, target); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// The source bytes are already in accepted artifact custody. Simulate a crash
	// after external-input resolution but before the target acceptance transaction:
	// discard the mutated in-memory records and reopen the durable baseline.
	staged := target
	ingester := BundleIngester{Store: source, NewTypedID: func(string) string { return "retained-input" }}
	if err := ingester.retainExternalInputs(ctx, manifest, &staged); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.OpenMigrated(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(reopened.Artifacts) != 1 || reopened.Artifacts[0].WorkflowRunID != "source-run" ||
		len(reopened.Tasks[0].CarriedInputs) != 0 || reopened.Attempts[0].Progress != domain.ProgressBlocked {
		t.Fatalf("crash published unaccepted successor state: %+v", reopened)
	}

	// Retry the same acceptance after restart. The exact accepted producer is
	// rebound once; a parallel producer with the same names cannot be selected.
	source.records.WorkflowRuns = append(source.records.WorkflowRuns, domain.WorkflowRun{
		ID: "parallel-run", WorkflowID: "parallel-workflow", Progress: domain.ProgressSucceeded,
	})
	source.records.Tasks = append(source.records.Tasks, domain.Task{
		ID: "parallel-producer-id", WorkflowID: "parallel-workflow", Name: "producer",
		Outputs: []domain.ArtifactDeclaration{{Name: "report.txt", MediaType: "text/plain"}},
	})
	source.records.Attempts = append(source.records.Attempts, domain.Attempt{
		ID: "parallel-attempt", WorkflowRunID: "parallel-run", TaskID: "parallel-producer-id",
		Number: 1, Progress: domain.ProgressSucceeded, Control: domain.ControlStopped,
	})
	source.records.Artifacts = append(source.records.Artifacts, domain.Artifact{
		ID: "parallel-output", WorkflowRunID: "parallel-run", TaskID: "parallel-producer-id",
		AttemptID: "parallel-attempt", Kind: domain.ArtifactOutput, Name: "report.txt",
		MediaType: "text/plain", Size: 7, SHA256: strings.Repeat("c", 64), StoragePath: "parallel/report.txt",
	})
	if err := ingester.retainExternalInputs(ctx, manifest, &reopened); err != nil {
		t.Fatal(err)
	}
	binding := reopened.Tasks[0].Context.References[0].Binding
	if binding == nil || binding.SourceRunID != "source-run" || binding.SourceAttemptID != "producer-attempt" ||
		binding.SourceArtifactID != "source-output" {
		t.Fatalf("wrong producer bound: %+v", binding)
	}
	if err := store.SaveCoordinatorRecords(ctx, reopened); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = sqlite.OpenMigrated(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	accepted, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted.Artifacts) != 2 || len(accepted.Tasks[0].CarriedInputs) != 1 {
		t.Fatalf("accepted custody missing after restart: %+v", accepted)
	}

	// Producer archival cannot make the successor rerun the accepted branch:
	// remove every source record and package only from the target-run reference.
	var targetRun domain.WorkflowRun
	var consumer domain.Task
	var consumerAttempt domain.Attempt
	var retained domain.Artifact
	for _, run := range accepted.WorkflowRuns {
		if run.ID == "target-run" {
			targetRun = run
		}
	}
	for _, task := range accepted.Tasks {
		if task.ID == "consumer-id" {
			consumer = task
		}
	}
	for _, attempt := range accepted.Attempts {
		if attempt.ID == "consumer-attempt" {
			consumerAttempt = attempt
		}
	}
	for _, artifact := range accepted.Artifacts {
		if artifact.WorkflowRunID == "target-run" {
			retained = artifact
		}
	}
	accepted.WorkflowRuns = []domain.WorkflowRun{targetRun}
	accepted.Tasks = []domain.Task{consumer}
	accepted.Attempts = []domain.Attempt{consumerAttempt}
	accepted.Artifacts = []domain.Artifact{retained}
	dependencies, err := packageDependencies(consumer, accepted.Tasks, mapArtifacts(accepted.Artifacts), "target-run")
	if err != nil {
		t.Fatal(err)
	}
	if len(dependencies) != 1 || dependencies[0].Provenance == nil ||
		dependencies[0].Provenance.RunID != "source-run" ||
		dependencies[0].Provenance.SourceArtifacts["retained-input"] != "source-output" {
		t.Fatalf("archived producer evidence was not reusable: %+v", dependencies)
	}
}
