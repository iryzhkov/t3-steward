package backlog

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// An overseer activation is dispatched like any other work and returns a result,
// but it is not a node of the run's graph, so no declared task will ever be
// found for it. Binding it to one made every import pass for that worker fail.
func TestAnActivationResultImportsWithoutADeclaredTask(t *testing.T) {
	ctx := context.Background()
	now := coordinatorTestTime
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	attempt := domain.Attempt{
		ID: "activation-attempt-1", WorkflowRunID: "run-1", TaskID: "activation-1",
		SupervisionActivationID: "activation-1", Number: 1, Progress: domain.ProgressVerifying,
		Control: domain.ControlStopped, Revision: 3, AssignmentID: "assignment-1", UpdatedAt: now,
	}
	assignment := domain.Assignment{
		ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1",
		State: domain.AssignmentCompleted, ThreadID: "thread-1", Epoch: 1, LeaseToken: "lease",
		DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now,
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: "workflow-1"}},
		Attempts:     []domain.Attempt{attempt},
		Assignments:  []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}

	archive := []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`)
	data := resultUploadOpener{
		"final-message-" + attempt.ID:  []byte("reviewed\n"),
		"thread-archive-" + attempt.ID: archive,
	}
	objects := []workerproto.ArtifactObject{
		resultObject("final-message-"+attempt.ID, "results/final-message.md", "summary", "text/markdown", data["final-message-"+attempt.ID]),
		resultObject("thread-archive-"+attempt.ID, "results/thread.json", "log", "application/json", archive),
	}
	manifest := resultManifest(now, assignment, objects)
	response := workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}
	importer := CoordinatorResultImporter{
		CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store,
		Artifacts:        CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store},
		MaxArtifactBytes: 1024, MaxTotalBytes: 4096, Now: func() time.Time { return now.Add(time.Minute) },
	}

	report, err := importer.Import(ctx, response, data)
	if err != nil {
		t.Fatalf("an activation result was refused: %v", err)
	}
	if len(report.Artifacts) != 2 || len(report.Transition) != 1 {
		t.Fatalf("report = %#v", report)
	}
	for _, artifact := range report.Artifacts {
		if artifact.TaskID != attempt.TaskID {
			t.Errorf("artifact %q was filed under task %q", artifact.ID, artifact.TaskID)
		}
	}
}

// A declared attempt whose task the coordinator does not have can never import,
// because task deletion is unsupported. It is dead-lettered once, so the rest of
// that worker's held results still move.
func TestAResultWhoseTaskIsGoneIsDeadLetteredRatherThanRetriedForever(t *testing.T) {
	ctx := context.Background()
	now := coordinatorTestTime
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	attempt := domain.Attempt{
		ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-that-is-gone", Number: 1,
		Progress: domain.ProgressVerifying, Control: domain.ControlStopped, Revision: 3,
		AssignmentID: "assignment-1", UpdatedAt: now,
	}
	assignment := domain.Assignment{
		ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1",
		State: domain.AssignmentCompleted, ThreadID: "thread-1", Epoch: 1, LeaseToken: "lease",
		DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now,
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: "workflow-1"}},
		Attempts:     []domain.Attempt{attempt},
		Assignments:  []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}

	data := resultUploadOpener{"final-message-attempt-1": []byte("finished\n")}
	objects := []workerproto.ArtifactObject{
		resultObject("final-message-attempt-1", "results/final-message.md", "summary", "text/markdown", data["final-message-attempt-1"]),
	}
	manifest := resultManifest(now, assignment, objects)
	response := workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}
	importer := CoordinatorResultImporter{
		CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store,
		Artifacts:        CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store},
		MaxArtifactBytes: 1024, MaxTotalBytes: 4096, Now: func() time.Time { return now.Add(time.Minute) },
	}

	report, err := importer.Import(ctx, response, data)
	if !errors.Is(err, ErrResultImportRejected) {
		t.Fatalf("error = %v, want a rejection", err)
	}
	if len(report.Transition) != 1 || !report.Transition[0].Attempt.Progress.Terminal() {
		t.Fatalf("the rejected attempt was left unsettled: %#v", report.Transition)
	}
}
