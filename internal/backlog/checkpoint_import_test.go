package backlog

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestCoordinatorCheckpointImporterRequiresThrottleEvidenceAndReplays(t *testing.T) {
	ctx := context.Background()
	now := coordinatorTestTime
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	task := testTask("task")
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 1, AssignmentID: "assignment-1", UpdatedAt: now}
	assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentClaimed, Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: task.WorkflowID}}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	data := []byte("resume here\n")
	object := resultObject("checkpoint-attempt-1-deadbeef", "checkpoints/checkpoint-attempt-1-deadbeef.md", "checkpoint", "text/markdown", data)
	checkpoint := &domain.CheckpointMetadata{ArtifactID: object.ID, Path: ".t3/checkpoint.md", SHA256: object.SHA256, Size: object.Size, CapturedAt: now.Add(time.Minute)}
	initial := domain.ThrottleAttemptRecord{DirectiveID: "directive-1", AttemptID: attempt.ID, Revision: 1, Delivery: domain.ThrottleDeliveryPending, Control: domain.ControlDraining, Command: domain.ThrottleCommand{ID: "throttle-1", DirectiveID: "directive-1", AttemptID: attempt.ID, Kind: domain.ThrottleCommandDrain}, UpdatedAt: now.Add(time.Minute)}
	if err := store.CommitThrottleAttemptTransitions(ctx, []domain.ThrottleAttemptTransition{{ExpectedRevision: 0, Record: initial}}); err != nil {
		t.Fatal(err)
	}
	acknowledged := initial
	acknowledged.Revision = 2
	acknowledged.Delivery = domain.ThrottleDeliveryAcknowledged
	acknowledged.Result = domain.ThrottleResultCheckpointed
	acknowledged.Control = domain.ControlPaused
	acknowledged.Checkpoint = checkpoint
	acknowledged.UpdatedAt = now.Add(2 * time.Minute)
	if err := store.CommitThrottleAttemptTransitions(ctx, []domain.ThrottleAttemptTransition{{ExpectedRevision: 1, Record: acknowledged}}); err != nil {
		t.Fatal(err)
	}
	manifest := workerproto.ArtifactTransferManifest{Version: 1, ID: "upload-assignment-1-checkpoint-deadbeef", Direction: "upload", CoordinatorEpoch: 1, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, Objects: []workerproto.ArtifactObject{object}, TotalBytes: object.Size, CreatedAt: now.Add(time.Minute), ExpiresAt: now.Add(time.Hour)}
	response := workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}
	importer := CoordinatorCheckpointImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 1024, MaxTotalBytes: 1024, Now: func() time.Time { return now.Add(3 * time.Minute) }}
	opener := resultUploadOpener{object.ID: data}
	artifact, err := importer.Import(ctx, response, opener)
	if err != nil {
		t.Fatal(err)
	}
	if artifact.ID != checkpoint.ArtifactID || artifact.Name != checkpoint.Path || artifact.Kind != domain.ArtifactCheckpoint {
		t.Fatalf("checkpoint artifact = %#v", artifact)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	records.Assignments[0].State = domain.AssignmentCompleted
	records.Attempts[0].Progress = domain.ProgressSucceeded
	records.Attempts[0].Control = domain.ControlStopped
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: records.Attempts, Assignments: records.Assignments}); err != nil {
		t.Fatal(err)
	}
	replay, err := importer.Import(ctx, response, opener)
	if err != nil || replay != artifact {
		t.Fatalf("checkpoint replay = %#v, %v", replay, err)
	}
}
