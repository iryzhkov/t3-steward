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

// S14: homelab announced a checkpoint under coordinator epoch 224. After the
// coordinator restarted, the importer required its own epoch, refused the
// upload on every boundary, and because a refused upload is never
// acknowledged, homelab's whole exchange failed on every boundary after that.
func TestCoordinatorCheckpointImporterAcceptsAnEarlierEpochAndRejectsASettledBinding(t *testing.T) {
	ctx := context.Background()
	now := coordinatorTestTime
	path := filepath.Join(t.TempDir(), "state.db")
	// Each restart is a new process acquiring coordinator authority, which
	// advances the epoch; the upload below was announced before them.
	var epoch int64
	for epoch < 3 {
		restarted, err := sqlite.OpenMigrated(path)
		if err != nil {
			t.Fatal(err)
		}
		if epoch, err = restarted.AcquireCoordinator(ctx, "coordinator"); err != nil {
			t.Fatal(err)
		}
		restarted.Close()
	}
	store, err := sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	task := testTask("task")
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressActive, Control: domain.ControlPaused, Revision: 2, AssignmentID: "assignment-1", CheckpointArtifactID: "checkpoint-attempt-1-deadbeef", UpdatedAt: now}
	assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentClaimed, Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: task.WorkflowID}}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	data := []byte("resume here\n")
	object := resultObject("checkpoint-attempt-1-deadbeef", "checkpoints/checkpoint-attempt-1-deadbeef.md", "checkpoint", "text/markdown", data)
	checkpoint := &domain.CheckpointMetadata{ArtifactID: object.ID, Path: ".t3/checkpoint.md", SHA256: object.SHA256, Size: object.Size, CapturedAt: now.Add(time.Minute)}
	record := domain.ThrottleAttemptRecord{DirectiveID: "directive-1", AttemptID: attempt.ID, Revision: 1, Delivery: domain.ThrottleDeliveryAcknowledged, Result: domain.ThrottleResultCheckpointed, Control: domain.ControlPaused, Checkpoint: checkpoint, Command: domain.ThrottleCommand{ID: "throttle-1", DirectiveID: "directive-1", AttemptID: attempt.ID, Kind: domain.ThrottleCommandDrain}, UpdatedAt: now.Add(time.Minute)}
	if err := store.CommitThrottleAttemptTransitions(ctx, []domain.ThrottleAttemptTransition{{ExpectedRevision: 0, Record: record}}); err != nil {
		t.Fatal(err)
	}
	// Announced under epoch 1; the coordinator is now at epoch 3.
	manifest := workerproto.ArtifactTransferManifest{Version: 1, ID: "upload-assignment-1-checkpoint-deadbeef", Direction: "upload", CoordinatorEpoch: 1, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, Objects: []workerproto.ArtifactObject{object}, TotalBytes: object.Size, CreatedAt: now.Add(time.Minute), ExpiresAt: now.Add(time.Hour)}
	response := workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}
	importer := CoordinatorCheckpointImporter{CoordinatorID: "coordinator", CoordinatorEpoch: epoch, Store: store, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 1024, MaxTotalBytes: 1024, Now: func() time.Time { return now.Add(3 * time.Minute) }}
	opener := resultUploadOpener{object.ID: data}
	if artifact, err := importer.Import(ctx, response, opener); err != nil || artifact.ID != object.ID {
		t.Fatalf("a checkpoint from an earlier coordinator epoch was refused: %#v, %v", artifact, err)
	}

	// Pause, then resume before the import: planning the resume rewrites the
	// throttle record's delivery and result but keeps its checkpoint, and the
	// running attempt still records it, so the import must still bind.
	resumed := record
	resumed.Revision, resumed.Delivery, resumed.Result, resumed.Control = 2, domain.ThrottleDeliveryPending, "", domain.ControlRunning
	if err := store.CommitThrottleAttemptTransitions(ctx, []domain.ThrottleAttemptTransition{{ExpectedRevision: 1, Record: resumed}}); err != nil {
		t.Fatal(err)
	}
	resumedAttempt := attempt
	resumedAttempt.Control, resumedAttempt.Revision = domain.ControlRunning, 3
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{resumedAttempt}}); err != nil {
		t.Fatal(err)
	}
	if artifact, err := importer.Import(ctx, response, opener); err != nil || artifact.ID != object.ID {
		t.Fatalf("a checkpoint of a paused-then-resumed attempt was refused: %#v, %v", artifact, err)
	}

	// A different checkpoint for an assignment that has since been released
	// can never be imported: it is rejected so the caller can discard it.
	other := resultObject("checkpoint-attempt-1-cafebabe", "checkpoints/checkpoint-attempt-1-cafebabe.md", "checkpoint", "text/markdown", []byte("stale\n"))
	stale := manifest
	stale.ID, stale.Objects, stale.TotalBytes = "upload-assignment-1-checkpoint-cafebabe", []workerproto.ArtifactObject{other}, other.Size
	released := assignment
	released.State = domain.AssignmentReleased
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Assignments: []domain.Assignment{released}}); err != nil {
		t.Fatal(err)
	}
	_, err = importer.Import(ctx, workerproto.ArtifactUploadResponse{Manifest: stale, Custody: resultCustody(t, stale, "coordinator")}, resultUploadOpener{other.ID: []byte("stale\n")})
	if !errors.Is(err, ErrCheckpointImportRejected) {
		t.Fatalf("a checkpoint for a released assignment was not rejected: %v", err)
	}

	// A claimed assignment whose attempt is not marked paused yet may still
	// be; that refusal is retried, never discarded.
	running := attempt
	running.Control, running.CheckpointArtifactID, running.Revision = domain.ControlRunning, "", 3
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{running}, Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	_, err = importer.Import(ctx, workerproto.ArtifactUploadResponse{Manifest: stale, Custody: resultCustody(t, stale, "coordinator")}, resultUploadOpener{other.ID: []byte("stale\n")})
	if err == nil || errors.Is(err, ErrCheckpointImportRejected) {
		t.Fatalf("a checkpoint that may yet bind was discarded or imported: %v", err)
	}
}
