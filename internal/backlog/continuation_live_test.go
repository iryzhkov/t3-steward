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

// liveContinuationUpload is the upload a worker makes when it takes a
// snapshot while its attempt runs: the snapshot and its metadata, under the
// attempt's identity and the snapshot's sequence, on the checkpoint channel.
func liveContinuationUpload(t *testing.T, assignment domain.Assignment, checkpoint domain.ContinuationCheckpoint, snapshot []byte, uploadedAt time.Time) (workerproto.ArtifactUploadResponse, resultUploadOpener) {
	t.Helper()
	snapshotID := "continuation-" + assignment.AttemptID + "-1"
	metadataID := "continuation-meta-" + assignment.AttemptID + "-1"
	data := resultUploadOpener{snapshotID: snapshot, metadataID: continuationMetadata(t, checkpoint)}
	objects := []workerproto.ArtifactObject{
		resultObject(snapshotID, "checkpoints/"+snapshotID+".md", "checkpoint", "text/markdown", data[snapshotID]),
		resultObject(metadataID, "checkpoints/"+metadataID+".json", "checkpoint", "application/json", data[metadataID]),
	}
	manifest := resultManifest(uploadedAt, assignment, objects)
	manifest.ID = "upload-" + assignment.ID + "-checkpoint-continuation-00000000000000000001"
	manifest.WorkerID, manifest.WorkerEpoch = assignment.WorkerID, assignment.WorkerEpoch
	return workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}, data
}

// R1: an attempt takes a turn-end snapshot and is superseded before it
// publishes any result. The snapshot reached the coordinator while the
// attempt still held its assignment, so the replacement attempt's package
// carries it, and the task's surfaces report it.
func TestSupersededAttemptHandsItsLiveCheckpointToItsReplacement(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 10, 22, 0, 0, 0, time.UTC)
	records, replacement := packageBuilderFixture(now)
	consumer := records.Tasks[1]

	// Capture: attempt-0 runs on worker-a and hands its turn-end snapshot on.
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	superseded := domain.Attempt{ID: "attempt-0", WorkflowRunID: "run-1", TaskID: consumer.ID, Number: 1, Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 3, AssignmentID: "assignment-0", UpdatedAt: now.Add(-time.Hour)}
	claimed := domain.Assignment{ID: "assignment-0", AttemptID: superseded.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentClaimed, Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour)}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: consumer.WorkflowID}}, Tasks: []domain.Task{consumer}, Attempts: []domain.Attempt{superseded}, Assignments: []domain.Assignment{claimed}}); err != nil {
		t.Fatal(err)
	}
	snapshot := []byte("step 3 of 5; next: the gate\n")
	captured := now.Add(-20 * time.Minute)
	checkpoint := domain.ContinuationCheckpoint{AttemptID: superseded.ID, Sequence: 1, Turn: "turn-3", Boundary: domain.ContinuationTurnEnd,
		SHA256: resultObject("x", "x", "checkpoint", "text/markdown", snapshot).SHA256, Size: int64(len(snapshot)), OriginalSize: int64(len(snapshot)), CapturedAt: captured}
	response, data := liveContinuationUpload(t, claimed, checkpoint, snapshot, now.Add(-19*time.Minute))
	importer := CoordinatorCheckpointImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 1024, MaxTotalBytes: 4096, Now: func() time.Time { return now.Add(-18 * time.Minute) }}
	artifact, err := importer.Import(ctx, response, data)
	if err != nil {
		t.Fatalf("the live snapshot of a running attempt was not imported: %v", err)
	}
	if artifact.Name != domain.ContinuationArtifactName || artifact.AttemptID != superseded.ID || !artifact.CreatedAt.Equal(captured) || artifact.Size != int64(len(snapshot)) {
		t.Fatalf("live snapshot artifact = %+v", artifact)
	}
	// A replay after a coordinator restart is the same immutable artifact.
	if replay, err := importer.Import(ctx, response, data); err != nil || replay != artifact {
		t.Fatalf("replay = %+v, %v", replay, err)
	}

	// Supersession: attempt-0's assignment moves on; it never publishes a result.
	superseded.Progress, superseded.Control = domain.ProgressFailed, domain.ControlStopped
	records.Attempts[0].Number = 2
	records.Attempts = append(records.Attempts, superseded)
	stored, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	records.Artifacts = append(records.Artifacts, stored.Artifacts...)

	// Replacement delivery: the next attempt receives the snapshot.
	builder := packageBuilder(t, records)
	builder.WorkerCapabilities = map[string][]string{"normandy": workerproto.SupportedPackageCapabilities()}
	offer, err := builder.BuildAssignmentOffer(ctx, replacement, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	pkg := offer.Package.Package
	if pkg.Continuation == nil || pkg.Continuation.AttemptID != superseded.ID || pkg.Continuation.Size != int64(len(snapshot)) || !pkg.Continuation.CapturedAt.Equal(captured) {
		t.Fatalf("the replacement did not receive the superseded attempt's checkpoint: %+v", pkg.Continuation)
	}
	var delivered *workerproto.ArtifactObject
	for index := range pkg.StaticInputs {
		if pkg.StaticInputs[index].Path == workerproto.ContinuationInputPath {
			delivered = &pkg.StaticInputs[index]
		}
	}
	if delivered == nil || delivered.ID != artifact.ID || delivered.SHA256 != artifact.SHA256 {
		t.Fatalf("static inputs = %+v", pkg.StaticInputs)
	}
	if latest := LatestContinuationArtifact(records.Artifacts, "run-1", consumer.ID, ""); latest == nil || latest.ID != artifact.ID {
		t.Fatalf("the task's latest checkpoint = %+v", latest)
	}
}

// A turn-end snapshot polled after the attempt reported its result, while it
// verifies, is still the attempt's own and is imported: it may be the only
// copy when the result could not carry it. Different bytes under a sequence
// already imported are refused for good rather than retried on every pass.
func TestLiveContinuationImportWhileVerifyingAndUnderAReusedSequence(t *testing.T) {
	ctx := context.Background()
	now := coordinatorTestTime
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	task := testTask("task")
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressVerifying, Control: domain.ControlStopped, Revision: 4, AssignmentID: "assignment-1", UpdatedAt: now}
	assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentCompleted, Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: task.WorkflowID}}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	importer := CoordinatorCheckpointImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 1024, MaxTotalBytes: 4096, Now: func() time.Time { return now.Add(2 * time.Minute) }}
	upload := func(snapshot []byte) (workerproto.ArtifactUploadResponse, resultUploadOpener) {
		checkpoint := domain.ContinuationCheckpoint{AttemptID: attempt.ID, Sequence: 1, Turn: "turn-1", Boundary: domain.ContinuationTurnEnd,
			SHA256: resultObject("x", "x", "checkpoint", "text/markdown", snapshot).SHA256, Size: int64(len(snapshot)), OriginalSize: int64(len(snapshot)), CapturedAt: now}
		return liveContinuationUpload(t, assignment, checkpoint, snapshot, now.Add(time.Minute))
	}
	response, data := upload([]byte("last turn: done\n"))
	if artifact, err := importer.Import(ctx, response, data); err != nil || artifact.Name != domain.ContinuationArtifactName {
		t.Fatalf("a snapshot of a verifying attempt was not imported: %+v, %v", artifact, err)
	}
	reused, reusedData := upload([]byte("other bytes, same sequence\n"))
	if artifact, err := importer.Import(ctx, reused, reusedData); !errors.Is(err, ErrCheckpointImportRejected) {
		t.Fatalf("a reused sequence = %+v, %v; want a final refusal", artifact, err)
	}
}

// The live checkpoint channel keeps the fences of every other upload: a
// snapshot from an assignment that has moved on, or one its metadata does not
// describe, is refused for good rather than imported.
func TestLiveContinuationImportIsFencedToTheAttempt(t *testing.T) {
	ctx := context.Background()
	now := coordinatorTestTime
	snapshot := []byte("resume at step 2\n")
	for _, tc := range []struct {
		name   string
		mutate func(*domain.Assignment, *domain.ContinuationCheckpoint)
	}{
		{name: "released", mutate: func(assignment *domain.Assignment, _ *domain.ContinuationCheckpoint) {
			assignment.State = domain.AssignmentReleased
		}},
		{name: "another attempt", mutate: func(_ *domain.Assignment, checkpoint *domain.ContinuationCheckpoint) {
			checkpoint.AttemptID = "attempt-9"
		}},
		{name: "size", mutate: func(_ *domain.Assignment, checkpoint *domain.ContinuationCheckpoint) { checkpoint.Size++ }},
		{name: "capture after upload", mutate: func(_ *domain.Assignment, checkpoint *domain.ContinuationCheckpoint) {
			checkpoint.CapturedAt = now.Add(time.Hour)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			task := testTask("task")
			attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 1, AssignmentID: "assignment-1", UpdatedAt: now}
			assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentClaimed, Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
			checkpoint := domain.ContinuationCheckpoint{AttemptID: attempt.ID, Sequence: 1, Turn: "turn-1", Boundary: domain.ContinuationPause,
				SHA256: resultObject("x", "x", "checkpoint", "text/markdown", snapshot).SHA256, Size: int64(len(snapshot)), OriginalSize: int64(len(snapshot)), CapturedAt: now}
			upload := assignment
			tc.mutate(&assignment, &checkpoint)
			response, data := liveContinuationUpload(t, upload, checkpoint, snapshot, now.Add(time.Minute))
			if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: task.WorkflowID}}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
				t.Fatal(err)
			}
			importer := CoordinatorCheckpointImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 1024, MaxTotalBytes: 4096, Now: func() time.Time { return now.Add(2 * time.Minute) }}
			artifact, err := importer.Import(ctx, response, data)
			if !errors.Is(err, ErrCheckpointImportRejected) {
				t.Fatalf("import = %+v, %v; want a final refusal", artifact, err)
			}
			stored, err := store.LoadCoordinatorRecords(ctx)
			if err != nil || len(stored.Artifacts) != 0 {
				t.Fatalf("a refused snapshot was kept: %+v, %v", stored.Artifacts, err)
			}
		})
	}
}
