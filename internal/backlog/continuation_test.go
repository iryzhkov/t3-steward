package backlog

import (
	"context"
	"encoding/json"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func (s packageRecordStore) FreezeAssignmentContinuation(ctx context.Context, epoch int64, coordinator string, assignment domain.Assignment, identity workerproto.ExecutionIdentity, proposed sqlite.ContinuationDecision) (sqlite.ContinuationDecision, error) {
	return s.decisions.FreezeAssignmentContinuation(ctx, epoch, coordinator, assignment, identity, proposed)
}

func continuationMetadata(t *testing.T, checkpoint domain.ContinuationCheckpoint) []byte {
	t.Helper()
	raw, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// The coordinator keeps the snapshot a result carries, dated by when the
// worker took it, and drops a snapshot whose metadata does not describe it
// without failing the result that carried it.
func TestResultImportKeepsTheContinuationCheckpointDatedByItsCapture(t *testing.T) {
	for _, valid := range []bool{true, false} {
		ctx := context.Background()
		now := coordinatorTestTime
		store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
		if err != nil {
			t.Fatal(err)
		}
		task := testTask("task")
		attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressVerifying, Control: domain.ControlStopped, Revision: 3, AssignmentID: "assignment-1", UpdatedAt: now}
		assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentCompleted, ThreadID: "thread-1", Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
		if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: task.WorkflowID}}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
			t.Fatal(err)
		}
		snapshot := []byte("step 3 of 5; next: the gate\n")
		captured := now.Add(-10 * time.Minute)
		checkpoint := domain.ContinuationCheckpoint{AttemptID: attempt.ID, Sequence: 2, Turn: "turn-2", Boundary: domain.ContinuationTurnEnd,
			SHA256: resultObject("x", "results/x", "checkpoint", "text/markdown", snapshot).SHA256, Size: int64(len(snapshot)), OriginalSize: int64(len(snapshot)), CapturedAt: captured}
		if !valid {
			checkpoint.Size++
		}
		snapshotID, metadataID := domain.ContinuationArtifactID(attempt.ID), domain.ContinuationMetadataArtifactID(attempt.ID)
		data := resultUploadOpener{
			"final-message-attempt-1":  []byte("BACKLOG STATUS: done\n"),
			"thread-archive-attempt-1": []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`),
			snapshotID:                 snapshot,
			metadataID:                 continuationMetadata(t, checkpoint),
		}
		objects := []workerproto.ArtifactObject{
			resultObject(snapshotID, "results/"+domain.ContinuationArtifactName, "checkpoint", "text/markdown", data[snapshotID]),
			resultObject(metadataID, "results/"+domain.ContinuationMetadataArtifactName, "checkpoint", "application/json", data[metadataID]),
			resultObject("final-message-attempt-1", "results/final-message.md", "summary", "text/markdown", data["final-message-attempt-1"]),
			resultObject("thread-archive-attempt-1", "results/thread.json", "log", "application/json", data["thread-archive-attempt-1"]),
		}
		manifest := resultManifest(now, assignment, objects)
		response := workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: resultCustody(t, manifest, "coordinator")}
		importer := CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 1024, MaxTotalBytes: 4096, Now: func() time.Time { return now.Add(time.Minute) }}
		report, err := importer.Import(ctx, response, data)
		if err != nil {
			t.Fatalf("valid=%t: %v", valid, err)
		}
		if len(report.Transition) != 1 || report.Transition[0].Attempt.Progress != domain.ProgressSucceeded {
			t.Fatalf("valid=%t: the continuation changed the result's verdict: %#v", valid, report.Transition)
		}
		var kept *domain.Artifact
		for index := range report.Artifacts {
			if report.Artifacts[index].Name == domain.ContinuationArtifactName {
				kept = &report.Artifacts[index]
			}
		}
		if !valid {
			if kept != nil || len(report.Artifacts) != 2 {
				t.Fatalf("a snapshot its metadata does not describe was kept: %+v", report.Artifacts)
			}
			store.Close()
			continue
		}
		if kept == nil || kept.Kind != domain.ArtifactCheckpoint || kept.Size != int64(len(snapshot)) || !kept.CreatedAt.Equal(captured) || kept.AttemptID != attempt.ID {
			t.Fatalf("kept snapshot = %+v", kept)
		}
		// A replay after a coordinator restart is the same immutable artifact.
		replay, err := importer.Import(ctx, response, data)
		if err != nil || len(replay.Artifacts) != 4 || len(replay.Transition) != 0 {
			t.Fatalf("replay = %#v err=%v", replay, err)
		}
		store.Close()
	}
}

// A snapshot is only accepted under its own attempt's identity.
func TestResultImportRefusesAContinuationUnderAnotherAttemptsIdentity(t *testing.T) {
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task"}
	object := resultObject(domain.ContinuationArtifactID("attempt-0"), "results/"+domain.ContinuationArtifactName, "checkpoint", "text/markdown", []byte("x"))
	if _, err := resultArtifact(object, workerproto.ArtifactTransferManifest{}, attempt, domain.Task{ID: "task"}, coordinatorTestTime); err == nil {
		t.Fatal("a continuation snapshot under another attempt's identity was accepted")
	}
}

func continuationRetryFixture(now time.Time) (sqlite.CoordinatorRecords, domain.Assignment) {
	records, assignment := packageBuilderFixture(now)
	consumer := records.Tasks[1]
	previous := domain.Attempt{ID: "attempt-0", WorkflowRunID: "run-1", TaskID: consumer.ID, Number: 2, Progress: domain.ProgressFailed, Control: domain.ControlStopped, Revision: 5, UpdatedAt: now.Add(-time.Hour)}
	older := domain.Attempt{ID: "attempt-00", WorkflowRunID: "run-1", TaskID: consumer.ID, Number: 1, Progress: domain.ProgressFailed, Control: domain.ControlStopped, Revision: 5, UpdatedAt: now.Add(-2 * time.Hour)}
	records.Attempts[0].Number = 3
	records.Attempts = append(records.Attempts, previous, older)
	snapshot := func(attemptID, taskID string, captured time.Time) domain.Artifact {
		return domain.Artifact{
			ID: domain.ContinuationArtifactID(attemptID), WorkflowRunID: "run-1", TaskID: taskID, AttemptID: attemptID,
			Kind: domain.ArtifactCheckpoint, Name: domain.ContinuationArtifactName, MediaType: "text/markdown",
			Size: 42, SHA256: strings.Repeat("b", 64), StoragePath: "objects/" + attemptID, Producer: "worker:normandy", CreatedAt: captured,
		}
	}
	records.Artifacts = append(records.Artifacts,
		// Listed newest first so that order of storage cannot pass for order of capture.
		snapshot("attempt-0", consumer.ID, now.Add(-10*time.Minute)),
		snapshot("attempt-00", consumer.ID, now.Add(-90*time.Minute)),
		snapshot("attempt-producer", "task-producer", now.Add(-time.Minute)),
	)
	return records, assignment
}

// A retry of a task receives the latest snapshot an earlier attempt of the
// same task left, as an input outside the task's tree, and only from a worker
// that understands it.
func TestRetryPackageCarriesTheLatestContinuationCheckpoint(t *testing.T) {
	now := time.Date(2026, 9, 10, 22, 0, 0, 0, time.UTC)
	records, assignment := continuationRetryFixture(now)
	builder := packageBuilder(t, records)
	builder.WorkerCapabilities = map[string][]string{"normandy": workerproto.SupportedPackageCapabilities()}
	offer, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	pkg := offer.Package.Package
	if !slices.Contains(pkg.RequiredCapabilities, workerproto.PackageCapabilityContinuationCheckpoint) {
		t.Fatalf("capabilities = %v", pkg.RequiredCapabilities)
	}
	if pkg.Continuation == nil || pkg.Continuation.Path != workerproto.ContinuationInputPath || pkg.Continuation.AttemptID != "attempt-0" ||
		pkg.Continuation.Size != 42 || !pkg.Continuation.CapturedAt.Equal(now.Add(-10*time.Minute)) {
		t.Fatalf("continuation = %+v", pkg.Continuation)
	}
	var delivered *workerproto.ArtifactObject
	for index := range pkg.StaticInputs {
		if pkg.StaticInputs[index].Path == workerproto.ContinuationInputPath {
			delivered = &pkg.StaticInputs[index]
		}
	}
	if delivered == nil || delivered.ID != domain.ContinuationArtifactID("attempt-0") {
		t.Fatalf("static inputs = %+v", pkg.StaticInputs)
	}
	if err := workerproto.ValidateExecutionPackageManifest(offer.Package, 1<<20); err != nil {
		t.Fatal(err)
	}

	// The decision is frozen with the first offer: a replay is the same
	// package when the inventory cannot be read and when a newer checkpoint
	// has arrived in the meantime.
	builder.WorkerCapabilities = nil
	builder.Store = packageRecordStore{records: records, decisions: builder.Store.(packageRecordStore).decisions, snapshots: nil}
	newer := records
	newer.Artifacts = append(append([]domain.Artifact(nil), records.Artifacts...), domain.Artifact{
		ID: domain.ContinuationArtifactID("attempt-late"), WorkflowRunID: "run-1", TaskID: "task-consumer", AttemptID: "attempt-late",
		Kind: domain.ArtifactCheckpoint, Name: domain.ContinuationArtifactName, MediaType: "text/markdown",
		Size: 7, SHA256: strings.Repeat("d", 64), StoragePath: "objects/late", Producer: "worker:normandy", CreatedAt: now,
	})
	builder.Store = packageRecordStore{records: newer, decisions: builder.Store.(packageRecordStore).decisions}
	replay, err := builder.BuildAssignmentOffer(context.Background(), assignment, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if replay.Package.SHA256 != offer.Package.SHA256 {
		t.Fatalf("replayed package changed:\n%+v\n%+v", replay.Package.Package.Continuation, offer.Package.Package.Continuation)
	}

	// A worker that does not advertise the capability gets neither the input
	// nor the declaration, exactly the package it got before.
	plain, err := packageBuilder(t, records).BuildAssignmentOffer(context.Background(), assignment, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if plain.Package.Package.Continuation != nil || slices.Contains(plain.Package.Package.RequiredCapabilities, workerproto.PackageCapabilityContinuationCheckpoint) ||
		len(plain.Package.Package.StaticInputs) != 1 {
		t.Fatalf("package for an older worker = %+v", plain.Package.Package)
	}

	// A first attempt has nothing to receive but still declares that its own
	// snapshot will be accepted.
	first, firstAssignment := packageBuilderFixture(now)
	builder = packageBuilder(t, first)
	builder.WorkerCapabilities = map[string][]string{"normandy": workerproto.SupportedPackageCapabilities()}
	offer, err = builder.BuildAssignmentOffer(context.Background(), firstAssignment, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if offer.Package.Package.Continuation != nil || !slices.Contains(offer.Package.Package.RequiredCapabilities, workerproto.PackageCapabilityContinuationCheckpoint) {
		t.Fatalf("first attempt package = %+v", offer.Package.Package)
	}
}

func TestLatestContinuationCheckpointPicksTheNewestCaptureOfTheTask(t *testing.T) {
	now := time.Date(2026, 9, 10, 22, 0, 0, 0, time.UTC)
	records, _ := continuationRetryFixture(now)
	latest := LatestContinuationArtifact(records.Artifacts, "run-1", "task-consumer", "")
	if latest == nil || latest.AttemptID != "attempt-0" {
		t.Fatalf("latest = %+v", latest)
	}
	if excluded := LatestContinuationArtifact(records.Artifacts, "run-1", "task-consumer", "attempt-0"); excluded == nil || excluded.AttemptID != "attempt-00" {
		t.Fatalf("latest excluding attempt-0 = %+v", excluded)
	}
	if none := LatestContinuationArtifact(records.Artifacts, "run-2", "task-consumer", ""); none != nil {
		t.Fatalf("another run's snapshot was offered: %+v", none)
	}
}
