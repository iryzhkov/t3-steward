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

// Review round 2, R1: the worker queued a valid snapshot, but the attempt's
// lease ended (or the assignment was released) before the coordinator's next
// checkpoint poll. The snapshot is still imported under the authority of the
// dispatch that took it, and the replacement's first offer carries it.
func TestSupersessionBeforeTheNextImportStillHandsTheCheckpointOn(t *testing.T) {
	for _, state := range []domain.AssignmentState{domain.AssignmentReleased, domain.AssignmentUnknown} {
		t.Run(string(state), func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 9, 10, 22, 0, 0, 0, time.UTC)
			records, replacement := packageBuilderFixture(now)
			consumer := records.Tasks[1]

			// Capture: attempt-0 runs on worker-a and queues its turn-end snapshot.
			store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			superseded := domain.Attempt{ID: "attempt-0", WorkflowRunID: "run-1", TaskID: consumer.ID, Number: 1, Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 3, AssignmentID: "assignment-0", UpdatedAt: now.Add(-time.Hour)}
			claimed := domain.Assignment{ID: "assignment-0", AttemptID: superseded.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentClaimed, Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour)}
			snapshot := []byte("step 3 of 5; next: the gate\n")
			captured := now.Add(-20 * time.Minute)
			checkpoint := domain.ContinuationCheckpoint{AttemptID: superseded.ID, Sequence: 1, Turn: "turn-3", Boundary: domain.ContinuationTurnEnd,
				SHA256: resultObject("x", "x", "checkpoint", "text/markdown", snapshot).SHA256, Size: int64(len(snapshot)), OriginalSize: int64(len(snapshot)), CapturedAt: captured}
			response, data := liveContinuationUpload(t, claimed, checkpoint, snapshot, now.Add(-19*time.Minute))

			// Supersession is persisted before the upload is polled: the lease
			// ends, the attempt fails and its revision moves on.
			claimed.State = state
			superseded.Progress, superseded.Control, superseded.Revision = domain.ProgressFailed, domain.ControlStopped, 4
			if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: consumer.WorkflowID}}, Tasks: []domain.Task{consumer}, Attempts: []domain.Attempt{superseded}, Assignments: []domain.Assignment{claimed}}); err != nil {
				t.Fatal(err)
			}
			importer := CoordinatorCheckpointImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 1024, MaxTotalBytes: 4096, Now: func() time.Time { return now.Add(-18 * time.Minute) }}
			artifact, err := importer.Import(ctx, response, data)
			if err != nil {
				t.Fatalf("queued snapshot import after supersession: %v", err)
			}
			if artifact.AttemptID != superseded.ID || !artifact.CreatedAt.Equal(captured) {
				t.Fatalf("imported snapshot = %+v", artifact)
			}

			// Replacement delivery: the next attempt's first offer carries it.
			records.Attempts[0].Number = 2
			records.Attempts = append(records.Attempts, superseded)
			stored, err := store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			records.Artifacts = append(records.Artifacts, stored.Artifacts...)
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
			if latest := LatestContinuationArtifact(records.Artifacts, records.Attempts, "run-1", consumer.ID, ""); latest == nil || latest.ID != artifact.ID {
				t.Fatalf("the task's latest checkpoint = %+v", latest)
			}
		})
	}
}

// Review round 2, R5: the coordinator orders snapshots by the task's
// execution order, never by a worker's clock or by unpadded identity text.
func TestLatestContinuationNeverSelectsAnOlderSnapshot(t *testing.T) {
	now := coordinatorTestTime
	snapshot := func(attemptID, id string, at time.Time) domain.Artifact {
		return domain.Artifact{ID: id, WorkflowRunID: "run-1", TaskID: "task", AttemptID: attemptID,
			Kind: domain.ArtifactCheckpoint, Name: domain.ContinuationArtifactName, CreatedAt: at}
	}
	live := func(attemptID string, sequence int64, at time.Time) domain.Artifact {
		return snapshot(attemptID, domain.ContinuationLiveArtifactID(attemptID, sequence), at)
	}
	attempts := []domain.Attempt{
		{ID: "attempt-b", WorkflowRunID: "run-1", TaskID: "task", Number: 1},
		{ID: "attempt-a", WorkflowRunID: "run-1", TaskID: "task", Number: 2},
	}
	for _, tc := range []struct {
		name          string
		older, newer  domain.Artifact
		withoutNumber bool
	}{
		{name: "same capture time", older: live("attempt-1", 9, now), newer: live("attempt-1", 10, now)},
		{name: "worker clock stepped backward", older: live("attempt-1", 9, now), newer: live("attempt-1", 10, now.Add(-time.Second))},
		{name: "the result outranks every live snapshot", older: live("attempt-1", 10, now), newer: snapshot("attempt-1", domain.ContinuationArtifactID("attempt-1"), now.Add(-time.Minute))},
		{name: "a later attempt outranks an earlier attempt's clock", older: live("attempt-b", 3, now.Add(time.Hour)), newer: live("attempt-a", 1, now)},
		{name: "capture time orders attempts of unknown number", older: live("attempt-y", 5, now), newer: live("attempt-x", 1, now.Add(time.Second)), withoutNumber: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			known := attempts
			if tc.withoutNumber {
				known = nil
			}
			for _, artifacts := range [][]domain.Artifact{{tc.older, tc.newer}, {tc.newer, tc.older}} {
				latest := LatestContinuationArtifact(artifacts, known, "run-1", "task", "")
				if latest == nil || latest.ID != tc.newer.ID {
					t.Fatalf("latest = %+v; want %s, not %s", latest, tc.newer.ID, tc.older.ID)
				}
			}
		})
	}
}
