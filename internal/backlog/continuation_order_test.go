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
			if latest := LatestContinuationArtifact(records.Artifacts, records.Attempts, "run-1", consumer.ID); latest == nil || latest.ID != artifact.ID {
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
		return snapshot(attemptID, domain.ContinuationLiveArtifactID(attemptID, 1, sequence), at)
	}
	dispatched := func(attemptID string, epoch, sequence int64, at time.Time) domain.Artifact {
		return snapshot(attemptID, domain.ContinuationLiveArtifactID(attemptID, epoch, sequence), at)
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
		{name: "a later dispatch of the attempt outranks an earlier one", older: dispatched("attempt-1", 1, 5, now), newer: dispatched("attempt-1", 2, 1, now.Add(-time.Minute))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			known := attempts
			if tc.withoutNumber {
				known = nil
			}
			for _, artifacts := range [][]domain.Artifact{{tc.older, tc.newer}, {tc.newer, tc.older}} {
				latest := LatestContinuationArtifact(artifacts, known, "run-1", "task")
				if latest == nil || latest.ID != tc.newer.ID {
					t.Fatalf("latest = %+v; want %s, not %s", latest, tc.newer.ID, tc.older.ID)
				}
			}
		})
	}

	// Two attempts of one number: each attempt's latest is chosen by its own
	// sequence first, so every storage order gives the same answer.
	t.Run("equal attempt numbers in any storage order", func(t *testing.T) {
		equal := []domain.Attempt{{ID: "attempt-x", WorkflowRunID: "run-1", TaskID: "task", Number: 2}, {ID: "attempt-y", WorkflowRunID: "run-1", TaskID: "task", Number: 2}}
		x1, x2, y1 := live("attempt-x", 1, now.Add(10*time.Minute)), live("attempt-x", 2, now.Add(5*time.Minute)), live("attempt-y", 1, now.Add(7*time.Minute))
		for _, artifacts := range [][]domain.Artifact{{x1, x2, y1}, {x1, y1, x2}, {x2, x1, y1}, {x2, y1, x1}, {y1, x1, x2}, {y1, x2, x1}} {
			if latest := LatestContinuationArtifact(artifacts, equal, "run-1", "task"); latest == nil || latest.ID != y1.ID {
				t.Fatalf("order %v: latest = %+v; want %s", []string{artifacts[0].ID, artifacts[1].ID, artifacts[2].ID}, latest, y1.ID)
			}
		}
	})
}

// M16-6 Option B, test (iv): the late epoch-1 snapshot of a re-armed
// attempt is imported after the epoch-2 dispatch's own snapshot, or before
// it. Either import order, the epoch-2 snapshot is the task's latest: a late
// import never regresses the replacement.
func TestALateImportOfAnEarlierEpochNeverRegressesTheLatest(t *testing.T) {
	for _, lateFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "epoch 2 imported first", true: "epoch 1 imported first"}[lateFirst], func(t *testing.T) {
			ctx := context.Background()
			f := newRearmedAttempt(t, &sqlite.ContinuationDecision{Offered: true}, nil)
			claimed := f.current
			claimed.State = domain.AssignmentClaimed
			attempt := f.attempt
			attempt.Progress, attempt.Control, attempt.AssignmentID = domain.ProgressActive, domain.ControlRunning, claimed.ID
			if err := f.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{claimed}}); err != nil {
				t.Fatal(err)
			}
			late, lateData := f.upload(t, f.first, f.snapshot)
			resumed, resumedData := f.upload(t, claimed, []byte("step 4 of 5\n"))
			imports := []struct {
				response workerproto.ArtifactUploadResponse
				data     resultUploadOpener
			}{{resumed, resumedData}, {late, lateData}}
			if lateFirst {
				imports[0], imports[1] = imports[1], imports[0]
			}
			for _, upload := range imports {
				if _, err := f.importer(t, f.store).Import(ctx, upload.response, upload.data); err != nil {
					t.Fatalf("import %s: %v", upload.response.Manifest.ID, err)
				}
			}
			stored, err := f.store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			want := domain.ContinuationLiveArtifactID(attempt.ID, 2, 1)
			if len(stored.Artifacts) != 2 {
				t.Fatalf("artifacts = %+v", stored.Artifacts)
			}
			for _, artifacts := range [][]domain.Artifact{stored.Artifacts, {stored.Artifacts[1], stored.Artifacts[0]}} {
				if latest := LatestContinuationArtifact(artifacts, stored.Attempts, attempt.WorkflowRunID, attempt.TaskID); latest == nil || latest.ID != want {
					t.Fatalf("latest = %+v; want the epoch-2 dispatch's %s", latest, want)
				}
			}
		})
	}
}

// Self-review of round 2: a lost lease releases the assignment and the
// planner offers the same attempt again at the next assignment epoch. The
// snapshot the first dispatch queued is imported after the release, and the
// second dispatch's first package carries the attempt's own checkpoint.
func TestAnAttemptOfferedAgainResumesFromItsEarlierDispatch(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 10, 22, 0, 0, 0, time.UTC)
	records, reoffer := packageBuilderFixture(now)
	consumer := records.Tasks[1]
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// The first dispatch ran on worker-a and queued a snapshot; its lease was
	// lost and the assignment released before the snapshot was polled.
	attempt := records.Attempts[0]
	attempt.Progress, attempt.Control, attempt.AssignmentID = domain.ProgressReady, domain.ControlUnassigned, ""
	first := domain.Assignment{ID: reoffer.ID, AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentReleased, Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Minute)}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: records.WorkflowRuns, Tasks: []domain.Task{consumer}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{first}}); err != nil {
		t.Fatal(err)
	}
	snapshot := []byte("step 3 of 5; next: the gate\n")
	captured := now.Add(-20 * time.Minute)
	checkpoint := domain.ContinuationCheckpoint{AttemptID: attempt.ID, Sequence: 1, Turn: "turn-3", Boundary: domain.ContinuationTurnEnd,
		SHA256: resultObject("x", "x", "checkpoint", "text/markdown", snapshot).SHA256, Size: int64(len(snapshot)), OriginalSize: int64(len(snapshot)), CapturedAt: captured}
	response, data := liveContinuationUpload(t, first, checkpoint, snapshot, now.Add(-19*time.Minute))
	importer := CoordinatorCheckpointImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 1024, MaxTotalBytes: 4096, Now: func() time.Time { return now }}
	artifact, err := importer.Import(ctx, response, data)
	if err != nil {
		t.Fatalf("the first dispatch's snapshot was not imported after the release: %v", err)
	}

	// The planner offers the same attempt again at epoch 2.
	reoffer.Epoch = 2
	records.Assignments[0] = reoffer
	stored, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	records.Artifacts = append(records.Artifacts, stored.Artifacts...)
	builder := packageBuilder(t, records)
	builder.WorkerCapabilities = map[string][]string{"normandy": workerproto.SupportedPackageCapabilities()}
	offer, err := builder.BuildAssignmentOffer(ctx, reoffer, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	pkg := offer.Package.Package
	if pkg.Continuation == nil || pkg.Continuation.AttemptID != attempt.ID || !pkg.Continuation.CapturedAt.Equal(captured) {
		t.Fatalf("the attempt offered again did not receive its own checkpoint: %+v", pkg.Continuation)
	}
	var delivered *workerproto.ArtifactObject
	for index := range pkg.StaticInputs {
		if pkg.StaticInputs[index].Path == workerproto.ContinuationInputPath {
			delivered = &pkg.StaticInputs[index]
		}
	}
	if delivered == nil || delivered.ID != artifact.ID {
		t.Fatalf("static inputs = %+v", pkg.StaticInputs)
	}

	// The second dispatch starts its sequence again at 1. Its snapshot is its
	// own artifact, never a conflict with the first dispatch's, and it is the
	// attempt's latest.
	second := first
	second.WorkerID, second.WorkerEpoch, second.State, second.Epoch = "worker-b", "worker-epoch-1", domain.AssignmentClaimed, 2
	attempt.Progress, attempt.Control, attempt.AssignmentID = domain.ProgressActive, domain.ControlRunning, second.ID
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: records.WorkflowRuns, Tasks: []domain.Task{consumer}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{second}}); err != nil {
		t.Fatal(err)
	}
	resumed := []byte("step 4 of 5\n")
	checkpoint.SHA256, checkpoint.Size, checkpoint.OriginalSize, checkpoint.CapturedAt = resultObject("x", "x", "checkpoint", "text/markdown", resumed).SHA256, int64(len(resumed)), int64(len(resumed)), now
	response, data = liveContinuationUpload(t, second, checkpoint, resumed, now.Add(time.Second))
	later, err := importer.Import(ctx, response, data)
	if err != nil || later.ID == artifact.ID {
		t.Fatalf("the second dispatch's first snapshot = %+v, %v", later, err)
	}
	stored, err = store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if latest := LatestContinuationArtifact(stored.Artifacts, stored.Attempts, "run-1", consumer.ID); latest == nil || latest.ID != later.ID {
		t.Fatalf("latest = %+v; want the second dispatch's %s", latest, later.ID)
	}
}
