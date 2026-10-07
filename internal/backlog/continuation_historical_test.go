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

// rearmedAttempt is attempt-1 of the consumer task. Its first dispatch, on
// worker-a at epoch 1, was offered with its continuation decision frozen in
// V39, then lost its lease and was released; planning then offered the same
// attempt again by re-arming the assignment at epoch 2 on normandy. Worker-a
// still holds the snapshot its dispatch took.
type rearmedAttempt struct {
	path     string
	store    *sqlite.Store
	now      time.Time
	records  sqlite.CoordinatorRecords
	attempt  domain.Attempt
	first    domain.Assignment
	current  domain.Assignment
	snapshot []byte
}

// newRearmedAttempt builds the fixture. decision is what the first offer
// froze, nil for no V39 row at all; frozen, when set, changes the first
// dispatch before its row is frozen.
func newRearmedAttempt(t *testing.T, decision *sqlite.ContinuationDecision, frozen func(*domain.Assignment)) rearmedAttempt {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 9, 10, 22, 0, 0, 0, time.UTC)
	records, reoffer := packageBuilderFixture(now)
	consumer := records.Tasks[1]
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	// The first offer, at epoch 1 on worker-a, freezes its decision.
	attempt := records.Attempts[0]
	first := domain.Assignment{ID: reoffer.ID, AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1", State: domain.AssignmentOffered, Epoch: 1, LeaseToken: "lease-1", DispatchToken: "dispatch-1", CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour)}
	if frozen != nil {
		frozen(&first)
	}
	attempt.AssignmentID = first.ID
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: records.WorkflowRuns, Tasks: []domain.Task{consumer}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{first}}); err != nil {
		t.Fatal(err)
	}
	if decision != nil {
		identity := workerproto.ExecutionIdentity{WorkflowID: consumer.WorkflowID, WorkflowRunID: attempt.WorkflowRunID, TaskID: attempt.TaskID, AttemptID: attempt.ID, AttemptRevision: attempt.Revision,
			AssignmentID: first.ID, AssignmentEpoch: first.Epoch, DispatchToken: first.DispatchToken}
		if _, err := store.FreezeAssignmentContinuation(ctx, 1, "coordinator", first, identity, *decision); err != nil {
			t.Fatal(err)
		}
	}

	// The lease is lost and planning offers the attempt again at epoch 2.
	current := reoffer
	current.Epoch, current.State = 2, domain.AssignmentOffered
	attempt.Progress, attempt.Control = domain.ProgressReady, domain.ControlUnassigned
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{current}}); err != nil {
		t.Fatal(err)
	}
	records.Attempts[0], records.Assignments[0] = attempt, current
	return rearmedAttempt{path: path, store: store, now: now, records: records, attempt: attempt, first: first, current: current, snapshot: []byte("step 3 of 5; next: the gate\n")}
}

// upload is the live snapshot the given dispatch of the attempt took.
func (f rearmedAttempt) upload(t *testing.T, dispatch domain.Assignment, snapshot []byte) (workerproto.ArtifactUploadResponse, resultUploadOpener) {
	t.Helper()
	checkpoint := domain.ContinuationCheckpoint{AttemptID: f.attempt.ID, Sequence: 1, Turn: "turn-3", Boundary: domain.ContinuationTurnEnd,
		SHA256: resultObject("x", "x", "checkpoint", "text/markdown", snapshot).SHA256, Size: int64(len(snapshot)), OriginalSize: int64(len(snapshot)), CapturedAt: f.now.Add(-20 * time.Minute)}
	return liveContinuationUpload(t, dispatch, checkpoint, snapshot, f.now.Add(-19*time.Minute))
}

func (f rearmedAttempt) importer(t *testing.T, store *sqlite.Store) CoordinatorCheckpointImporter {
	return CoordinatorCheckpointImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: CoordinatorArtifactStore{Root: filepath.Join(filepath.Dir(f.path), "artifacts"), Catalog: store}, MaxArtifactBytes: 1024, MaxTotalBytes: 4096, Now: func() time.Time { return f.now }}
}

// M16-6 Option B, test (i) at the importer and (iii): a snapshot of epoch 1
// polled only after the assignment was re-armed at epoch 2 is authenticated
// against the epoch-1 dispatch's V39 row, imported and published, and the
// epoch-2 dispatch's first offer carries it. Replaying it, before and after a
// coordinator restart, is the same artifact and adds nothing.
func TestALateSnapshotOfAReArmedDispatchIsImportedUnderItsFrozenDispatch(t *testing.T) {
	ctx := context.Background()
	f := newRearmedAttempt(t, &sqlite.ContinuationDecision{Offered: true}, nil)
	response, data := f.upload(t, f.first, f.snapshot)
	artifact, err := f.importer(t, f.store).Import(ctx, response, data)
	if err != nil {
		t.Fatalf("the late snapshot of the re-armed dispatch was refused: %v", err)
	}
	if artifact.ID != domain.ContinuationLiveArtifactID(f.attempt.ID, 1, 1) || artifact.AttemptID != f.attempt.ID || artifact.Producer != "worker:worker-a" {
		t.Fatalf("imported snapshot = %+v", artifact)
	}
	if replay, err := f.importer(t, f.store).Import(ctx, response, data); err != nil || replay != artifact {
		t.Fatalf("replay = %+v, %v", replay, err)
	}

	// A coordinator restart reopens the same database.
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sqlite.OpenMigrated(f.path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if replay, err := f.importer(t, reopened).Import(ctx, response, data); err != nil || replay != artifact {
		t.Fatalf("replay after restart = %+v, %v", replay, err)
	}
	stored, err := reopened.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Artifacts) != 1 || stored.Artifacts[0].ID != artifact.ID {
		t.Fatalf("artifacts after replays = %+v", stored.Artifacts)
	}
	events, err := reopened.LoadAuditEvents(ctx, f.attempt.WorkflowRunID)
	if err != nil {
		t.Fatal(err)
	}
	published := 0
	for _, event := range events {
		if event.Kind == "artifact-published" && event.TargetID == artifact.ID {
			published++
		}
	}
	if published != 1 {
		t.Fatalf("%d publication events for one snapshot", published)
	}

	// The epoch-2 dispatch's first offer, and the task's latest, are it.
	records := f.records
	records.Artifacts = append(records.Artifacts, stored.Artifacts...)
	builder := packageBuilder(t, records)
	builder.WorkerCapabilities = map[string][]string{"normandy": workerproto.SupportedPackageCapabilities()}
	offer, err := builder.BuildAssignmentOffer(ctx, f.current, f.now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if pkg := offer.Package.Package; pkg.Continuation == nil || pkg.Continuation.AttemptID != f.attempt.ID || pkg.Continuation.Size != int64(len(f.snapshot)) {
		t.Fatalf("the re-armed dispatch did not receive the late snapshot: %+v", pkg.Continuation)
	}
	if latest := LatestContinuationArtifact(stored.Artifacts, stored.Attempts, f.attempt.WorkflowRunID, f.attempt.TaskID); latest == nil || latest.ID != artifact.ID {
		t.Fatalf("the task's latest checkpoint = %+v", latest)
	}
}

// M16-6 Option B, test (ii) and the adversarial list: an upload for an
// earlier epoch is authenticated only by that epoch's V39 row. A missing row,
// a row that never offered the capability, or one naming another worker,
// worker process, attempt or epoch is refused for good and audited, and
// nothing is kept.
func TestALateSnapshotWithoutItsFrozenDispatchIsRefusedForGood(t *testing.T) {
	ctx := context.Background()
	offered := &sqlite.ContinuationDecision{Offered: true}
	for _, tc := range []struct {
		name     string
		decision *sqlite.ContinuationDecision
		frozen   func(*domain.Assignment)
		// uploader turns the first dispatch into the identity the upload
		// claims; after changes the store once the attempt was re-armed.
		uploader func(f rearmedAttempt) domain.Assignment
		after    func(t *testing.T, f rearmedAttempt)
	}{
		{name: "no V39 row for the epoch"},
		{name: "the capability was never offered", decision: &sqlite.ContinuationDecision{}},
		{name: "the row names another worker", decision: offered, frozen: func(a *domain.Assignment) { a.WorkerID = "worker-b" }},
		{name: "a re-enrolled worker process", decision: offered, uploader: func(f rearmedAttempt) domain.Assignment {
			upload := f.first
			upload.WorkerEpoch = "worker-epoch-2"
			return upload
		}},
		{name: "the current worker under the earlier epoch", decision: offered, uploader: func(f rearmedAttempt) domain.Assignment {
			upload := f.first
			upload.WorkerID, upload.WorkerEpoch = f.current.WorkerID, f.current.WorkerEpoch
			return upload
		}},
		{name: "an epoch above the current one", decision: offered, uploader: func(f rearmedAttempt) domain.Assignment {
			upload := f.first
			upload.Epoch = 3
			return upload
		}},
		{name: "the attempt moved to another assignment", decision: offered, after: func(t *testing.T, f rearmedAttempt) {
			moved := f.attempt
			moved.AssignmentID = "assignment-9"
			if err := f.store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{Attempts: []domain.Attempt{moved}}); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "the assignment now runs another attempt", decision: offered, after: func(t *testing.T, f rearmedAttempt) {
			other := domain.Attempt{ID: "attempt-2", WorkflowRunID: f.attempt.WorkflowRunID, TaskID: f.attempt.TaskID, Number: 2, Progress: domain.ProgressReady, Control: domain.ControlUnassigned, Revision: 1, AssignmentID: f.current.ID, UpdatedAt: f.now}
			reused := f.current
			reused.AttemptID = other.ID
			if err := f.store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{Attempts: []domain.Attempt{other}, Assignments: []domain.Assignment{reused}}); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRearmedAttempt(t, tc.decision, tc.frozen)
			if tc.after != nil {
				tc.after(t, f)
			}
			upload := f.first
			if tc.frozen != nil {
				// The upload comes from worker-a whatever the row says.
				upload.WorkerID, upload.WorkerEpoch = "worker-a", "worker-epoch-1"
			}
			if tc.uploader != nil {
				upload = tc.uploader(f)
			}
			response, data := f.upload(t, upload, f.snapshot)
			artifact, err := f.importer(t, f.store).Import(ctx, response, data)
			if !errors.Is(err, ErrCheckpointImportRejected) {
				t.Fatalf("import = %+v, %v; want a final refusal", artifact, err)
			}
			stored, err := f.store.LoadCoordinatorRecords(ctx)
			if err != nil || len(stored.Artifacts) != 0 {
				t.Fatalf("a refused snapshot was kept: %+v, %v", stored.Artifacts, err)
			}
			events, err := f.store.LoadAuditEvents(ctx, f.attempt.WorkflowRunID)
			if err != nil {
				t.Fatal(err)
			}
			audited := false
			for _, event := range events {
				audited = audited || event.Kind == "checkpoint-import-rejected" && event.ID == sqlite.CheckpointImportRejectionEventID(response.Manifest.ID)
			}
			if !audited {
				t.Fatalf("the refusal was not audited: %+v", events)
			}
		})
	}

	// Epoch 0 or below never reaches the binding: the manifest itself is
	// invalid, and nothing is imported.
	for _, epoch := range []int64{0, -1} {
		f := newRearmedAttempt(t, offered, nil)
		response, data := f.upload(t, f.first, f.snapshot)
		response.Manifest.AssignmentEpoch = epoch
		response.Custody = resultCustody(t, response.Manifest, "coordinator")
		if artifact, err := f.importer(t, f.store).Import(ctx, response, data); err == nil {
			t.Fatalf("epoch %d imported %+v", epoch, artifact)
		}
	}
}

// Adversarial list, wrong assignment: worker-a holds a genuine epoch-1
// dispatch of another task's attempt, frozen and re-armed like the fixture's,
// and uses it to upload a snapshot under the fixture attempt's identity. The
// row authenticates the dispatch, not the content, so the identity check
// refuses it for good.
func TestALateSnapshotUnderAnotherDispatchOfTheSameWorkerIsRefused(t *testing.T) {
	ctx := context.Background()
	f := newRearmedAttempt(t, &sqlite.ContinuationDecision{Offered: true}, nil)
	other := f.first
	other.AttemptID = "attempt-other"
	other.ID, other.LeaseToken, other.DispatchToken = "assignment-other", "lease-other", "dispatch-other"
	otherAttempt := domain.Attempt{ID: other.AttemptID, WorkflowRunID: f.attempt.WorkflowRunID, TaskID: f.attempt.TaskID, Number: 7, Progress: domain.ProgressReady, Control: domain.ControlUnassigned, Revision: 1, AssignmentID: other.ID, UpdatedAt: f.now}
	if err := f.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{otherAttempt}, Assignments: []domain.Assignment{other}}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.FreezeAssignmentContinuation(ctx, 1, "coordinator", other, workerproto.ExecutionIdentity{WorkflowID: f.records.Tasks[1].WorkflowID, WorkflowRunID: otherAttempt.WorkflowRunID, TaskID: otherAttempt.TaskID,
		AttemptID: otherAttempt.ID, AttemptRevision: otherAttempt.Revision, AssignmentID: other.ID, AssignmentEpoch: 1, DispatchToken: other.DispatchToken}, sqlite.ContinuationDecision{Offered: true}); err != nil {
		t.Fatal(err)
	}
	rearmed := other
	rearmed.Epoch, rearmed.WorkerID = 2, "normandy"
	if err := f.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Assignments: []domain.Assignment{rearmed}}); err != nil {
		t.Fatal(err)
	}
	// The snapshot names the fixture's attempt; the manifest names the other
	// dispatch.
	response, data := f.upload(t, f.first, f.snapshot)
	response.Manifest.AssignmentID = other.ID
	response.Custody = resultCustody(t, response.Manifest, "coordinator")
	if artifact, err := f.importer(t, f.store).Import(ctx, response, data); !errors.Is(err, ErrCheckpointImportRejected) {
		t.Fatalf("import = %+v, %v; want a final refusal", artifact, err)
	}
	stored, err := f.store.LoadCoordinatorRecords(ctx)
	if err != nil || len(stored.Artifacts) != 0 {
		t.Fatalf("a refused snapshot was kept: %+v, %v", stored.Artifacts, err)
	}
}
