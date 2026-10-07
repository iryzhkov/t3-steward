package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// rearmedDispatch is attempt-1, whose assignment was offered to worker-a at
// epoch 1 with its continuation decision frozen, then released and offered
// again at epoch 2 to worker-b.
type rearmedDispatch struct {
	store   *Store
	now     time.Time
	attempt domain.Attempt
	first   domain.Assignment
	current domain.Assignment
}

// newRearmedDispatch builds the fixture. freeze, when set, is the decision
// frozen for the first dispatch; nil leaves epoch 1 without a V39 row.
func newRearmedDispatch(t *testing.T, freeze *ContinuationDecision) rearmedDispatch {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	run := domain.WorkflowRun{ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressActive, Revision: 1, CreatedAt: now, UpdatedAt: now}
	task := domain.Task{ID: "task-1", WorkflowID: run.WorkflowID, Name: "task"}
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: run.ID, TaskID: task.ID, Number: 1, Progress: domain.ProgressReady, Control: domain.ControlUnassigned, Revision: 2, AssignmentID: "assignment-1", UpdatedAt: now}
	first := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "worker-a", WorkerEpoch: "worker-epoch-a", State: domain.AssignmentOffered, Epoch: 1, LeaseToken: "lease-1", DispatchToken: "dispatch-1", CreatedAt: now, UpdatedAt: now}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{first}}); err != nil {
		t.Fatal(err)
	}
	if freeze != nil {
		identity := workerproto.ExecutionIdentity{WorkflowID: run.WorkflowID, WorkflowRunID: run.ID, TaskID: task.ID, AttemptID: attempt.ID, AttemptRevision: attempt.Revision,
			AssignmentID: first.ID, AssignmentEpoch: first.Epoch, DispatchToken: first.DispatchToken}
		if _, err := store.FreezeAssignmentContinuation(ctx, 1, "coordinator", first, identity, *freeze); err != nil {
			t.Fatal(err)
		}
	}
	current := first
	current.WorkerID, current.WorkerEpoch, current.Epoch, current.LeaseToken, current.DispatchToken = "worker-b", "worker-epoch-b", 2, "lease-2", "dispatch-2"
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Assignments: []domain.Assignment{current}}); err != nil {
		t.Fatal(err)
	}
	return rearmedDispatch{store: store, now: now, attempt: attempt, first: first, current: current}
}

// publication is worker-a's late live snapshot of the first dispatch.
func (f rearmedDispatch) publication() domain.ArtifactPublication {
	sum := sha256.Sum256([]byte("step 1\n"))
	hash := hex.EncodeToString(sum[:])
	return domain.ArtifactPublication{
		CoordinatorEpoch: 1, WorkerID: f.first.WorkerID, WorkerEpoch: f.first.WorkerEpoch,
		AssignmentID: f.first.ID, AssignmentEpoch: f.first.Epoch, AttemptRevision: f.attempt.Revision, LiveContinuation: true,
		Artifact: domain.Artifact{
			ID: domain.ContinuationLiveArtifactID(f.attempt.ID, f.first.Epoch, 1), WorkflowRunID: f.attempt.WorkflowRunID, TaskID: f.attempt.TaskID, AttemptID: f.attempt.ID,
			Kind: domain.ArtifactCheckpoint, Name: domain.ContinuationArtifactName, MediaType: "text/markdown", Size: 7, SHA256: hash,
			StoragePath: path.Join("objects", hash[:2], hash), Producer: "worker:" + f.first.WorkerID, CreatedAt: f.now,
		},
	}
}

// main's registry ends at V37; this unit adds V39 after it. A database at
// V37 migrates to V39 and gains the continuation table, and migrating again
// changes nothing.
func TestAssignmentContinuationV39MigratesAV37Database(t *testing.T) {
	s, err := open(filepath.Join(t.TempDir(), "state.db"), true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.migrateThrough(37); err != nil {
		t.Fatal(err)
	}
	if version := schemaVersionOf(t, s); version != 37 {
		t.Fatalf("fixture schema version = %d, want 37", version)
	}
	for pass := 0; pass < 2; pass++ {
		if err := s.Migrate(); err != nil {
			t.Fatal(err)
		}
		if version := schemaVersionOf(t, s); version != 39 || currentSchemaVersion != 39 {
			t.Fatalf("schema version = %d (current %d), want 39", version, currentSchemaVersion)
		}
	}
	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM coordinator_assignment_continuations").Scan(&count); err != nil || count != 0 {
		t.Fatalf("continuation table = %d rows, %v", count, err)
	}
}

// M16-6 Option B: the V39 row is the durable record of every dispatch a
// worker could have claimed. It is read back by (assignment, epoch); an
// unknown row is not an error, and a corrupt one is never an acceptance.
func TestAssignmentContinuationBindingReadsTheFrozenDispatch(t *testing.T) {
	ctx := context.Background()
	f := newRearmedDispatch(t, &ContinuationDecision{Offered: true})
	dispatch, found, err := f.store.AssignmentContinuationBinding(ctx, f.first.ID, 1)
	if err != nil || !found {
		t.Fatalf("frozen dispatch = %v, %v", found, err)
	}
	if dispatch.Assignment.WorkerID != "worker-a" || dispatch.Assignment.WorkerEpoch != "worker-epoch-a" || dispatch.Assignment.Epoch != 1 ||
		dispatch.Assignment.AttemptID != f.attempt.ID || dispatch.Identity.DispatchToken != "dispatch-1" || !dispatch.Offered {
		t.Fatalf("frozen dispatch = %+v", dispatch)
	}
	for _, epoch := range []int64{0, -1, 2, 3} {
		if _, found, err := f.store.AssignmentContinuationBinding(ctx, f.first.ID, epoch); found || err != nil {
			t.Fatalf("epoch %d: found %t, %v", epoch, found, err)
		}
	}
	if _, found, err := f.store.AssignmentContinuationBinding(ctx, "assignment-9", 1); found || err != nil {
		t.Fatalf("unknown assignment: found %t, %v", found, err)
	}

	for name, corrupt := range map[string]string{
		"binding is not JSON":              `UPDATE coordinator_assignment_continuations SET binding = '{' WHERE assignment_id = 'assignment-1'`,
		"binding names another dispatch":   `UPDATE coordinator_assignment_continuations SET binding = json_set(binding, '$.Assignment.epoch', 2) WHERE assignment_id = 'assignment-1'`,
		"identity names another attempt":   `UPDATE coordinator_assignment_continuations SET binding = json_set(binding, '$.Identity.attemptId', 'attempt-9') WHERE assignment_id = 'assignment-1'`,
		"decision is not a decision":       `UPDATE coordinator_assignment_continuations SET decision = '[]' WHERE assignment_id = 'assignment-1'`,
		"binding names another assignment": `UPDATE coordinator_assignment_continuations SET binding = json_set(binding, '$.Assignment.id', 'assignment-9') WHERE assignment_id = 'assignment-1'`,
	} {
		t.Run(name, func(t *testing.T) {
			f := newRearmedDispatch(t, &ContinuationDecision{Offered: true})
			if _, err := f.store.db.ExecContext(ctx, corrupt); err != nil {
				t.Fatal(err)
			}
			if dispatch, found, err := f.store.AssignmentContinuationBinding(ctx, f.first.ID, 1); err == nil {
				t.Fatalf("a corrupt row was read as %+v (found %t)", dispatch, found)
			}
			if _, err := f.store.CommitArtifactPublication(ctx, f.publication()); err == nil {
				t.Fatal("a corrupt row authenticated a publication")
			}
		})
	}
}

// M16-6 Option B: the publication fence authenticates a live snapshot of an
// earlier epoch against that epoch's V39 row inside the publication
// transaction, and refuses it when the row is missing, was never offered the
// capability, or names another worker process, attempt or epoch.
func TestTheHistoricalPublicationFenceUsesTheFrozenDispatch(t *testing.T) {
	ctx := context.Background()
	t.Run("accepted", func(t *testing.T) {
		f := newRearmedDispatch(t, &ContinuationDecision{Offered: true})
		published, err := f.store.CommitArtifactPublication(ctx, f.publication())
		if err != nil || published.ID != f.publication().Artifact.ID {
			t.Fatalf("late snapshot of epoch 1 = %+v, %v", published, err)
		}
		if replay, err := f.store.CommitArtifactPublication(ctx, f.publication()); err != nil || replay != published {
			t.Fatalf("replay = %+v, %v", replay, err)
		}
	})
	for _, tc := range []struct {
		name   string
		freeze *ContinuationDecision
		mutate func(*rearmedDispatch, *domain.ArtifactPublication)
	}{
		{name: "no row for the epoch"},
		{name: "the capability was never offered", freeze: &ContinuationDecision{}},
		{name: "the current worker under the old epoch", mutate: func(f *rearmedDispatch, p *domain.ArtifactPublication) {
			p.WorkerID, p.WorkerEpoch = f.current.WorkerID, f.current.WorkerEpoch
		}},
		{name: "a re-enrolled worker process", mutate: func(_ *rearmedDispatch, p *domain.ArtifactPublication) { p.WorkerEpoch = "worker-epoch-a2" }},
		{name: "an epoch above the current one", mutate: func(f *rearmedDispatch, p *domain.ArtifactPublication) {
			p.AssignmentEpoch = 3
			p.Artifact.ID = domain.ContinuationLiveArtifactID(f.attempt.ID, 3, 1)
		}},
		{name: "a snapshot labelled with another epoch", mutate: func(f *rearmedDispatch, p *domain.ArtifactPublication) {
			p.Artifact.ID = domain.ContinuationLiveArtifactID(f.attempt.ID, 2, 1)
		}},
		{name: "the attempt moved to another assignment", mutate: func(f *rearmedDispatch, _ *domain.ArtifactPublication) {
			moved := f.attempt
			moved.AssignmentID = "assignment-9"
			if err := f.store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{moved}}); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "the assignment now runs another attempt", mutate: func(f *rearmedDispatch, _ *domain.ArtifactPublication) {
			other := domain.Attempt{ID: "attempt-2", WorkflowRunID: f.attempt.WorkflowRunID, TaskID: f.attempt.TaskID, Number: 2, Progress: domain.ProgressReady, Control: domain.ControlUnassigned, Revision: 1, AssignmentID: f.current.ID, UpdatedAt: f.now}
			reused := f.current
			reused.AttemptID = other.ID
			if err := f.store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{other}, Assignments: []domain.Assignment{reused}}); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			freeze := tc.freeze
			if freeze == nil && tc.name != "no row for the epoch" {
				freeze = &ContinuationDecision{Offered: true}
			}
			f := newRearmedDispatch(t, freeze)
			publication := f.publication()
			if tc.mutate != nil {
				tc.mutate(&f, &publication)
			}
			if published, err := f.store.CommitArtifactPublication(ctx, publication); !errors.Is(err, ErrStaleArtifactPublication) {
				t.Fatalf("publication = %+v, %v; want a stale refusal", published, err)
			}
			records, err := f.store.LoadCoordinatorRecords(ctx)
			if err != nil || len(records.Artifacts) != 0 {
				t.Fatalf("a refused publication was kept: %+v, %v", records.Artifacts, err)
			}
		})
	}
	// Self-review, lens 5: the fence is the authority, so it refuses on its
	// own what the importer refuses: a live snapshot labelled with an epoch
	// other than its dispatch's, which would outrank the replacement's, and a
	// snapshot of a supervision activation.
	t.Run("current epoch, snapshot labelled with a higher epoch", func(t *testing.T) {
		f := newRearmedDispatch(t, nil)
		claimed := f.current
		claimed.State = domain.AssignmentClaimed
		if err := f.store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Assignments: []domain.Assignment{claimed}}); err != nil {
			t.Fatal(err)
		}
		publication := f.publication()
		publication.WorkerID, publication.WorkerEpoch, publication.AssignmentEpoch = claimed.WorkerID, claimed.WorkerEpoch, 2
		publication.Artifact.ID = domain.ContinuationLiveArtifactID(f.attempt.ID, 99, 1)
		if published, err := f.store.CommitArtifactPublication(ctx, publication); !errors.Is(err, ErrStaleArtifactPublication) {
			t.Fatalf("the epoch-2 dispatch published a snapshot labelled epoch 99: %+v, %v", published, err)
		}
	})
	for _, current := range []bool{false, true} {
		t.Run(map[bool]string{false: "earlier epoch", true: "current epoch"}[current]+", supervision activation", func(t *testing.T) {
			f := newRearmedDispatch(t, &ContinuationDecision{Offered: true})
			publication := f.publication()
			if current {
				claimed := f.current
				claimed.State = domain.AssignmentClaimed
				if err := f.store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Assignments: []domain.Assignment{claimed}}); err != nil {
					t.Fatal(err)
				}
				publication.WorkerID, publication.WorkerEpoch, publication.AssignmentEpoch = claimed.WorkerID, claimed.WorkerEpoch, 2
				publication.Artifact.ID = domain.ContinuationLiveArtifactID(f.attempt.ID, 2, 1)
			}
			activation := f.attempt
			activation.SupervisionActivationID = "activation-1"
			if err := f.store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{activation}}); err != nil {
				t.Fatal(err)
			}
			if published, err := f.store.CommitArtifactPublication(ctx, publication); !errors.Is(err, ErrStaleArtifactPublication) {
				t.Fatalf("a supervision activation's snapshot was published: %+v, %v", published, err)
			}
		})
	}
	// The equal-epoch path is unchanged: the current dispatch's own worker
	// publishes against the current row, with or without a V39 row.
	t.Run("current epoch", func(t *testing.T) {
		f := newRearmedDispatch(t, nil)
		claimed := f.current
		claimed.State = domain.AssignmentClaimed
		if err := f.store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Assignments: []domain.Assignment{claimed}}); err != nil {
			t.Fatal(err)
		}
		publication := f.publication()
		publication.WorkerID, publication.WorkerEpoch, publication.AssignmentEpoch = claimed.WorkerID, claimed.WorkerEpoch, 2
		publication.Artifact.ID = domain.ContinuationLiveArtifactID(f.attempt.ID, 2, 1)
		if _, err := f.store.CommitArtifactPublication(ctx, publication); err != nil {
			t.Fatal(err)
		}
	})
}
