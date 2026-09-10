package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var fleetTestTime = time.Date(2026, 9, 10, 18, 0, 0, 0, time.UTC)

func TestAssignmentPlanCommitsBeforeEpochBoundClaim(t *testing.T) {
	store := openFleetTestStore(t)
	saveFleetAttempt(t, store, fleetAttempt("attempt-1"))
	saveFleetSnapshot(t, store, fleetSnapshot(1, "worker-epoch-1", 1, true, fleetTestTime.Add(time.Minute)))

	commit := fleetPlanCommit(1, "assignment-1", "attempt-1", "worker-epoch-1", 1)
	assignments, err := store.CommitAssignmentPlan(context.Background(), commit)
	if err != nil {
		t.Fatalf("commit assignment plan: %v", err)
	}
	if len(assignments) != 1 || assignments[0].State != domain.AssignmentOffered {
		t.Fatalf("committed assignments = %#v", assignments)
	}
	replayed, err := store.CommitAssignmentPlan(context.Background(), commit)
	if err != nil || len(replayed) != 1 || replayed[0].ID != assignments[0].ID {
		t.Fatalf("replay assignment plan: assignments=%#v error=%v", replayed, err)
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Attempts) != 1 || records.Attempts[0].AssignmentID != "assignment-1" ||
		records.Attempts[0].Revision != 2 || records.Attempts[0].Control != domain.ControlUnassigned {
		t.Fatalf("offered attempt projection = %#v", records.Attempts)
	}

	claimed, err := store.ClaimAssignment(context.Background(), fleetClaim(1, "worker-epoch-1", fleetTestTime.Add(time.Second), fleetTestTime.Add(time.Minute)))
	if err != nil {
		t.Fatalf("claim assignment: %v", err)
	}
	if claimed.State != domain.AssignmentClaimed || claimed.LeaseExpiresAt != fleetTestTime.Add(time.Minute) {
		t.Fatalf("claimed assignment = %#v", claimed)
	}
	records, err = store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if records.Attempts[0].Progress != domain.ProgressActive ||
		records.Attempts[0].Control != domain.ControlPreparing ||
		records.Attempts[0].Revision != 3 {
		t.Fatalf("claimed attempt projection = %#v", records.Attempts[0])
	}
}

func TestConcurrentAssignmentClaimsHaveOneWinner(t *testing.T) {
	store := openFleetTestStore(t)
	saveFleetAttempt(t, store, fleetAttempt("attempt-1"))
	saveFleetSnapshot(t, store, fleetSnapshot(1, "worker-epoch-1", 1, true, fleetTestTime.Add(time.Minute)))
	if _, err := store.CommitAssignmentPlan(context.Background(),
		fleetPlanCommit(1, "assignment-1", "attempt-1", "worker-epoch-1", 1)); err != nil {
		t.Fatal(err)
	}

	requests := []domain.AssignmentClaimRequest{
		fleetClaim(1, "worker-epoch-1", fleetTestTime.Add(time.Second), fleetTestTime.Add(time.Minute)),
		fleetClaim(1, "worker-epoch-1", fleetTestTime.Add(time.Second), fleetTestTime.Add(2*time.Minute)),
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(requests))
	for _, request := range requests {
		request := request
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.ClaimAssignment(context.Background(), request)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	successes, rejected := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrAssignmentClaim):
			rejected++
		default:
			t.Fatalf("unexpected claim error: %v", err)
		}
	}
	if successes != 1 || rejected != 1 {
		t.Fatalf("claim results: successes=%d rejected=%d", successes, rejected)
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Assignments) != 1 || records.Assignments[0].State != domain.AssignmentClaimed {
		t.Fatalf("durable assignments = %#v", records.Assignments)
	}
}

func TestCoordinatorRestartFencesOldClaimsAndRetainsPlan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	saveFleetAttempt(t, store, fleetAttempt("attempt-1"))
	saveFleetSnapshot(t, store, fleetSnapshot(1, "worker-epoch-1", 1, true, fleetTestTime.Add(time.Minute)))
	if _, err := store.CommitAssignmentPlan(context.Background(),
		fleetPlanCommit(1, "assignment-1", "attempt-1", "worker-epoch-1", 1)); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	epoch, err := store.AdvanceCoordinatorEpoch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if epoch != 2 {
		t.Fatalf("coordinator epoch = %d, want 2", epoch)
	}
	oldClaim := fleetClaim(1, "worker-epoch-1", fleetTestTime.Add(time.Second), fleetTestTime.Add(time.Minute))
	if _, err := store.ClaimAssignment(context.Background(), oldClaim); !errors.Is(err, ErrStaleCoordinatorEpoch) {
		t.Fatalf("old claim error = %v, want stale coordinator epoch", err)
	}

	reconnected := fleetSnapshot(2, "worker-epoch-1", 2, true, fleetTestTime.Add(2*time.Minute))
	reconnected.ObservedAt = fleetTestTime.Add(time.Second)
	saveFleetSnapshot(t, store, reconnected)
	newClaim := fleetClaim(2, "worker-epoch-1", fleetTestTime.Add(2*time.Second), fleetTestTime.Add(time.Minute))
	claimed, err := store.ClaimAssignment(context.Background(), newClaim)
	if err != nil {
		t.Fatalf("claim retained plan after restart: %v", err)
	}
	if claimed.ID != "assignment-1" {
		t.Fatalf("claimed assignment = %#v", claimed)
	}
}

func TestAssignmentPlanAndClaimRefuseUnavailableWorker(t *testing.T) {
	t.Run("disconnected planning snapshot", func(t *testing.T) {
		store := openFleetTestStore(t)
		saveFleetAttempt(t, store, fleetAttempt("attempt-1"))
		saveFleetSnapshot(t, store, fleetSnapshot(1, "worker-epoch-1", 1, false, fleetTestTime.Add(time.Minute)))
		_, err := store.CommitAssignmentPlan(context.Background(),
			fleetPlanCommit(1, "assignment-1", "attempt-1", "worker-epoch-1", 1))
		if !errors.Is(err, ErrWorkerUnavailable) {
			t.Fatalf("plan error = %v, want worker unavailable", err)
		}
		records, loadErr := store.LoadCoordinatorRecords(context.Background())
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		if len(records.Assignments) != 0 || records.Attempts[0].AssignmentID != "" {
			t.Fatalf("failed plan partially committed: %#v", records)
		}
	})

	t.Run("stale before claim", func(t *testing.T) {
		store := openFleetTestStore(t)
		saveFleetAttempt(t, store, fleetAttempt("attempt-1"))
		saveFleetSnapshot(t, store, fleetSnapshot(1, "worker-epoch-1", 1, true, fleetTestTime.Add(time.Minute)))
		if _, err := store.CommitAssignmentPlan(context.Background(),
			fleetPlanCommit(1, "assignment-1", "attempt-1", "worker-epoch-1", 1)); err != nil {
			t.Fatal(err)
		}
		request := fleetClaim(1, "worker-epoch-1", fleetTestTime.Add(2*time.Minute), fleetTestTime.Add(3*time.Minute))
		if _, err := store.ClaimAssignment(context.Background(), request); !errors.Is(err, ErrWorkerUnavailable) {
			t.Fatalf("claim error = %v, want worker unavailable", err)
		}
	})
}

func TestWorkerSnapshotsAreMonotonicAndEpochBound(t *testing.T) {
	store := openFleetTestStore(t)
	first := fleetSnapshot(1, "worker-epoch-1", 1, true, fleetTestTime.Add(time.Minute))
	saveFleetSnapshot(t, store, first)
	if err := store.SaveWorkerSnapshot(context.Background(), first); err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	stale := first
	stale.Connected = false
	if err := store.SaveWorkerSnapshot(context.Background(), stale); !errors.Is(err, ErrStaleWorkerSnapshot) {
		t.Fatalf("stale sequence error = %v", err)
	}
	wrongEpoch := fleetSnapshot(2, "worker-epoch-1", 2, true, fleetTestTime.Add(2*time.Minute))
	if err := store.SaveWorkerSnapshot(context.Background(), wrongEpoch); !errors.Is(err, ErrStaleCoordinatorEpoch) {
		t.Fatalf("future coordinator epoch error = %v", err)
	}
	restarted := fleetSnapshot(1, "worker-epoch-2", 1, true, fleetTestTime.Add(2*time.Minute))
	restarted.ObservedAt = fleetTestTime.Add(time.Second)
	saveFleetSnapshot(t, store, restarted)
	snapshots, err := store.LoadWorkerSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 || snapshots[0].WorkerEpoch != "worker-epoch-2" {
		t.Fatalf("snapshots = %#v", snapshots)
	}
}

func TestAssignmentPlanRollsBackWhenAnyAttemptIsStale(t *testing.T) {
	store := openFleetTestStore(t)
	saveFleetAttempt(t, store, fleetAttempt("attempt-1"))
	second := fleetAttempt("attempt-2")
	second.TaskID = "task-2"
	second.Revision = 2
	saveFleetAttempt(t, store, second)
	saveFleetSnapshot(t, store, fleetSnapshot(1, "worker-epoch-1", 1, true, fleetTestTime.Add(time.Minute)))

	commit := fleetPlanCommit(1, "assignment-1", "attempt-1", "worker-epoch-1", 1)
	secondItem := commit.Items[0]
	secondItem.Assignment.ID = "assignment-2"
	secondItem.Assignment.AttemptID = "attempt-2"
	secondItem.Assignment.LeaseToken = "lease-token-2"
	secondItem.Assignment.DispatchToken = "dispatch-token-2"
	secondItem.ExpectedAttemptRevision = 1
	commit.Items = append(commit.Items, secondItem)

	if _, err := store.CommitAssignmentPlan(context.Background(), commit); !errors.Is(err, ErrStaleAttemptRevision) {
		t.Fatalf("plan error = %v, want stale attempt revision", err)
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Assignments) != 0 {
		t.Fatalf("atomic plan retained assignments: %#v", records.Assignments)
	}
	for _, attempt := range records.Attempts {
		if attempt.AssignmentID != "" {
			t.Fatalf("atomic plan modified attempt: %#v", attempt)
		}
	}
}

func openFleetTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func saveFleetAttempt(t *testing.T, store *Store, attempt domain.Attempt) {
	t.Helper()
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{Attempts: []domain.Attempt{attempt}}); err != nil {
		t.Fatal(err)
	}
}

func saveFleetSnapshot(t *testing.T, store *Store, snapshot domain.WorkerSnapshot) {
	t.Helper()
	if err := store.SaveWorkerSnapshot(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
}

func fleetAttempt(id string) domain.Attempt {
	return domain.Attempt{
		ID: id, WorkflowRunID: "run-1", TaskID: "task-1", Number: 1,
		Progress: domain.ProgressReady, Control: domain.ControlUnassigned,
		Revision: 1, UpdatedAt: fleetTestTime,
	}
}

func fleetSnapshot(coordinatorEpoch int64, workerEpoch string, sequence int64, connected bool, validUntil time.Time) domain.WorkerSnapshot {
	return domain.WorkerSnapshot{
		WorkerID: "normandy", WorkerEpoch: workerEpoch,
		CoordinatorEpoch: coordinatorEpoch, Sequence: sequence, Connected: connected,
		Inventory: domain.WorkerInventory{
			ID: "normandy", AcceptBacklog: true, Health: domain.WorkerHealthReady,
			ObservedAt: fleetTestTime,
		},
		ObservedAt: fleetTestTime, ValidUntil: validUntil,
	}
}

func fleetPlanCommit(coordinatorEpoch int64, assignmentID, attemptID, workerEpoch string, workerSequence int64) domain.AssignmentPlanCommit {
	return domain.AssignmentPlanCommit{
		CoordinatorEpoch: coordinatorEpoch, CommittedAt: fleetTestTime,
		Items: []domain.AssignmentPlanItem{{
			ExpectedAttemptRevision: 1, WorkerEpoch: workerEpoch,
			WorkerSnapshotSequence: workerSequence,
			Assignment: domain.Assignment{
				ID: assignmentID, AttemptID: attemptID, WorkerID: "normandy",
				Route: domain.ProviderRoute{ProviderInstanceID: "codex", Model: "gpt-5.6-sol"},
				State: domain.AssignmentOffered, Epoch: 1,
				LeaseToken: "lease-token-1", DispatchToken: "dispatch-token-1",
			},
		}},
	}
}

func fleetClaim(coordinatorEpoch int64, workerEpoch string, claimedAt, expiresAt time.Time) domain.AssignmentClaimRequest {
	return domain.AssignmentClaimRequest{
		CoordinatorEpoch: coordinatorEpoch, WorkerID: "normandy", WorkerEpoch: workerEpoch,
		AssignmentID: "assignment-1", AssignmentEpoch: 1, LeaseToken: "lease-token-1",
		ClaimedAt: claimedAt, LeaseExpiresAt: expiresAt,
	}
}
