package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestCommitWorkerStateTransitionsAtomicallyFencesSnapshotAndAttempt(t *testing.T) {
	ctx := context.Background()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := fleetTestTime.Add(10 * time.Minute)
	snapshot := fleetSnapshot(1, "worker-epoch-1", 1, true, now.Add(time.Hour))
	snapshot.ObservedAt = now
	snapshot.ValidUntil = now.Add(time.Hour)
	if err := store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	attempt := fleetAttempt("attempt-1")
	attempt.Progress = domain.ProgressActive
	attempt.Control = domain.ControlPreparing
	attempt.AssignmentID = "assignment-1"
	attempt.Revision = 4
	attempt.UpdatedAt = fleetTestTime
	assignment := domain.Assignment{
		ID: "assignment-1", AttemptID: attempt.ID, WorkerID: snapshot.WorkerID,
		WorkerEpoch: snapshot.WorkerEpoch, State: domain.AssignmentUnknown, Epoch: 1,
		LeaseToken: "lease-1", DispatchToken: "dispatch-1",
		LeaseExpiresAt: fleetTestTime, CreatedAt: fleetTestTime, UpdatedAt: fleetTestTime,
	}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}

	nextAssignment := assignment
	nextAssignment.State = domain.AssignmentReleased
	nextAssignment.LeaseExpiresAt = time.Time{}
	nextAssignment.UpdatedAt = now
	nextAttempt := attempt
	nextAttempt.AssignmentID = ""
	nextAttempt.Progress = domain.ProgressReady
	nextAttempt.Control = domain.ControlUnassigned
	nextAttempt.Revision++
	nextAttempt.UpdatedAt = now
	transition := domain.WorkerStateTransition{
		CoordinatorEpoch: 1, WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch,
		WorkerSequence: snapshot.Sequence, TransitionedAt: now,
		ExpectedAssignment: assignment, ExpectedAttemptRevision: attempt.Revision,
		Assignment: nextAssignment, Attempt: nextAttempt, Reason: "worker-observed-absent",
	}
	tampered := transition
	tampered.ExpectedAssignment.UpdatedAt = tampered.ExpectedAssignment.UpdatedAt.Add(time.Second)
	if _, err := store.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{tampered});
		err == nil || strings.Contains(err.Error(), assignment.LeaseToken) || strings.Contains(err.Error(), assignment.DispatchToken) {
		t.Fatalf("stale assignment error exposed capability: %v", err)
	}
	applied, err := store.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{transition})
	if err != nil {
		t.Fatal(err)
	}
	if len(applied) != 1 || applied[0].State != domain.AssignmentReleased {
		t.Fatalf("applied = %#v", applied)
	}
	if _, err := store.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{transition}); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	assertNativeAuditEvent(t, store, workerStateAuditID(transition), "worker:"+snapshot.WorkerID, "released")

	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := records.Attempts[0]; got.AssignmentID != "" ||
		got.Control != domain.ControlUnassigned || got.Revision != nextAttempt.Revision {
		t.Fatalf("attempt = %#v", got)
	}

	newSnapshot := snapshot
	newSnapshot.Sequence = 2
	newSnapshot.ObservedAt = now.Add(time.Minute)
	newSnapshot.ValidUntil = now.Add(time.Hour)
	if err := store.SaveWorkerSnapshot(ctx, newSnapshot); err != nil {
		t.Fatal(err)
	}
	stale := transition
	stale.ExpectedAssignment = nextAssignment
	stale.ExpectedAttemptRevision = nextAttempt.Revision
	stale.Assignment = nextAssignment
	stale.Assignment.State = domain.AssignmentCompleted
	stale.Attempt = nextAttempt
	stale.Attempt.AssignmentID = nextAssignment.ID
	stale.Attempt.Revision++
	stale.Reason = "stale"
	if _, err := store.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{stale}); !errors.Is(err, ErrStaleWorkerStateTransition) {
		t.Fatalf("stale snapshot error = %v", err)
	}
}

func TestReleasedWorkerStateSuppressesPreviouslyPendingDispatch(t *testing.T) {
	ctx := context.Background()
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := fleetTestTime.Add(10 * time.Minute)
	snapshot := fleetSnapshot(1, "worker-epoch-1", 1, true, now.Add(time.Hour))
	snapshot.ObservedAt = now
	snapshot.ValidUntil = now.Add(time.Hour)
	if err := store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	attempt := fleetAttempt("attempt-1")
	attempt.Progress = domain.ProgressActive
	attempt.Control = domain.ControlPreparing
	attempt.AssignmentID = "assignment-1"
	attempt.Revision = 2
	assignment := domain.Assignment{
		ID: "assignment-1", AttemptID: attempt.ID, WorkerID: snapshot.WorkerID,
		WorkerEpoch: snapshot.WorkerEpoch, State: domain.AssignmentClaimed, Epoch: 1,
		LeaseToken: "lease-1", DispatchToken: "dispatch-1",
		LeaseExpiresAt: now.Add(time.Hour), CreatedAt: fleetTestTime, UpdatedAt: fleetTestTime,
	}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}
	command := fleetWorkerCommand(domain.WorkerCommandDispatch, "dispatch-command", 1, now)
	command.AssignmentID = assignment.ID
	command.AssignmentEpoch = assignment.Epoch
	if _, err := store.CommitWorkerCommands(ctx, []domain.WorkerCommand{command}); err != nil {
		t.Fatal(err)
	}

	nextAssignment := assignment
	nextAssignment.State = domain.AssignmentReleased
	nextAssignment.LeaseExpiresAt = time.Time{}
	nextAssignment.UpdatedAt = now
	nextAttempt := attempt
	nextAttempt.AssignmentID = ""
	nextAttempt.Progress = domain.ProgressReady
	nextAttempt.Control = domain.ControlUnassigned
	nextAttempt.Revision++
	nextAttempt.UpdatedAt = now
	transition := domain.WorkerStateTransition{
		CoordinatorEpoch: 1, WorkerID: snapshot.WorkerID, WorkerEpoch: snapshot.WorkerEpoch,
		WorkerSequence: snapshot.Sequence, TransitionedAt: now,
		ExpectedAssignment: assignment, ExpectedAttemptRevision: attempt.Revision,
		Assignment: nextAssignment, Attempt: nextAttempt, Reason: "worker-observed-absent",
	}
	if _, err := store.CommitWorkerStateTransitions(ctx, []domain.WorkerStateTransition{transition}); err != nil {
		t.Fatal(err)
	}
	pending, err := store.LoadPendingWorkerCommands(
		ctx, snapshot.WorkerID, snapshot.WorkerEpoch, 1, snapshot.Sequence, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending commands = %#v, want none", pending)
	}
}
