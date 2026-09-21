package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestCapacityReviewStaleExplicitZeroFailsClosed(t *testing.T) {
	store := openFleetTestStore(t)
	snapshot := fleetSnapshot(1, "worker-epoch-1", 1, true, fleetTestTime.Add(time.Second))
	snapshot.Inventory.Allocatable.ExecutorSlots = 0
	saveFleetSnapshot(t, store, snapshot)

	available, err := store.ExecutorSlotAvailable(context.Background(), snapshot.WorkerID, fleetTestTime.Add(2*time.Second))
	if err == nil || !errors.Is(err, ErrExecutorCapacityEvidence) || available {
		t.Fatalf("stale explicit-zero snapshot returned available=%v err=%v; want fail-closed evidence error", available, err)
	}
}

func TestCapacityReviewOwnershipStates(t *testing.T) {
	attempt := domain.Attempt{Control: domain.ControlRunning}
	for _, tc := range []struct {
		state domain.AssignmentState
		owns  bool
	}{
		{domain.AssignmentOffered, true},
		{domain.AssignmentClaimed, true},
		{domain.AssignmentUnknown, true},
		{domain.AssignmentCompleted, false},
		{domain.AssignmentReleased, false},
	} {
		if got := domain.AssignmentOwnsExecutorCapacity(attempt, domain.Assignment{State: tc.state}); got != tc.owns {
			t.Fatalf("state %q owns=%v, want %v", tc.state, got, tc.owns)
		}
	}
	attempt.Control = domain.ControlWaitingExternal
	if domain.AssignmentOwnsExecutorCapacity(attempt, domain.Assignment{State: domain.AssignmentClaimed}) {
		t.Fatal("parked claimed assignment retained executor capacity")
	}
	attempt.Control = domain.ControlResuming
	if !domain.AssignmentOwnsExecutorCapacity(attempt, domain.Assignment{State: domain.AssignmentClaimed}) {
		t.Fatal("resuming claimed assignment did not reacquire executor capacity")
	}
}

func TestCapacityReviewSimultaneousWakesAcquireAtMostOneSlot(t *testing.T) {
	ctx := context.Background()
	store, first, now := taskWaitFixture(t)
	snapshot := domain.WorkerSnapshot{
		WorkerID: "worker", WorkerEpoch: "worker-epoch-1", CoordinatorEpoch: 1, Sequence: 2,
		Connected: true, ObservedAt: now.Add(time.Second), ValidUntil: now.Add(time.Hour),
		Inventory: domain.WorkerInventory{ID: "worker", AcceptBacklog: true,
			Health: domain.WorkerHealthReady, ObservedAt: now.Add(time.Second),
			Allocatable: domain.AllocatableCapacity{ExecutorSlots: 1}},
	}
	if err := store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}

	second := first
	second.ID = "attempt-second-wake"
	second.Number = 2
	second.AssignmentID = "assignment-second-wake"
	second.Revision = 1
	second.Progress = domain.ProgressActive
	second.Control = domain.ControlRunning
	secondAssignment := domain.Assignment{
		ID: second.AssignmentID, AttemptID: second.ID, WorkerID: "worker",
		WorkerEpoch: "worker-epoch-1", Epoch: 1, State: domain.AssignmentClaimed,
		ThreadID: "thread-second", CreatedAt: now, UpdatedAt: now,
	}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Attempts: []domain.Attempt{second}, Assignments: []domain.Assignment{secondAssignment},
	}); err != nil {
		t.Fatal(err)
	}

	firstWait, err := store.RegisterTaskWait(ctx, taskWaitRegistration(first, "simultaneous-first", domain.WakeEach), now)
	if err != nil {
		t.Fatal(err)
	}
	secondReg := taskWaitRegistration(second, "simultaneous-second", domain.WakeEach)
	secondReg.ThreadID = second.ThreadID
	secondReg.IssuedRevision = second.Revision
	secondWait, err := store.RegisterTaskWait(ctx, secondReg, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SettleTaskWait(ctx, firstWait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SettleTaskWait(ctx, secondWait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	wakes, err := store.WakeTaskWaits(ctx, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(wakes) != 1 {
		t.Fatalf("simultaneous settled waits produced %d wakes on one slot: %#v", len(wakes), wakes)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	resuming, parked := 0, 0
	for _, attempt := range records.Attempts {
		if attempt.ID != first.ID && attempt.ID != second.ID {
			continue
		}
		switch attempt.Control {
		case domain.ControlResuming:
			resuming++
		case domain.ControlWaitingExternal:
			parked++
		}
	}
	if resuming != 1 || parked != 1 {
		t.Fatalf("resuming=%d parked=%d, want one of each", resuming, parked)
	}
}
