package sqlite

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestAssignmentPlanOneSlotCommitsOnlyOneOffer(t *testing.T) {
	store := openFleetTestStore(t)
	saveFleetAttempt(t, store, fleetAttempt("attempt-capacity-1"))
	secondAttempt := fleetAttempt("attempt-capacity-2")
	secondAttempt.Number = 2
	saveFleetAttempt(t, store, secondAttempt)
	snapshot := fleetSnapshot(1, "worker-epoch-1", 1, true, fleetTestTime.Add(time.Hour))
	snapshot.Inventory.Allocatable.ExecutorSlots = 1
	saveFleetSnapshot(t, store, snapshot)

	first := fleetPlanCommit(1, "assignment-capacity-1", "attempt-capacity-1", "worker-epoch-1", 1)
	second := fleetPlanCommit(1, "assignment-capacity-2", "attempt-capacity-2", "worker-epoch-1", 1)
	first.Items = append(first.Items, second.Items...)

	got, err := store.CommitAssignmentPlan(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("committed assignments = %d, want one slot-bound offer", len(got))
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Assignments) != 1 {
		t.Fatalf("durable assignments = %d, want one", len(records.Assignments))
	}
}

func TestAssignmentPlanFencesFrozenSizedDemand(t *testing.T) {
	store := openFleetTestStore(t)
	saveFleetAttempt(t, store, fleetAttempt("attempt-sized-1"))
	secondAttempt := fleetAttempt("attempt-sized-2")
	secondAttempt.Number = 2
	saveFleetAttempt(t, store, secondAttempt)
	snapshot := fleetSnapshot(1, "worker-epoch-1", 1, true, fleetTestTime.Add(time.Hour))
	snapshot.Inventory.CPUClass = domain.CPUClassMedium
	snapshot.Inventory.Allocatable = domain.AllocatableCapacity{
		ExecutorSlots: 2, CPUUnits: 2, MemoryMB: 2048, ScratchMB: 1024,
	}
	saveFleetSnapshot(t, store, snapshot)

	demand := domain.ResourceDemand{
		MinCPUClass: domain.CPUClassMedium, CPUUnits: 2, MemoryMB: 2048, ScratchMB: 1024,
	}
	first := fleetPlanCommit(1, "assignment-sized-1", "attempt-sized-1", "worker-epoch-1", 1)
	first.Items[0].Assignment.ExecutorDemand = &demand
	second := fleetPlanCommit(1, "assignment-sized-2", "attempt-sized-2", "worker-epoch-1", 1)
	second.Items[0].Assignment.ExecutorDemand = &demand
	first.Items = append(first.Items, second.Items...)

	got, err := store.CommitAssignmentPlan(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("committed assignments = %d, want sized capacity to admit one", len(got))
	}
}

func TestConcurrentOneSlotOffersNeverBothCommit(t *testing.T) {
	store := openFleetTestStore(t)
	saveFleetAttempt(t, store, fleetAttempt("attempt-race-1"))
	secondAttempt := fleetAttempt("attempt-race-2")
	secondAttempt.Number = 2
	saveFleetAttempt(t, store, secondAttempt)
	snapshot := fleetSnapshot(1, "worker-epoch-1", 1, true, fleetTestTime.Add(time.Hour))
	snapshot.Inventory.Allocatable.ExecutorSlots = 1
	saveFleetSnapshot(t, store, snapshot)

	commits := []domain.AssignmentPlanCommit{
		fleetPlanCommit(1, "assignment-race-1", "attempt-race-1", "worker-epoch-1", 1),
		fleetPlanCommit(1, "assignment-race-2", "attempt-race-2", "worker-epoch-1", 1),
	}
	var wg sync.WaitGroup
	for _, commit := range commits {
		commit := commit
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = store.CommitAssignmentPlan(context.Background(), commit)
		}()
	}
	wg.Wait()

	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Assignments) > 1 {
		t.Fatalf("concurrent offers overcommitted one slot: %#v", records.Assignments)
	}
}

func TestSettledWaitStaysParkedWithStaleExplicitZeroSnapshot(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	stale := domain.WorkerSnapshot{
		WorkerID: "worker", WorkerEpoch: "worker-epoch-1", CoordinatorEpoch: 1, Sequence: 2,
		Connected: true, ObservedAt: now.Add(time.Minute), ValidUntil: now.Add(2 * time.Minute),
		Inventory: domain.WorkerInventory{
			ID: "worker", AcceptBacklog: true, Health: domain.WorkerHealthReady,
			ObservedAt: now.Add(time.Minute),
		},
	}
	if err := store.SaveWorkerSnapshot(ctx, stale); err != nil {
		t.Fatal(err)
	}
	wait, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "stale-zero", domain.WakeEach), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SettleTaskWait(ctx, wait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now); err != nil {
		t.Fatal(err)
	}
	wakes, err := store.WakeTaskWaits(ctx, now.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(wakes) != 0 || loadAttempt(t, store, attempt.ID).Control != domain.ControlWaitingExternal {
		t.Fatalf("wake=%#v attempt=%#v, want stale zero-slot evidence to remain parked", wakes, loadAttempt(t, store, attempt.ID))
	}
}

func TestSettledWaitStaysParkedUntilSlotCanBeReacquired(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	snapshot := domain.WorkerSnapshot{
		WorkerID: "worker", WorkerEpoch: "worker-epoch-1", CoordinatorEpoch: 1, Sequence: 2,
		Connected: true, ObservedAt: now.Add(time.Second), ValidUntil: now.Add(time.Hour),
		Inventory: domain.WorkerInventory{
			ID: "worker", AcceptBacklog: true, Health: domain.WorkerHealthReady,
			Allocatable: domain.AllocatableCapacity{ExecutorSlots: 1}, ObservedAt: now.Add(time.Second),
		},
	}
	if err := store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	wait, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "capacity-wake", domain.WakeEach), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SettleTaskWait(ctx, wait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	ownerAttempt := domain.Attempt{
		ID: "attempt-competing", WorkflowRunID: attempt.WorkflowRunID, TaskID: attempt.TaskID,
		Number: 2, AssignmentID: "assignment-competing", Progress: domain.ProgressActive,
		Control: domain.ControlRunning, Revision: 1, UpdatedAt: now,
	}
	ownerAssignment := domain.Assignment{
		ID: "assignment-competing", AttemptID: ownerAttempt.ID, WorkerID: "worker",
		Epoch: 1, State: domain.AssignmentClaimed, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Attempts: []domain.Attempt{ownerAttempt}, Assignments: []domain.Assignment{ownerAssignment},
	}); err != nil {
		t.Fatal(err)
	}
	wakes, err := store.WakeTaskWaits(ctx, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(wakes) != 0 || loadAttempt(t, store, attempt.ID).Control != domain.ControlWaitingExternal {
		t.Fatalf("wake=%#v attempt=%#v, want settled wait parked while slot is occupied", wakes, loadAttempt(t, store, attempt.ID))
	}

	ownerAssignment.State = domain.AssignmentCompleted
	ownerAssignment.UpdatedAt = now.Add(3 * time.Second)
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Assignments: []domain.Assignment{ownerAssignment}}); err != nil {
		t.Fatal(err)
	}
	wakes, err = store.WakeTaskWaits(ctx, now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(wakes) != 1 || loadAttempt(t, store, attempt.ID).Control != domain.ControlResuming {
		t.Fatalf("wake=%#v attempt=%#v, want one capacity-reacquired resumption", wakes, loadAttempt(t, store, attempt.ID))
	}
}
