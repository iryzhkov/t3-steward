package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestCapacityReviewWakeRequiresCurrentWorkerEpoch(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assignment := records.Assignments[0]
	assignment.WorkerEpoch = "worker-epoch-1"
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	wait, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "epoch-fence", domain.WakeEach), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SettleTaskWait(ctx, wait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	restarted := domain.WorkerSnapshot{
		WorkerID: "worker", WorkerEpoch: "worker-epoch-2", CoordinatorEpoch: 1, Sequence: 2,
		Connected: true, ObservedAt: now.Add(time.Second), ValidUntil: now.Add(time.Hour),
		Inventory: domain.WorkerInventory{
			ID: "worker", AcceptBacklog: true, Health: domain.WorkerHealthReady,
			Allocatable: domain.AllocatableCapacity{ExecutorSlots: 1}, ObservedAt: now.Add(time.Second),
		},
	}
	if err := store.SaveWorkerSnapshot(ctx, restarted); err != nil {
		t.Fatal(err)
	}
	wakes, err := store.WakeTaskWaits(ctx, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(wakes) != 0 {
		t.Fatalf("stale worker epoch resumed on replacement worker: %#v", wakes)
	}
	if got := loadAttempt(t, store, attempt.ID).Control; got != domain.ControlWaitingExternal {
		t.Fatalf("attempt control=%q, want parked after worker epoch replacement", got)
	}
}
