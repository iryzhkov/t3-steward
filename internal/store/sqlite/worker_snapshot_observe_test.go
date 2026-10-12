package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestObserveWorkerSnapshotAttentionStopAfterStoredObservation(t *testing.T) {
	ctx := context.Background()
	store, attempt, wait, decision := registerAttentionFixture(t, domain.AttentionDirection)
	defer store.Close()
	decision.Kind, decision.Reason = domain.AttentionStop, "operator stop"
	prepareAttentionStopBinding(t, store, attempt, wait.RegisteredAt.Add(30*time.Second))
	appliedAt := wait.RegisteredAt.Add(time.Minute)
	if _, _, err := store.DecideAttentionForCoordinator(ctx, decision, "remote:human", "coord-1", appliedAt); err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadThrottleAttemptRecords(ctx)
	if err != nil || len(records) != 1 {
		t.Fatalf("records=%+v err=%v", records, err)
	}
	stored := stoppedAttentionSnapshot(t, store, records[0], 3, appliedAt.Add(time.Second))
	if _, err := store.ObserveWorkerSnapshot(ctx, stored, false); err != nil {
		t.Fatal(err)
	}
	var before string
	if err := store.db.QueryRowContext(ctx, "SELECT record FROM coordinator_worker_snapshots WHERE worker_id=?", stored.WorkerID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	eventsBefore, err := store.LoadAuditEvents(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	workerEvents := func(events []domain.AuditEvent) int {
		n := 0
		for _, event := range events {
			if event.TargetType == domain.AuditTargetWorker {
				n++
			}
		}
		return n
	}
	ackAt := appliedAt.Add(2 * time.Second)
	acknowledgeAttentionStop(t, store, ackAt)
	incoming := stored
	incoming.Assignments = append([]domain.WorkerAssignmentObservation(nil), stored.Assignments...)
	incoming.Sequence++
	incoming.ObservedAt = appliedAt.Add(3 * time.Second)
	incoming.ValidUntil = incoming.ObservedAt.Add(time.Hour)
	incoming.Assignments[0].ObservedAt = incoming.ObservedAt
	got, err := store.ObserveWorkerSnapshot(ctx, incoming, false)
	if err != nil {
		t.Fatal(err)
	}
	if got.Sequence != stored.Sequence {
		t.Fatalf("skipped observation persisted: %+v", got)
	}
	var after string
	if err := store.db.QueryRowContext(ctx, "SELECT record FROM coordinator_worker_snapshots WHERE worker_id=?", stored.WorkerID).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("attention scan rewrote worker snapshot")
	}
	eventsAfter, err := store.LoadAuditEvents(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if workerEvents(eventsAfter) != workerEvents(eventsBefore) {
		t.Fatal("attention scan added worker audit")
	}
	if got := latestAttentionReceipt(t, store, wait.ID); got.State != domain.AttentionObserved {
		t.Fatalf("receipt=%+v", got)
	}
	if got := loadAttempt(t, store, attempt.ID); got.Control != domain.ControlStopped {
		t.Fatalf("attempt=%+v", got)
	}
	assignments, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, assignment := range assignments.Assignments {
		if assignment.ID == attempt.AssignmentID && assignment.State != domain.AssignmentReleased {
			t.Fatalf("assignment=%+v", assignment)
		}
	}
}

func TestWorkerSnapshotSubstanceAndRefresh(t *testing.T) {
	a := fleetSnapshot(1, "epoch", 1, true, fleetTestTime.Add(time.Minute))
	b := a
	b.Sequence++
	b.Inventory.Sequence++
	b.ObservedAt = a.ObservedAt.Add(29 * time.Second)
	b.ValidUntil = b.ObservedAt.Add(time.Minute)
	b.Inventory.Providers = []domain.WorkerProviderInventory{}
	if !workerSnapshotSubstanceEqual(a, b) || workerSnapshotRefreshDue(a, b) {
		t.Fatal("metadata/empty slice changed substance or refreshed early")
	}
	b.ObservedAt = a.ObservedAt.Add(30 * time.Second)
	if !workerSnapshotRefreshDue(a, b) {
		t.Fatal("refresh missing at half validity")
	}
	b.Inventory.Health = domain.WorkerHealthDegraded
	if workerSnapshotSubstanceEqual(a, b) {
		t.Fatal("health change ignored")
	}
}
