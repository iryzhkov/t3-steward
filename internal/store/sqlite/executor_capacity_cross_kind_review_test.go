package sqlite

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestCapacityReviewActivationAndOrdinaryOfferRaceForOneSlot(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	seedSupervisedRun(t, store, nil)
	snapshot := supervisionWorkerSnapshot()
	snapshot.Sequence = 2
	snapshot.ObservedAt = supervisionTestTime.Add(time.Second)
	snapshot.Inventory.ObservedAt = snapshot.ObservedAt
	snapshot.Inventory.Allocatable.ExecutorSlots = 1
	snapshot.Inventory.Capabilities = []string{workerproto.CapabilityCampaignSupervision}
	if err := store.SaveWorkerSnapshot(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}

	activationAttempt := domain.Attempt{
		ID: "attempt-activation-race", WorkflowRunID: "run-1",
		SupervisionActivationID: "activation-race", Number: 1,
		Progress: domain.ProgressReady, Control: domain.ControlUnassigned,
	}
	activationAssignment := domain.Assignment{
		ID: "assignment-activation-race", AttemptID: activationAttempt.ID, WorkerID: snapshot.WorkerID,
		WorkerEpoch: snapshot.WorkerEpoch, State: domain.AssignmentOffered, Epoch: 1,
		LeaseToken: "activation-lease", DispatchToken: "activation-dispatch", ThreadID: "activation-thread",
	}
	activationCommit := ActivationAssignmentCommit{
		CoordinatorEpoch: 1, Attempt: activationAttempt, Assignment: activationAssignment,
		WorkerEpoch: snapshot.WorkerEpoch, WorkerSnapshotSequence: snapshot.Sequence,
		CommittedAt: snapshot.ObservedAt,
	}
	ordinaryCommit := supervisionPlanCommit()
	ordinaryCommit.CommittedAt = snapshot.ObservedAt
	ordinaryCommit.Items[0].WorkerSnapshotSequence = snapshot.Sequence

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, _ = store.CommitActivationAssignment(context.Background(), activationCommit)
	}()
	go func() {
		defer wg.Done()
		<-start
		_, _ = store.CommitAssignmentPlan(context.Background(), ordinaryCommit)
	}()
	close(start)
	wg.Wait()

	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Assignments) != 1 {
		t.Fatalf("cross-kind one-slot race committed %d assignments: %#v", len(records.Assignments), records.Assignments)
	}
}
