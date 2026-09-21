package main

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestCapacityReviewActivationSkipsFullCandidateForFreeWorker(t *testing.T) {
	fixture := newActivationLeaseFixture(t)
	fixture.superviseRun(t)
	ctx := context.Background()
	makeSnapshot := func(id string) domain.WorkerSnapshot {
		return domain.WorkerSnapshot{
			WorkerID: id, WorkerEpoch: "worker-epoch-1", CoordinatorEpoch: 1, Sequence: 1,
			Connected: true, ObservedAt: fixture.now, ValidUntil: fixture.now.Add(time.Hour),
			Inventory: domain.WorkerInventory{
				ID: id, AcceptBacklog: true, Health: domain.WorkerHealthReady,
				Allocatable:  domain.AllocatableCapacity{ExecutorSlots: 1},
				Capabilities: []string{workerproto.CapabilityCampaignSupervision},
				ObservedAt:   fixture.now,
				Providers: []domain.WorkerProviderInventory{{
					InstanceID: "claudeAgent", Models: []string{"claude-fable-5-1"},
					QuotaPoolID: "claude-main", Available: true,
				}},
			},
		}
	}
	full, free := makeSnapshot("a-full"), makeSnapshot("z-free")
	if err := fixture.store.SaveWorkerSnapshot(ctx, full); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.SaveWorkerSnapshot(ctx, free); err != nil {
		t.Fatal(err)
	}
	ownerAttempt := domain.Attempt{
		ID: "attempt-full-owner", WorkflowRunID: activationLeaseRun, TaskID: "task-producer",
		Number: 2, AssignmentID: "assignment-full-owner", Progress: domain.ProgressActive,
		Control: domain.ControlRunning, Revision: 1, UpdatedAt: fixture.now,
	}
	ownerAssignment := domain.Assignment{
		ID: ownerAttempt.AssignmentID, AttemptID: ownerAttempt.ID, WorkerID: full.WorkerID,
		WorkerEpoch: full.WorkerEpoch, Epoch: 1, State: domain.AssignmentClaimed,
		CreatedAt: fixture.now, UpdatedAt: fixture.now,
	}
	if err := fixture.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Attempts: []domain.Attempt{ownerAttempt}, Assignments: []domain.Assignment{ownerAssignment},
	}); err != nil {
		t.Fatal(err)
	}

	fixture.coordinator.DispatchActivations(ctx, admittingQuota())
	state, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.State != domain.ActivationPendingDispatch {
		t.Fatalf("activation state=%q, want dispatch through free later candidate", state.Activation.State)
	}
	records, err := fixture.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, assignment := range records.Assignments {
		if assignment.ID != ownerAssignment.ID && assignment.WorkerID != free.WorkerID {
			t.Fatalf("activation selected worker %q, want %q", assignment.WorkerID, free.WorkerID)
		}
	}
}
