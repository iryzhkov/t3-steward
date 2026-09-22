package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

const recoveryBoundaryWorker = "worker-recovery-boundary"

func TestRecoveryBoundaryCycleDispatchesRepairThroughOneHealthySlot(t *testing.T) {
	ctx := context.Background()
	fixture := newRecoveryLivenessFixture(t)
	cycle := recoveryBoundaryCycle(t, fixture, 1)

	cycle.Tick(ctx)

	state, err := fixture.supervision.LoadSupervisionActivationState(ctx, fixture.run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.Purpose != domain.RecoveryActivationRepair ||
		state.Activation.State != domain.ActivationPendingDispatch {
		t.Fatalf("repair activation was not dispatched: %+v", state.Activation)
	}
	_, assignment := recoveryBoundaryActivationAssignment(t, fixture, state.Activation.ID)
	if assignment.State != domain.AssignmentOffered ||
		assignment.WorkerID != recoveryBoundaryWorker ||
		assignment.Route.QuotaPoolID != "recovery-pool" {
		t.Fatalf("repair assignment did not consume the admitted slot: %+v", assignment)
	}
}

func TestRecoveryBoundaryCycleBoundsOlderOrdinaryContentionAtTwoSlots(t *testing.T) {
	ctx := context.Background()
	fixture := newRecoveryLivenessFixture(t)
	fixture.observe(t, fixture.now)
	triggeredAt := fixture.now
	fixture.now = fixture.now.Add(10 * time.Minute)
	addRecoveryBoundaryOrdinary(t, fixture, "older", triggeredAt.Add(-time.Minute))
	addRecoveryBoundaryOrdinary(t, fixture, "younger", triggeredAt.Add(time.Minute))
	cycle := recoveryBoundaryCycle(t, fixture, 2)

	cycle.Tick(ctx)

	state, err := fixture.supervision.LoadSupervisionActivationState(ctx, fixture.run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.Purpose != domain.RecoveryActivationRepair ||
		state.Activation.State != domain.ActivationPendingDispatch {
		t.Fatalf("repair activation did not receive the bounded second slot: %+v", state.Activation)
	}
	_, repairAssignment := recoveryBoundaryActivationAssignment(t, fixture, state.Activation.ID)
	records, err := fixture.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	offered := map[string]bool{}
	for _, assignment := range records.Assignments {
		if assignment.State == domain.AssignmentOffered {
			offered[assignment.AttemptID] = true
		}
	}
	if !offered["attempt-recovery-ordinary-older"] {
		t.Fatal("older ordinary attempt did not receive the first slot")
	}
	if offered["attempt-recovery-ordinary-younger"] {
		t.Fatalf("younger ordinary attempt bypassed the repair activation: %+v", offered)
	}
	if !offered[repairAssignment.AttemptID] {
		t.Fatalf("repair activation has no real offered assignment: %+v", offered)
	}
}

func TestRecoveryRestartWatchdogRewakesAcknowledgedTriggerThenEscalates(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		healthyQuota bool
	}{
		{name: "acknowledged initial trigger has no supported wait"},
		{name: "healthy admission without dispatch progress", healthyQuota: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := newRecoveryLivenessFixture(t)
			configureRecoveryActivationStore(fixture)
			first := fixture.observe(t, fixture.now)

			records, err := fixture.store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			state, err := fixture.supervision.LoadSupervisionActivationState(ctx, fixture.run.ID)
			if err != nil {
				t.Fatal(err)
			}
			signal, wanted, err := fixture.coordinator.activationSignal(records, state)
			if err != nil || !wanted {
				t.Fatalf("initial repair signal wanted=%v err=%v signal=%+v", wanted, err, signal)
			}
			plan, err := fixture.coordinator.activations.Advance(ctx, fixture.run.ID, signal)
			if err != nil {
				t.Fatal(err)
			}
			if plan.Dispatch == nil || plan.Activation.Purpose != domain.RecoveryActivationRepair {
				t.Fatalf("initial repair trigger was not acknowledged by a dispatch plan: %+v", plan)
			}
			restartRecoveryCoordinator(fixture)
			if testCase.healthyQuota {
				if err := fixture.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{QuotaPools: []domain.QuotaPool{{
					ID: "recovery-pool", ProviderInstanceIDs: []string{"repairer"},
					Admission: domain.AdmissionOpen, MaxConcurrent: 1,
				}}}); err != nil {
					t.Fatal(err)
				}
			}

			rewoken := fixture.observe(t, fixture.now.Add(3*time.Hour))
			if rewoken.Revision <= first.Revision ||
				rewoken.Recovery.State != domain.RecoveryPendingDispatch ||
				rewoken.Recovery.NextAction != domain.RecoveryDispatchRepair {
				t.Fatalf("restart watchdog did not re-wake stalled repair: %+v", rewoken.Recovery)
			}
			inbox, err := fixture.supervision.ListSupervisionInbox(ctx, fixture.run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(inbox) != 2 {
				t.Fatalf("recovery inbox=%d want acknowledged initial trigger plus one watchdog re-wake", len(inbox))
			}

			escalated := fixture.observe(t, fixture.now.Add(6*time.Hour))
			if escalated.Recovery.State != domain.RecoveryNeedsHuman ||
				escalated.Recovery.NextAction != domain.RecoveryEscalateHuman {
				t.Fatalf("stalled repair was not bounded by escalation: %+v", escalated.Recovery)
			}
		})
	}
}

func TestRecoveryRestartWatchdogRecognizesObservedQuotaAndCapacityWaits(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		capacity bool
	}{
		{name: "closed quota"},
		{name: "exhausted executor capacity", capacity: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := newRecoveryLivenessFixture(t)
			configureRecoveryActivationStore(fixture)
			fixture.observe(t, fixture.now)
			pool := domain.QuotaPool{
				ID: "recovery-pool", ProviderInstanceIDs: []string{"repairer"},
				Admission: domain.AdmissionClosed, MaxConcurrent: 1,
			}
			if testCase.capacity {
				pool.Admission = domain.AdmissionOpen
				saveRecoveryBoundaryWorker(t, fixture, 1)
				addRecoveryBoundaryOccupiedSlot(t, fixture)
			}
			if err := fixture.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{QuotaPools: []domain.QuotaPool{pool}}); err != nil {
				t.Fatal(err)
			}

			waiting := fixture.observe(t, fixture.now.Add(3*time.Hour))
			if waiting.Recovery.State != domain.RecoveryExpectedWait ||
				waiting.Recovery.NextAction != domain.RecoveryAwaitCapacity ||
				waiting.Recovery.AttemptsUsed != 0 {
				t.Fatalf("observed blocker was not preserved as a supported wait: %+v", waiting.Recovery)
			}
			if testCase.capacity && waiting.Recovery.WaitReason != "repair trigger is durably queued without a capable worker executor slot" {
				t.Fatalf("capacity wait reason=%q", waiting.Recovery.WaitReason)
			}
			if !testCase.capacity && waiting.Recovery.WaitReason != "repair trigger is durably queued behind a closed or exhausted quota pool" {
				t.Fatalf("quota wait reason=%q", waiting.Recovery.WaitReason)
			}
		})
	}
}

func restartRecoveryCoordinator(fixture *recoveryLivenessFixture) {
	fixture.coordinator = coordinatorSupervision{
		store:  backlog.CoordinatorSupervisionStore{Store: fixture.store},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	configureRecoveryActivationStore(fixture)
}

func configureRecoveryActivationStore(fixture *recoveryLivenessFixture) {
	fixture.store.SetClock(func() time.Time { return fixture.now })
	fixture.coordinator.now = func() time.Time { return fixture.now }
	fixture.coordinator.activations = backlog.SupervisionActivationService{
		Store: fixture.supervision,
		Now:   func() time.Time { return fixture.now },
	}
	fixture.coordinator.settings = coordinatorActivationSettings{
		CoordinatorID: "coordinator-recovery-boundary", CoordinatorEpoch: 1,
		SupervisorClient: "recovery-supervisor",
	}
}

func recoveryBoundaryCycle(t *testing.T, fixture *recoveryLivenessFixture, slots int) coordinatorBoundaryCycle {
	t.Helper()
	configureRecoveryActivationStore(fixture)
	saveRecoveryBoundaryWorker(t, fixture, slots)
	quota := activationFairnessQuota(fixture.now, domain.QuotaPool{
		ID: "recovery-pool", Provider: "repair",
		ProviderInstanceIDs: []string{"repairer"}, Admission: domain.AdmissionOpen,
		MaxConcurrent: slots,
	})
	if err := fixture.store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{QuotaPools: quota.Pools}); err != nil {
		t.Fatal(err)
	}
	planner := &coordinatorPlanner{
		store: fixture.store, coordinator: backlog.FleetCoordinator{Store: fixture.store}, epoch: 1,
		maxWorkerSnapshotAge: time.Hour, maxQuotaObservationAge: time.Hour,
		deadlineRiskWindow: time.Hour, checkpointMargin: time.Minute,
		now: func() time.Time { return fixture.now },
	}
	return coordinatorBoundaryCycle{
		quota: fairnessQuota{quota}, schedules: fairnessSchedules{}, planning: planner,
		admin: fairnessAdmin{}, legacy: fairnessLegacy{}, supervision: &fixture.coordinator,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func saveRecoveryBoundaryWorker(t *testing.T, fixture *recoveryLivenessFixture, slots int) {
	t.Helper()
	fixture.coordinator.workers = func(ctx context.Context) ([]domain.WorkerSnapshot, error) {
		return fixture.store.LoadWorkerSnapshots(ctx)
	}
	if err := fixture.store.SaveWorkerSnapshot(context.Background(), domain.WorkerSnapshot{
		WorkerID: recoveryBoundaryWorker, WorkerEpoch: "worker-epoch-recovery", CoordinatorEpoch: 1, Sequence: 1,
		Connected: true, ObservedAt: fixture.now, ValidUntil: fixture.now.Add(time.Hour),
		Inventory: domain.WorkerInventory{
			ID: recoveryBoundaryWorker, AcceptBacklog: true, Health: domain.WorkerHealthReady,
			ObservedAt: fixture.now, Allocatable: domain.AllocatableCapacity{ExecutorSlots: slots},
			Capabilities: []string{workerproto.CapabilityCampaignSupervision},
			Projects:     []domain.WorkerProjectInventory{{Name: "recovery-project", Available: true, UpdatedAt: fixture.now}},
			Providers: []domain.WorkerProviderInventory{{
				InstanceID: "repairer", Models: []string{"repair-model"},
				QuotaPoolID: "recovery-pool", Available: true,
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
}

func addRecoveryBoundaryOrdinary(t *testing.T, fixture *recoveryLivenessFixture, suffix string, readyAt time.Time) {
	t.Helper()
	workflowID := "workflow-recovery-ordinary-" + suffix
	runID := "run-recovery-ordinary-" + suffix
	taskID := "task-recovery-ordinary-" + suffix
	attemptID := "attempt-recovery-ordinary-" + suffix
	route := domain.ProviderRoute{
		WorkerID: recoveryBoundaryWorker, ProviderInstanceID: "repairer",
		Model: "repair-model", QuotaPoolID: "recovery-pool",
	}
	if err := fixture.store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: workflowID, Version: 1, Name: workflowID, Project: "recovery-project",
			Class: domain.TaskClassRequired, TaskIDs: []string{taskID}, CreatedAt: readyAt,
		}},
		WorkflowRuns: []domain.WorkflowRun{{
			ID: runID, WorkflowID: workflowID, Progress: domain.ProgressActive,
			Revision: 1, CreatedAt: readyAt, UpdatedAt: readyAt,
		}},
		Tasks: []domain.Task{{
			ID: taskID, WorkflowID: workflowID, Name: taskID, Class: domain.TaskClassRequired,
			Routes: []domain.ProviderRoute{route}, MaxTurns: 2,
		}},
		Attempts: []domain.Attempt{{
			ID: attemptID, WorkflowRunID: runID, TaskID: taskID, Number: 1,
			Progress: domain.ProgressReady, Control: domain.ControlUnassigned,
			Revision: 1, UpdatedAt: readyAt,
		}},
	}); err != nil {
		t.Fatal(err)
	}
}

func addRecoveryBoundaryOccupiedSlot(t *testing.T, fixture *recoveryLivenessFixture) {
	t.Helper()
	addRecoveryBoundaryOrdinary(t, fixture, "occupied", fixture.now.Add(-time.Minute))
	records, err := fixture.store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for index := range records.Attempts {
		if records.Attempts[index].ID != "attempt-recovery-ordinary-occupied" {
			continue
		}
		records.Attempts[index].AssignmentID = "assignment-recovery-ordinary-occupied"
		records.Attempts[index].Progress = domain.ProgressActive
		records.Attempts[index].Control = domain.ControlRunning
	}
	if err := fixture.store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{
		Attempts: records.Attempts,
		Assignments: []domain.Assignment{{
			ID: "assignment-recovery-ordinary-occupied", AttemptID: "attempt-recovery-ordinary-occupied",
			Project: "recovery-project", WorkerID: recoveryBoundaryWorker, WorkerEpoch: "worker-epoch-recovery",
			Route: domain.ProviderRoute{
				WorkerID: recoveryBoundaryWorker, ProviderInstanceID: "repairer",
				Model: "repair-model", QuotaPoolID: "recovery-pool",
			},
			State: domain.AssignmentClaimed, Epoch: 1,
			CreatedAt: fixture.now.Add(-time.Minute), UpdatedAt: fixture.now,
		}},
	}); err != nil {
		t.Fatal(err)
	}
}

func recoveryBoundaryActivationAssignment(t *testing.T, fixture *recoveryLivenessFixture, activationID string) (domain.Attempt, domain.Assignment) {
	t.Helper()
	records, err := fixture.store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range records.Attempts {
		if attempt.SupervisionActivationID != activationID {
			continue
		}
		for _, assignment := range records.Assignments {
			if assignment.AttemptID == attempt.ID {
				return attempt, assignment
			}
		}
	}
	t.Fatalf("activation %q has no durable assignment", activationID)
	return domain.Attempt{}, domain.Assignment{}
}
