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
)

func TestOlderOrdinaryAttemptPrecedesNewerActivationAtSharedCapacity(t *testing.T) {
	ctx := context.Background()
	fixture := newActivationLeaseFixture(t)
	fixture.superviseRun(t)
	const (
		project          = "shared-project"
		ordinaryWorkflow = "workflow-ordinary-fairness"
		ordinaryRun      = "run-ordinary-fairness"
		ordinaryTask     = "task-ordinary-fairness"
		ordinaryAttempt  = "attempt-ordinary-fairness"
		parkedWorkflow   = "workflow-parked-fairness"
		parkedRun        = "run-parked-fairness"
		parkedTask       = "task-parked-fairness"
		parkedAttempt    = "attempt-parked-fairness"
		parkedAssignment = "assignment-parked-fairness"
	)
	route := domain.ProviderRoute{
		WorkerID: activationLeaseWorker, ProviderInstanceID: "claudeAgent",
		Model: "claude-fable-5-1", QuotaPoolID: "claude-main",
	}
	older := fixture.now.Add(-2 * time.Minute)
	ordinary := domain.Attempt{
		ID: ordinaryAttempt, WorkflowRunID: ordinaryRun, TaskID: ordinaryTask, Number: 1,
		Progress: domain.ProgressReady, Control: domain.ControlUnassigned,
		Revision: 1, UpdatedAt: older,
	}
	parked := domain.Attempt{
		ID: parkedAttempt, WorkflowRunID: parkedRun, TaskID: parkedTask, Number: 1,
		AssignmentID: parkedAssignment, ThreadID: "thread-parked-fairness",
		Progress: domain.ProgressWaitingExternal, Control: domain.ControlWaitingExternal,
		Revision: 2, UpdatedAt: older,
	}
	if err := fixture.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{
			{ID: ordinaryWorkflow, Version: 1, Name: ordinaryWorkflow, Project: project, Class: domain.TaskClassRequired, TaskIDs: []string{ordinaryTask}, CreatedAt: older},
			{ID: parkedWorkflow, Version: 1, Name: parkedWorkflow, Project: project, Class: domain.TaskClassRequired, TaskIDs: []string{parkedTask}, CreatedAt: older},
		},
		WorkflowRuns: []domain.WorkflowRun{
			{ID: ordinaryRun, WorkflowID: ordinaryWorkflow, Progress: domain.ProgressActive, Revision: 1, CreatedAt: older, UpdatedAt: older},
			{ID: parkedRun, WorkflowID: parkedWorkflow, Progress: domain.ProgressActive, Revision: 1, CreatedAt: older, UpdatedAt: older},
		},
		Tasks: []domain.Task{
			{ID: ordinaryTask, WorkflowID: ordinaryWorkflow, Name: "ordinary", Class: domain.TaskClassRequired, Routes: []domain.ProviderRoute{route}, MaxTurns: 2},
			{ID: parkedTask, WorkflowID: parkedWorkflow, Name: "parked", Class: domain.TaskClassRequired, Routes: []domain.ProviderRoute{route}, MaxTurns: 2},
		},
		Attempts: []domain.Attempt{ordinary, parked},
		Assignments: []domain.Assignment{{
			ID: parkedAssignment, AttemptID: parkedAttempt, Project: project,
			WorkerID: activationLeaseWorker, WorkerEpoch: "worker-epoch-1", Route: route,
			ExecutorDemand: &domain.ResourceDemand{}, State: domain.AssignmentClaimed, Epoch: 1,
			ThreadID: parked.ThreadID, CreatedAt: older, UpdatedAt: older,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	snapshots, err := fixture.store.LoadWorkerSnapshots(ctx)
	if err != nil || len(snapshots) != 1 {
		t.Fatalf("worker snapshots=%+v err=%v", snapshots, err)
	}
	snapshot := snapshots[0]
	snapshot.Sequence++
	snapshot.ObservedAt = fixture.now.Add(time.Second)
	snapshot.ValidUntil = fixture.now.Add(time.Hour)
	snapshot.Inventory.ObservedAt = snapshot.ObservedAt
	snapshot.Inventory.Projects = []domain.WorkerProjectInventory{{Name: project, Available: true, UpdatedAt: snapshot.ObservedAt}}
	if err := fixture.store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	wait, err := fixture.store.RegisterTaskWait(ctx, domain.TaskWaitRegistration{
		RequestID: "wait-parked-fairness", WorkflowRunID: parkedRun, TaskID: parkedTask,
		AttemptID: parkedAttempt, IssuedRevision: parked.Revision, ThreadID: parked.ThreadID,
		Wake: domain.WakeEach, MaxDuration: time.Hour, Name: "ready", Condition: "true",
	}, older)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.SettleTaskWait(ctx, wait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, older); err != nil {
		t.Fatal(err)
	}

	quota := backlog.QuotaBridgeReport{
		Pools: []domain.QuotaPool{{
			ID: "claude-main", Provider: "claude", ProviderInstanceIDs: []string{"claudeAgent"},
			Admission: domain.AdmissionOpen, MaxConcurrent: 1,
		}},
		Windows: []backlog.QuotaWindowBudget{{
			QuotaPoolID: "claude-main", WindowID: "primary", ObservedAt: fixture.now,
			Admission: domain.AdmissionOpen, Capacity: 100, ResetsAt: fixture.now.Add(time.Hour),
		}},
		Derived: []backlog.QuotaPoolAdmissionSnapshot{{
			QuotaPoolID: "claude-main", Admission: domain.AdmissionOpen, ObservedAt: fixture.now,
		}},
	}
	planner := &coordinatorPlanner{
		store: fixture.store, coordinator: backlog.FleetCoordinator{Store: fixture.store}, epoch: 1,
		maxWorkerSnapshotAge: time.Hour, maxQuotaObservationAge: time.Hour,
		deadlineRiskWindow: time.Hour, checkpointMargin: time.Minute,
		now: func() time.Time { return fixture.now },
	}
	cycle := coordinatorBoundaryCycle{
		quota: fairnessQuota{quota}, schedules: fairnessSchedules{}, planning: planner,
		admin: fairnessAdmin{}, legacy: fairnessLegacy{}, supervision: &fixture.coordinator,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	cycle.Tick(ctx)

	state, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.State != "" {
		t.Fatalf("activation state=%q, want newer activation still queued", state.Activation.State)
	}
	records, err := fixture.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var ordinaryOffer bool
	for _, assignment := range records.Assignments {
		if assignment.AttemptID == ordinaryAttempt && assignment.State == domain.AssignmentOffered {
			ordinaryOffer = true
		}
		if assignment.AttemptID != ordinaryAttempt && assignment.AttemptID != parkedAttempt &&
			assignment.WorkerID == activationLeaseWorker && assignment.State == domain.AssignmentOffered {
			t.Fatalf("newer activation received assignment before older ordinary attempt: %#v", assignment)
		}
	}
	if !ordinaryOffer {
		t.Fatal("older eligible ordinary attempt did not receive the shared capacity")
	}
	gotParked := loadFairnessAttempt(t, ctx, fixture.store, parkedAttempt)
	if gotParked.Control != domain.ControlWaitingExternal {
		t.Fatalf("older settled wake control=%q, want it held behind newer activation", gotParked.Control)
	}
}
