package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestCompletedActivationReconcilesAndEscalatesWithClosedAdmissionAndOfflineWorker(t *testing.T) {
	fixture := newActivationLeaseFixture(t)
	fixture.superviseRun(t)
	fixture.activate(t)
	fixture.now = activationLeaseTime.Add(3 * time.Minute)
	fixture.finishActivationTurn(t)
	fixture.coordinator.workers = func(context.Context) ([]domain.WorkerSnapshot, error) {
		return nil, nil
	}

	fixture.coordinator.DispatchActivations(context.Background(), backlog.WorkerAdmissionPolicy{})

	state, err := fixture.supervision.LoadSupervisionActivationState(context.Background(), activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.State != domain.ActivationSpent ||
		state.Activation.Outcome != domain.ActivationOutcomeNoDecision {
		t.Fatalf("activation = %#v, want spent with no-decision outcome", state.Activation)
	}
	if incident := fixture.incident(t); incident.State != domain.IncidentEscalated {
		t.Fatalf("incident state = %q, want escalated", incident.State)
	}
	if got := len(escalationsOf(fixture.outbox(t))); got != 1 {
		t.Fatalf("escalation count = %d, want one", got)
	}
}

func TestCompletedActivationReconcilesWhenWorkerSnapshotCollectionFails(t *testing.T) {
	fixture := newActivationLeaseFixture(t)
	fixture.superviseRun(t)
	fixture.activate(t)
	fixture.now = activationLeaseTime.Add(3 * time.Minute)
	fixture.finishActivationTurn(t)
	fixture.coordinator.workers = func(context.Context) ([]domain.WorkerSnapshot, error) {
		return nil, errors.New("worker inventory unavailable")
	}

	fixture.coordinator.DispatchActivations(context.Background(), backlog.WorkerAdmissionPolicy{})

	state, err := fixture.supervision.LoadSupervisionActivationState(context.Background(), activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.State != domain.ActivationSpent {
		t.Fatalf("activation state = %q, want completion reconciled despite worker collection failure", state.Activation.State)
	}
}

func TestBoundaryTickReconcilesCompletedActivationWhenQuotaFailsAndBlocksNextDispatch(t *testing.T) {
	fixture := newActivationLeaseFixture(t)
	fixture.superviseRun(t)
	activation := fixture.activate(t)
	fixture.now = activationLeaseTime.Add(3 * time.Minute)
	fixture.finishActivationTurn(t)

	var quotaCalls, scheduleCalls, planningCalls, adminCalls, legacyCalls int
	cycle := coordinatorBoundaryCycle{
		projection:  fixture.supervision,
		quota:       failingCoordinatorQuotaTicker{calls: &quotaCalls},
		schedules:   recordingCoordinatorScheduleTicker{calls: &scheduleCalls},
		planning:    recordingCoordinatorPlanningTicker{calls: &planningCalls},
		admin:       recordingCoordinatorAdminExecutor{calls: &adminCalls},
		legacy:      recordingCoordinatorLegacyTicker{calls: &legacyCalls},
		supervision: &fixture.coordinator,
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	cycle.Tick(context.Background())

	state, err := fixture.supervision.LoadSupervisionActivationState(context.Background(), activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.State != domain.ActivationSpent ||
		state.Activation.Outcome != domain.ActivationOutcomeNoDecision {
		t.Fatalf("activation = %#v, want completion reconciled during failed quota tick", state.Activation)
	}

	fixture.now = activationLeaseTime.Add(4 * time.Minute)
	if _, err := fixture.supervision.AppendSupervisionEvents(context.Background(), activationLeaseRun,
		[]backlog.SupervisionEvent{{
			ID: "event-reassess-after-quota-failure", RunID: activationLeaseRun,
			Kind: backlog.TriggerOperatorReassessment, Reason: "reassess after completion",
			IncidentID: activationLeaseIncident, OccurredAt: fixture.now,
		}}); err != nil {
		t.Fatal(err)
	}
	cycle.Tick(context.Background()) // spent -> idle is reconciliation and needs no admission
	cycle.Tick(context.Background()) // idle -> dispatch remains blocked by fail-closed admission

	state, err = fixture.supervision.LoadSupervisionActivationState(context.Background(), activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.State != domain.ActivationIdle {
		t.Fatalf("activation state = %q, want idle while unhealthy quota blocks dispatch", state.Activation.State)
	}
	if state.Record.ActivationsUsed != int(activation.Epoch) {
		t.Fatalf("activations used = %d, want no additional activation budget spent", state.Record.ActivationsUsed)
	}
	if quotaCalls != 3 || planningCalls != 0 || adminCalls != 0 {
		t.Fatalf("calls quota=%d planning=%d admin=%d, want failed quota on each tick and no gated work", quotaCalls, planningCalls, adminCalls)
	}
}

func TestActivationDispatchWaitsForOrdinaryOwnerAndUsesParkedRelease(t *testing.T) {
	fixture := newActivationLeaseFixture(t)
	fixture.superviseRun(t)
	ctx := context.Background()
	ownerAttempt := domain.Attempt{
		ID: "attempt-capacity-owner", WorkflowRunID: activationLeaseRun,
		TaskID: "task-producer", Number: 2, AssignmentID: "assignment-capacity-owner",
		Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 1,
		UpdatedAt: fixture.now,
	}
	ownerAssignment := domain.Assignment{
		ID: "assignment-capacity-owner", AttemptID: ownerAttempt.ID,
		WorkerID: activationLeaseWorker, WorkerEpoch: "worker-epoch-1",
		Epoch: 1, State: domain.AssignmentClaimed, CreatedAt: fixture.now, UpdatedAt: fixture.now,
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
	if state.Activation.State != "" && state.Activation.State != domain.ActivationIdle {
		t.Fatalf("activation state = %q, want full worker to block dispatch", state.Activation.State)
	}

	ownerAttempt.Control = domain.ControlWaitingExternal
	ownerAttempt.Progress = domain.ProgressWaitingExternal
	ownerAttempt.Revision++
	ownerAttempt.UpdatedAt = fixture.now.Add(time.Minute)
	if err := fixture.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Attempts: []domain.Attempt{ownerAttempt},
	}); err != nil {
		t.Fatal(err)
	}
	fixture.now = fixture.now.Add(time.Minute)
	fixture.coordinator.DispatchActivations(ctx, admittingQuota())
	state, err = fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.State != domain.ActivationPendingDispatch {
		t.Fatalf("activation state = %q, want parked owner to release slot for dispatch", state.Activation.State)
	}
}

func TestActivationDispatchStillRequiresAdmission(t *testing.T) {
	fixture := newActivationLeaseFixture(t)
	fixture.superviseRun(t)
	fixture.coordinator.DispatchActivations(context.Background(), backlog.WorkerAdmissionPolicy{})

	state, err := fixture.supervision.LoadSupervisionActivationState(context.Background(), activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.State != "" && state.Activation.State != domain.ActivationIdle {
		t.Fatalf("activation state = %q, want no dispatch while admission is closed", state.Activation.State)
	}
	if state.Record.ActivationsUsed != 0 {
		t.Fatalf("activations used = %d, want no budget spent while admission is closed", state.Record.ActivationsUsed)
	}
}
