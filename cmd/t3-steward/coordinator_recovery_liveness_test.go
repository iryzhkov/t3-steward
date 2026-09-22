package main

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type recoveryLivenessFixture struct {
	store       *sqlite.Store
	supervision backlog.CoordinatorSupervisionStore
	coordinator coordinatorSupervision
	run         domain.WorkflowRun
	attempt     domain.Attempt
	now         time.Time
}

func newRecoveryLivenessFixture(t *testing.T) *recoveryLivenessFixture {
	t.Helper()
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, 9, 22, 8, 0, 0, 0, time.UTC)
	config := recoveryTestSupervisionConfig()
	config.Escalation = domain.SupervisionEscalation{NotifyThread: true, ThreadID: "thread-recovery-liveness"}
	run := domain.WorkflowRun{
		ID: "run-recovery-liveness", WorkflowID: "workflow-recovery-liveness",
		GraphRevision: 1, Progress: domain.ProgressActive, Revision: 1,
		CreatedAt: now, UpdatedAt: now,
		Supervision: &domain.SupervisionRecord{RunID: "run-recovery-liveness", Config: config},
	}
	attempt := domain.Attempt{
		ID: "attempt-recovery-liveness", WorkflowRunID: run.ID, TaskID: "task-recovery-liveness",
		Number: 1, Progress: domain.ProgressFailed, Control: domain.ControlStopped,
		Failure: "repairable failure", Revision: 1, UpdatedAt: now,
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: run.WorkflowID, Version: 1, Name: "recovery", Class: domain.TaskClassRequired, TaskIDs: []string{attempt.TaskID}, CreatedAt: now}},
		WorkflowRuns: []domain.WorkflowRun{run},
		Tasks:        []domain.Task{{ID: attempt.TaskID, WorkflowID: run.WorkflowID, Name: "repair", Class: domain.TaskClassRequired}},
		Attempts:     []domain.Attempt{attempt},
	}); err != nil {
		t.Fatal(err)
	}
	supervision := backlog.CoordinatorSupervisionStore{Store: store}
	if _, err := store.PutSupervision(ctx, sqlite.SupervisionMaterialization{Record: *run.Supervision}); err != nil {
		t.Fatal(err)
	}
	return &recoveryLivenessFixture{
		store: store, supervision: supervision, run: run, attempt: attempt, now: now,
		coordinator: coordinatorSupervision{
			store: supervision, logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
}

func (f *recoveryLivenessFixture) observe(t *testing.T, at time.Time) domain.ReviewIncident {
	t.Helper()
	if err := f.coordinator.observeRecoveryFailures(context.Background(), f.run, at); err != nil {
		t.Fatal(err)
	}
	state, err := f.supervision.LoadSupervisionAdminState(context.Background(), f.run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Incidents) != 1 {
		t.Fatalf("incidents=%d want 1", len(state.Incidents))
	}
	return state.Incidents[0].Incident
}

func TestRecoveryStartupRecreatesMissingInitialTriggerOnce(t *testing.T) {
	fixture := newRecoveryLivenessFixture(t)
	first := fixture.observe(t, fixture.now)
	second := fixture.observe(t, fixture.now.Add(time.Minute))
	if first.ID != second.ID || first.Revision != second.Revision {
		t.Fatalf("restart replay changed incident: first=%+v second=%+v", first, second)
	}
	inbox, err := fixture.supervision.ListSupervisionInbox(context.Background(), fixture.run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(inbox) != 1 {
		t.Fatalf("initial recovery triggers=%d want 1", len(inbox))
	}
}

func TestRecoveryWatchdogRewakesThenEscalatesStalledDiagnosisOnce(t *testing.T) {
	fixture := newRecoveryLivenessFixture(t)
	fixture.observe(t, fixture.now)
	rewoken := fixture.observe(t, fixture.now.Add(3*time.Hour))
	if rewoken.Recovery.State != domain.RecoveryPendingDispatch || rewoken.Recovery.NextAction != domain.RecoveryDispatchRepair {
		t.Fatalf("rewoken recovery=%+v", rewoken.Recovery)
	}
	inbox, err := fixture.supervision.ListSupervisionInbox(context.Background(), fixture.run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(inbox) != 2 {
		t.Fatalf("inbox=%d want initial plus one watchdog trigger", len(inbox))
	}
	escalated := fixture.observe(t, fixture.now.Add(6*time.Hour))
	if escalated.Recovery.State != domain.RecoveryNeedsHuman || escalated.Recovery.NextAction != domain.RecoveryEscalateHuman {
		t.Fatalf("escalated recovery=%+v", escalated.Recovery)
	}
	fixture.observe(t, fixture.now.Add(9*time.Hour))
	outbox, err := fixture.supervision.ListSupervisionOutbox(context.Background(), fixture.run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(outbox) != 1 {
		t.Fatalf("escalation intents=%d want 1", len(outbox))
	}
}

func TestRecoveryAbsoluteDeadlineEscalatesOnce(t *testing.T) {
	fixture := newRecoveryLivenessFixture(t)
	fixture.observe(t, fixture.now)
	incident := fixture.observe(t, fixture.now.Add(25*time.Hour))
	if incident.Recovery.State != domain.RecoveryNeedsHuman ||
		incident.Recovery.ExhaustionReason == "" || incident.Recovery.AttemptsUsed != 0 {
		t.Fatalf("deadline recovery=%+v", incident.Recovery)
	}
	fixture.observe(t, fixture.now.Add(26*time.Hour))
	outbox, err := fixture.supervision.ListSupervisionOutbox(context.Background(), fixture.run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(outbox) != 1 {
		t.Fatalf("deadline escalation intents=%d want 1", len(outbox))
	}
}

func TestRecoveryDeliberateAssignmentWaitDoesNotBurnAttemptAndWorkerLossRewakes(t *testing.T) {
	ctx := context.Background()
	fixture := newRecoveryLivenessFixture(t)
	fixture.observe(t, fixture.now)
	assignment := domain.Assignment{
		ID: "assignment-recovery-liveness", AttemptID: fixture.attempt.ID,
		WorkerID: "worker-recovery-liveness",
		Route:    domain.ProviderRoute{ProviderInstanceID: "repairer", Model: "repair-model"},
		State:    domain.AssignmentOffered, Epoch: 1, CreatedAt: fixture.now, UpdatedAt: fixture.now,
	}
	if err := fixture.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	waiting := fixture.observe(t, fixture.now.Add(3*time.Hour))
	if waiting.Recovery.State != domain.RecoveryExpectedWait ||
		waiting.Recovery.NextAction != domain.RecoveryAwaitCapacity ||
		waiting.Recovery.AttemptsUsed != 0 {
		t.Fatalf("deliberate wait recovery=%+v", waiting.Recovery)
	}
	assignment.State = domain.AssignmentReleased
	assignment.UpdatedAt = fixture.now.Add(4 * time.Hour)
	if err := fixture.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	rewoken := fixture.observe(t, fixture.now.Add(6*time.Hour))
	if rewoken.Recovery.State != domain.RecoveryPendingDispatch ||
		rewoken.Recovery.NextAction != domain.RecoveryDispatchRepair ||
		rewoken.Recovery.AttemptsUsed != 0 {
		t.Fatalf("worker-loss recovery=%+v", rewoken.Recovery)
	}
}
