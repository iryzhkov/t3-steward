package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type fairnessQuota struct{ report backlog.QuotaBridgeReport }

func (q fairnessQuota) Tick(context.Context) (backlog.QuotaBridgeReport, error) { return q.report, nil }

type fairnessSchedules struct{}

func (fairnessSchedules) Tick(context.Context) (backlog.ScheduleTickReport, error) {
	return backlog.ScheduleTickReport{}, nil
}

type fairnessAdmin struct{}

func (fairnessAdmin) ExecutePendingCommands(context.Context) (backlogadmin.CommandExecutionReport, error) {
	return backlogadmin.CommandExecutionReport{}, nil
}

type fairnessLegacy struct{}

func (fairnessLegacy) Tick(context.Context) backlog.LegacySubmissionReport {
	return backlog.LegacySubmissionReport{}
}

// This is a desired fairness contract and deliberately fails before shared
// arbitration: every real coordinator boundary commits ordinary work before a
// separately scheduled task wake can reacquire the worker's only slot.
func TestSettledParkedWakeGetsServiceUnderContinuousOrdinaryLoad(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	epoch, err := store.AcquireCoordinator(ctx, "coordinator")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	const workerID, workerEpoch = "worker", "worker-epoch"
	snapshot := domain.WorkerSnapshot{
		WorkerID: workerID, WorkerEpoch: workerEpoch, CoordinatorEpoch: epoch,
		Sequence: 1, Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Hour),
		Inventory: domain.WorkerInventory{ID: workerID, AcceptBacklog: true,
			Health: domain.WorkerHealthReady, ObservedAt: now,
			Projects:    []domain.WorkerProjectInventory{{Name: "project", Available: true, UpdatedAt: now}},
			Providers:   []domain.WorkerProviderInventory{{InstanceID: "instance", Models: []string{"model"}, QuotaPoolID: "pool", Available: true}},
			Allocatable: domain.AllocatableCapacity{ExecutorSlots: 1}},
	}
	if err := store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}

	parked := domain.Attempt{
		ID: "parked-attempt", WorkflowRunID: "parked-run", TaskID: "parked-task", Number: 1,
		AssignmentID: "parked-assignment", ThreadID: "parked-thread",
		Progress: domain.ProgressWaitingExternal, Control: domain.ControlWaitingExternal,
		Revision: 2, UpdatedAt: now,
	}
	parkedAssignment := domain.Assignment{
		ID: parked.AssignmentID, AttemptID: parked.ID, WorkerID: workerID,
		WorkerEpoch: workerEpoch, Epoch: 1, State: domain.AssignmentClaimed,
		ThreadID: parked.ThreadID, CreatedAt: now, UpdatedAt: now,
		ExecutorDemand: &domain.ResourceDemand{},
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{
			{ID: "parked-workflow", Version: 1, Name: "parked-workflow", Project: "project", Class: domain.TaskClassRequired, TaskIDs: []string{"parked-task"}, CreatedAt: now},
			{ID: "ordinary-workflow", Version: 1, Name: "ordinary-workflow", Project: "project", Class: domain.TaskClassRequired, TaskIDs: []string{"ordinary-task"}, CreatedAt: now},
		},
		WorkflowRuns: []domain.WorkflowRun{
			{ID: "parked-run", WorkflowID: "parked-workflow", Progress: domain.ProgressActive, Revision: 1, CreatedAt: now, UpdatedAt: now},
		},
		Tasks: []domain.Task{
			{ID: "parked-task", WorkflowID: "parked-workflow", Name: "parked", Class: domain.TaskClassRequired, Routes: []domain.ProviderRoute{{ProviderInstanceID: "instance", Model: "model", QuotaPoolID: "pool"}}, MaxTurns: 2},
			{ID: "ordinary-task", WorkflowID: "ordinary-workflow", Name: "ordinary", Class: domain.TaskClassRequired, Routes: []domain.ProviderRoute{{ProviderInstanceID: "instance", Model: "model", QuotaPoolID: "pool"}}, MaxTurns: 2},
		},
		Attempts: []domain.Attempt{parked}, Assignments: []domain.Assignment{parkedAssignment},
	}); err != nil {
		t.Fatal(err)
	}
	w, err := store.RegisterTaskWait(ctx, domain.TaskWaitRegistration{
		RequestID: "settled-wait", WorkflowRunID: parked.WorkflowRunID, TaskID: parked.TaskID,
		AttemptID: parked.ID, IssuedRevision: parked.Revision, ThreadID: parked.ThreadID,
		Wake: domain.WakeEach, MaxDuration: time.Hour, Name: "ready", Condition: "true",
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SettleTaskWait(ctx, w.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now); err != nil {
		t.Fatal(err)
	}

	quota := backlog.QuotaBridgeReport{
		Pools: []domain.QuotaPool{{
			ID: "pool", Provider: "provider", ProviderInstanceIDs: []string{"instance"},
			Admission: domain.AdmissionOpen, MaxConcurrent: 1,
		}},
		Windows: []backlog.QuotaWindowBudget{{
			QuotaPoolID: "pool", WindowID: "primary", ObservedAt: now,
			Admission: domain.AdmissionOpen, Capacity: 100, ResetsAt: now.Add(time.Hour),
		}},
	}
	currentNow := now
	planner := &coordinatorPlanner{
		store: store, coordinator: backlog.FleetCoordinator{Store: store}, epoch: epoch,
		maxWorkerSnapshotAge: time.Hour, maxQuotaObservationAge: time.Hour,
		deadlineRiskWindow: time.Hour, checkpointMargin: time.Minute,
		now: func() time.Time { return currentNow },
	}
	cycle := coordinatorBoundaryCycle{
		quota: fairnessQuota{quota}, schedules: fairnessSchedules{}, planning: planner,
		admin: fairnessAdmin{}, legacy: fairnessLegacy{},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	const serviceBound = 8
	for pass := 1; pass <= serviceBound; pass++ {
		ordinary := domain.Attempt{
			ID:            time.Unix(int64(pass), 0).UTC().Format("ordinary-150405"),
			WorkflowRunID: "ordinary-run-" + time.Unix(int64(pass), 0).UTC().Format("150405"), TaskID: "ordinary-task", Number: pass,
			Progress: domain.ProgressReady, Control: domain.ControlUnassigned,
			Revision: 1, UpdatedAt: now.Add(time.Duration(pass) * time.Second),
		}
		run := domain.WorkflowRun{ID: ordinary.WorkflowRunID, WorkflowID: "ordinary-workflow", Progress: domain.ProgressActive, Revision: 1, CreatedAt: ordinary.UpdatedAt, UpdatedAt: ordinary.UpdatedAt}
		if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}, Attempts: []domain.Attempt{ordinary}}); err != nil {
			t.Fatal(err)
		}
		currentNow = ordinary.UpdatedAt
		cycle.Tick(ctx)
		wakes, err := store.WakeTaskWaits(ctx, ordinary.UpdatedAt)
		if err != nil {
			t.Fatal(err)
		}
		records, err := store.LoadCoordinatorRecords(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, attempt := range records.Attempts {
			if attempt.ID == parked.ID && attempt.Control == domain.ControlResuming {
				if pass != 1 {
					t.Fatalf("older settled wake was not served on first eligible boundary; pass=%d", pass)
				}
				return
			}
		}
		if err != nil {
			t.Fatal(err)
		}
		var offered domain.Assignment
		for _, assignment := range records.Assignments {
			if assignment.AttemptID == ordinary.ID && assignment.State == domain.AssignmentOffered {
				offered = assignment
			}
		}
		if offered.ID == "" {
			t.Fatalf("pass %d made no ordinary offer before wake; fixture did not maintain contention", pass)
		}
		if len(wakes) == 1 {
			return
		}

		completed := offered
		completed.State = domain.AssignmentCompleted
		completed.UpdatedAt = ordinary.UpdatedAt.Add(time.Millisecond)
		ordinary.AssignmentID = completed.ID
		ordinary.Revision++
		ordinary.Progress = domain.ProgressSucceeded
		ordinary.Control = domain.ControlStopped
		ordinary.UpdatedAt = completed.UpdatedAt
		if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
			Attempts: []domain.Attempt{ordinary}, Assignments: []domain.Assignment{completed},
		}); err != nil {
			t.Fatal(err)
		}
	}

	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range records.Attempts {
		if attempt.ID == parked.ID {
			t.Fatalf("settled parked wake received no slot in %d coordinator passes; control=%s revision=%d", serviceBound, attempt.Control, attempt.Revision)
		}
	}
	t.Fatal("parked attempt disappeared")
}
