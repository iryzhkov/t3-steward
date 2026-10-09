package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

// TestFeedback147NestedChildrenOnAFullWorkerAllComplete reproduces feedback
// 147 through the coordinator's own planning boundary and wake admission: a
// worker with N executor slots runs N parents, each parent submits a child that
// can run only on that worker and parks on a task-bound wait for it. Parking
// releases the parents' slots, so every child runs; when the waits settle
// every parent is re-admitted on the same worker and completes. At no boundary
// does the worker hold more than N slots.
func TestFeedback147NestedChildrenOnAFullWorkerAllComplete(t *testing.T) {
	const slots = 3
	const worker, workerEpoch = "omarchy-pc", "omarchy-epoch"
	ctx := context.Background()
	store, err := sqlitetest.OpenMigrated(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	epoch, err := store.AcquireCoordinator(ctx, "coordinator")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	providers := []domain.WorkerProviderInventory{{InstanceID: "claudeAgent", Models: []string{"opus"}, QuotaPoolID: "claude-main", Available: true}}
	projects := []domain.WorkerProjectInventory{{Name: "project", Available: true, UpdatedAt: now}}
	if err := store.SaveWorkerSnapshot(ctx, domain.WorkerSnapshot{
		WorkerID: worker, WorkerEpoch: workerEpoch, CoordinatorEpoch: epoch,
		Sequence: 1, Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Hour),
		Inventory: domain.WorkerInventory{ID: worker, AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: now,
			Projects: projects, Providers: providers, Allocatable: domain.AllocatableCapacity{ExecutorSlots: slots}},
	}); err != nil {
		t.Fatal(err)
	}
	route := domain.ProviderRoute{ProviderInstanceID: "claudeAgent", Model: "opus", QuotaPoolID: "claude-main"}
	records := sqlite.CoordinatorRecords{
		QuotaPools: []domain.QuotaPool{{ID: "claude-main", Provider: "claude", ProviderInstanceIDs: []string{"claudeAgent"},
			Admission: domain.AdmissionOpen, MaxConcurrent: 10}},
	}
	var parents, children []string
	for index := 0; index < slots; index++ {
		parentRun, childRun := fmt.Sprintf("parent-run-%d", index), fmt.Sprintf("child-run-%d", index)
		parentTask, childTask := fmt.Sprintf("parent-task-%d", index), fmt.Sprintf("child-task-%d", index)
		parent := domain.Attempt{
			ID: fmt.Sprintf("parent-%d", index), WorkflowRunID: parentRun, TaskID: parentTask, Number: 1,
			AssignmentID: fmt.Sprintf("parent-assignment-%d", index), ThreadID: fmt.Sprintf("parent-thread-%d", index),
			Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 2, UpdatedAt: now.Add(-time.Hour),
		}
		child := domain.Attempt{
			ID: fmt.Sprintf("child-%d", index), WorkflowRunID: childRun, TaskID: childTask, Number: 1,
			Progress: domain.ProgressReady, Control: domain.ControlUnassigned, Revision: 1, UpdatedAt: now,
		}
		parents, children = append(parents, parent.ID), append(children, child.ID)
		for _, pair := range [][2]string{{parentRun, parentTask}, {childRun, childTask}} {
			records.Workflows = append(records.Workflows, domain.Workflow{
				ID: "workflow-" + pair[0], Version: 1, Name: pair[0], Project: "project",
				Class: domain.TaskClassRequired, TaskIDs: []string{pair[1]}, CreatedAt: now,
			})
			records.Tasks = append(records.Tasks, domain.Task{
				ID: pair[1], WorkflowID: "workflow-" + pair[0], Name: pair[1], Class: domain.TaskClassRequired,
				Routes: []domain.ProviderRoute{route}, MaxTurns: 2,
				// The probes of feedback 147 could run only on omarchy-pc.
				Placement: domain.Placement{Hosts: []string{worker}},
			})
		}
		records.WorkflowRuns = append(records.WorkflowRuns,
			domain.WorkflowRun{ID: parentRun, WorkflowID: "workflow-" + parentRun, Progress: domain.ProgressActive, Revision: 1,
				CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour)},
			domain.WorkflowRun{ID: childRun, WorkflowID: "workflow-" + childRun, Progress: domain.ProgressActive, Revision: 1,
				CreatedAt: now, UpdatedAt: now, Lineage: &domain.RunLineage{
					ParentRunID: parentRun, ParentTaskID: parentTask, ParentAttemptID: parent.ID,
					ParentStartedAt: now.Add(-time.Hour), RecordedAt: now,
				}})
		records.Attempts = append(records.Attempts, parent, child)
		records.Assignments = append(records.Assignments, domain.Assignment{
			ID: parent.AssignmentID, AttemptID: parent.ID, WorkerID: worker, WorkerEpoch: workerEpoch, Project: "project",
			Epoch: 1, State: domain.AssignmentClaimed, ThreadID: parent.ThreadID, Route: route,
			LeaseToken: "lease-" + parent.ID, DispatchToken: "dispatch-" + parent.ID,
			ExecutorDemand: &domain.ResourceDemand{}, CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour),
		})
	}
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitQuotaAdmissionTransitions(ctx, []domain.QuotaAdmissionTransition{{
		Record: domain.QuotaAdmissionRecord{QuotaPoolID: "claude-main", Revision: 1, Admission: domain.AdmissionOpen,
			ObservedAt: now, AppliedAt: now, Reason: "fixture admission"},
	}}); err != nil {
		t.Fatal(err)
	}
	quota := backlog.QuotaBridgeReport{
		Pools: records.QuotaPools,
		Windows: []backlog.QuotaWindowBudget{{
			QuotaPoolID: "claude-main", WindowID: "primary", ObservedAt: now,
			Admission: domain.AdmissionOpen, Capacity: 100, ResetsAt: now.Add(time.Hour),
		}},
	}
	clock := now
	planner := &coordinatorPlanner{
		store: store, coordinator: backlog.FleetCoordinator{Store: store}, epoch: epoch,
		maxWorkerSnapshotAge: time.Hour, maxQuotaObservationAge: time.Hour,
		deadlineRiskWindow: time.Hour, checkpointMargin: time.Minute,
		now: func() time.Time { return clock },
	}
	boundary := func(label string) sqlite.CoordinatorRecords {
		t.Helper()
		clock = clock.Add(time.Second)
		if _, err := planner.tick(ctx, quota, true); err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		loaded, err := store.LoadCoordinatorRecords(ctx)
		if err != nil {
			t.Fatal(err)
		}
		assertWorkerSlotsWithin(t, label, loaded, worker, slots)
		return loaded
	}

	// The worker is full of running parents: no child can start.
	loaded := boundary("parents running")
	if offered := offeredAttempts(loaded, children); len(offered) != 0 {
		t.Fatalf("children %v were offered while every slot was held by a running parent", offered)
	}

	// Each parent parks on a task-bound wait for its child.
	waits := make(map[string]string, slots)
	for _, id := range parents {
		parent := attemptByID(t, loaded, id)
		wait, err := store.RegisterTaskWait(ctx, domain.TaskWaitRegistration{
			RequestID: "wait-" + id, WorkflowRunID: parent.WorkflowRunID, TaskID: parent.TaskID,
			AttemptID: parent.ID, IssuedRevision: parent.Revision, ThreadID: parent.ThreadID,
			Wake: domain.WakeEach, MaxDuration: time.Hour, Name: "child", Condition: "child finished",
		}, clock)
		if err != nil {
			t.Fatal(err)
		}
		waits[id] = wait.ID
	}
	loaded = boundary("parents parked")
	if offered := offeredAttempts(loaded, children); len(offered) != slots {
		t.Fatalf("offered children = %v, want every child once the parents parked", offered)
	}

	// The children run and finish on the worker.
	finishAttempts(t, store, loaded, children)

	// The parents' waits settle; each parent is re-admitted on its worker.
	for _, id := range parents {
		if _, err := store.SettleTaskWait(ctx, waits[id], domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, clock); err != nil {
			t.Fatal(err)
		}
	}
	loaded = boundary("waits settled")
	for _, id := range parents {
		parent := attemptByID(t, loaded, id)
		if parent.Control != domain.ControlResuming {
			t.Fatalf("parent %s control = %s after its wait settled, want resuming", id, parent.Control)
		}
		if assignment := assignmentByID(t, loaded, parent.AssignmentID); assignment.WorkerID != worker {
			t.Fatalf("parent %s resumed on %s, want its own worker", id, assignment.WorkerID)
		}
	}

	// The resumed turns run and finish.
	finishAttempts(t, store, loaded, parents)
	loaded = boundary("all finished")
	for _, id := range append(append([]string(nil), parents...), children...) {
		if attempt := attemptByID(t, loaded, id); attempt.Progress != domain.ProgressSucceeded {
			t.Fatalf("attempt %s ended %s, want every parent and child to complete", id, attempt.Progress)
		}
	}
}

func assertWorkerSlotsWithin(t *testing.T, label string, records sqlite.CoordinatorRecords, worker string, slots int) {
	t.Helper()
	attempts := make(map[string]domain.Attempt, len(records.Attempts))
	for _, attempt := range records.Attempts {
		attempts[attempt.ID] = attempt
	}
	held := 0
	for _, assignment := range records.Assignments {
		if attempt, found := attempts[assignment.AttemptID]; found && assignment.WorkerID == worker &&
			domain.AssignmentOwnsExecutorCapacity(attempt, assignment) {
			held++
		}
	}
	if held > slots {
		t.Fatalf("%s: worker %s holds %d slots of %d", label, worker, held, slots)
	}
}

func offeredAttempts(records sqlite.CoordinatorRecords, ids []string) []string {
	wanted := make(map[string]bool, len(ids))
	for _, id := range ids {
		wanted[id] = true
	}
	var offered []string
	for _, assignment := range records.Assignments {
		if wanted[assignment.AttemptID] && assignment.State == domain.AssignmentOffered {
			offered = append(offered, assignment.AttemptID)
		}
	}
	return offered
}

func attemptByID(t *testing.T, records sqlite.CoordinatorRecords, id string) domain.Attempt {
	t.Helper()
	for _, attempt := range records.Attempts {
		if attempt.ID == id {
			return attempt
		}
	}
	t.Fatalf("attempt %q not found", id)
	return domain.Attempt{}
}

func assignmentByID(t *testing.T, records sqlite.CoordinatorRecords, id string) domain.Assignment {
	t.Helper()
	for _, assignment := range records.Assignments {
		if assignment.ID == id {
			return assignment
		}
	}
	t.Fatalf("assignment %q not found", id)
	return domain.Assignment{}
}

// finishAttempts records what a worker reports for a turn that ran to
// success: the assignment completes and the attempt succeeds.
func finishAttempts(t *testing.T, store *sqlite.Store, records sqlite.CoordinatorRecords, ids []string) {
	t.Helper()
	var finished sqlite.CoordinatorRecords
	for _, id := range ids {
		attempt := attemptByID(t, records, id)
		var assignment domain.Assignment
		for _, candidate := range records.Assignments {
			if candidate.AttemptID == id {
				assignment = candidate
			}
		}
		if assignment.ID == "" {
			t.Fatalf("attempt %s has no assignment to finish", id)
		}
		assignment.State = domain.AssignmentCompleted
		attempt.AssignmentID = assignment.ID
		attempt.Progress, attempt.Control = domain.ProgressSucceeded, domain.ControlStopped
		attempt.Revision++
		finished.Attempts = append(finished.Attempts, attempt)
		finished.Assignments = append(finished.Assignments, assignment)
	}
	if err := store.SaveCoordinatorRecords(context.Background(), finished); err != nil {
		t.Fatal(err)
	}
}
