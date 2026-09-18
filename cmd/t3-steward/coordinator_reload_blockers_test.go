package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// F-2 (step A): a refused catalog change names every blocking assignment with
// its worker, attempt, the attempt's progress and control, the phase the
// worker last reported, and the command that unblocks it, in the error and in
// the receipt. On the base the message was "worker W has retained assignment
// A; drain and settle before changing its execution catalog": one assignment,
// no attempt, no command, and no receipt at all.
func TestCoordinatorReloadRefusalNamesEveryBlockerAndItsUnblockingCommand(t *testing.T) {
	fixture := newReloadFixture(t)
	ctx := context.Background()
	workerID := qualificationWorkerID()
	now := time.Now().UTC()
	epoch, err := fixture.store.AcquireCoordinator(ctx, fixture.cfg.BacklogV2.Coordinator.ID)
	if err != nil {
		t.Fatal(err)
	}
	attempts := []domain.Attempt{
		{ID: "attempt-paused", WorkflowRunID: "run-1", TaskID: "implement", Number: 1, Progress: domain.ProgressActive, Control: domain.ControlPaused, Revision: 1, AssignmentID: "assignment-paused", UpdatedAt: now},
		{ID: "attempt-parked", WorkflowRunID: "run-2", TaskID: "review", Number: 1, Progress: domain.ProgressWaitingExternal, Control: domain.ControlWaitingExternal, Revision: 1, AssignmentID: "assignment-parked", UpdatedAt: now},
		{ID: "attempt-running", WorkflowRunID: "run-3", TaskID: "build", Number: 1, Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 1, AssignmentID: "assignment-running", UpdatedAt: now},
		{ID: "attempt-done", WorkflowRunID: "run-4", TaskID: "done", Number: 1, Progress: domain.ProgressSucceeded, Control: domain.ControlStopped, Revision: 1, AssignmentID: "assignment-done", UpdatedAt: now},
	}
	assignments := []domain.Assignment{
		{ID: "assignment-paused", AttemptID: "attempt-paused", WorkerID: workerID, WorkerEpoch: "worker-1", Epoch: 1, State: domain.AssignmentClaimed, DispatchToken: "dispatch-paused", CreatedAt: now, LeaseExpiresAt: now.Add(time.Hour)},
		{ID: "assignment-parked", AttemptID: "attempt-parked", WorkerID: workerID, WorkerEpoch: "worker-1", Epoch: 1, State: domain.AssignmentClaimed, DispatchToken: "dispatch-parked", CreatedAt: now, LeaseExpiresAt: now.Add(time.Hour)},
		{ID: "assignment-running", AttemptID: "attempt-running", WorkerID: workerID, WorkerEpoch: "worker-1", Epoch: 1, State: domain.AssignmentClaimed, DispatchToken: "dispatch-running", CreatedAt: now, LeaseExpiresAt: now.Add(time.Hour)},
		{ID: "assignment-done", AttemptID: "attempt-done", WorkerID: workerID, WorkerEpoch: "worker-1", Epoch: 1, State: domain.AssignmentCompleted, DispatchToken: "dispatch-done", CreatedAt: now, LeaseExpiresAt: now.Add(time.Hour)},
	}
	if err := fixture.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: attempts, Assignments: assignments}); err != nil {
		t.Fatal(err)
	}
	// The worker's last snapshot carries the journal phase of each attempt,
	// which is what tells paused from parked from running on the worker side.
	snapshot := domain.WorkerSnapshot{
		WorkerID: workerID, WorkerEpoch: "worker-1", CoordinatorEpoch: epoch, Sequence: 1, Connected: true,
		Inventory: domain.WorkerInventory{ID: workerID, Epoch: "worker-1", AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: now},
		Assignments: []domain.WorkerAssignmentObservation{
			{AssignmentID: "assignment-paused", AssignmentEpoch: 1, State: domain.AssignmentClaimed, Control: domain.ControlPaused, Journal: &domain.WorkerJournalExcerpt{Phase: "stopped", PauseReason: "claudeAgent/claude/seven_day at 97%", UpdatedAt: now}, ObservedAt: now},
			{AssignmentID: "assignment-parked", AssignmentEpoch: 1, State: domain.AssignmentClaimed, Control: domain.ControlWaitingExternal, Journal: &domain.WorkerJournalExcerpt{Phase: "waiting-external", UpdatedAt: now}, ObservedAt: now},
			{AssignmentID: "assignment-running", AssignmentEpoch: 1, State: domain.AssignmentClaimed, Control: domain.ControlRunning, Journal: &domain.WorkerJournalExcerpt{Phase: "running", UpdatedAt: now}, ObservedAt: now},
		},
		ObservedAt: now, ValidUntil: now.Add(time.Minute),
	}
	if err := fixture.store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	next := cloneReloadConfig(t, fixture.cfg)
	project := next.BacklogV2.Projects["steward"]
	project.DefaultRef = "release"
	next.BacklogV2.Projects["steward"] = project
	writeReloadConfig(t, fixture.cfg.Path, next)

	_, blockers, err := loadCoordinatorReload(ctx, fixture.cfg, fixture.store)
	if err == nil {
		t.Fatal("a catalog change on a worker with retained work was accepted")
	}
	message := err.Error()
	for _, want := range []string{
		"3 retained assignment(s) on 1 worker(s)",
		"assignment assignment-paused attempt attempt-paused (active/paused, worker journal stopped): wait for the pause to lift and the attempt to settle, or t3-steward backlog cancel run-1/implement --reason TEXT",
		"assignment assignment-parked attempt attempt-parked (waiting-external/waiting-external, worker journal waiting-external): wait for the task wait to settle and the attempt to finish, or t3-steward backlog cancel run-2/review --reason TEXT",
		"assignment assignment-running attempt attempt-running (active/running, worker journal running): let the attempt settle, or t3-steward backlog cancel run-3/build --reason TEXT",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("refusal lacks %q:\n%s", want, message)
		}
	}
	if strings.Contains(message, "assignment-done") {
		t.Fatalf("a completed assignment blocks the reload:\n%s", message)
	}
	if len(blockers) != 3 {
		t.Fatalf("blockers = %+v, want the three retained assignments", blockers)
	}
	parked := blockers[0]
	if parked.AssignmentID != "assignment-parked" || parked.AttemptID != "attempt-parked" || parked.WorkerID != workerID ||
		parked.Progress != "waiting-external" || parked.Control != "waiting-external" || parked.JournalPhase != "waiting-external" ||
		!strings.Contains(parked.Unblock, "t3-steward backlog cancel run-2/review --reason TEXT") {
		t.Fatalf("parked blocker = %+v", parked)
	}
	paused := blockers[1]
	if paused.AssignmentID != "assignment-paused" || paused.Control != "paused" || paused.JournalPhase != "stopped" || !strings.HasPrefix(paused.Unblock, "wait for the pause to lift") {
		t.Fatalf("paused blocker = %+v", paused)
	}

	// The same blockers reach the receipt, in the same order.
	decision := evaluateCoordinatorReload(ctx, fixture.cfg, fixture.logger, fixture.store, fixture.receipts)
	if decision.proceed {
		t.Fatal("the reload proceeded past its blockers")
	}
	receipt := fixture.readReceipt(t)
	if receipt.Outcome != backlogadmin.ReloadRejected || len(receipt.Blockers) != 3 || receipt.Blockers[1].Unblock != paused.Unblock || receipt.Error != message {
		t.Fatalf("receipt = %+v", receipt)
	}

	// An untouched worker's retained work never blocks: a policy-only change
	// that leaves this worker's catalog revision alone is accepted.
	policy := cloneReloadConfig(t, fixture.cfg)
	policy.BacklogV2.Scheduling.Interval += 1
	writeReloadConfig(t, fixture.cfg.Path, policy)
	if _, blockers, err := loadCoordinatorReload(ctx, fixture.cfg, fixture.store); err != nil || len(blockers) != 0 {
		t.Fatalf("a change outside the worker's catalog was refused: %v (%+v)", err, blockers)
	}
}

// An assignment whose attempt record is missing is still named, with a generic
// unblocking action, rather than dropped from the refusal.
func TestCoordinatorReloadBlockerWithoutAnAttemptRecordIsStillNamed(t *testing.T) {
	blocker := reloadUnblockAction(domain.Attempt{}, false)
	if !strings.Contains(blocker, "t3-steward backlog cancel <run>/<task> --reason TEXT") || !strings.Contains(blocker, "drain the worker") {
		t.Fatalf("generic action = %q", blocker)
	}
	message := reloadBlockersMessage([]backlogadmin.ReloadBlocker{{WorkerID: "w", AssignmentID: "a", AttemptID: "t", Unblock: blocker}})
	if !strings.Contains(message, "attempt t (attempt record missing)") {
		t.Fatalf("message = %q", message)
	}
}
