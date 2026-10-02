package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// A whole-run cancel closes the run's supervision but may leave the run
// waiting a few boundaries for a cancelled worker to stop before its sink
// settles. The review event that was pending when the operator cancelled must
// not wake an overseer in that window, nor advance a gate: either would reopen
// what the cancel resolved. The boundaries read the closure the cancel
// recorded on the supervision record.
func TestNoOverseerIsWokenForARunCancelledWhole(t *testing.T) {
	ctx := context.Background()
	fixture := newActivationLeaseFixture(t)
	fixture.superviseRun(t)
	records, err := fixture.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	run := records.WorkflowRuns[0]
	bound, err := domain.BindRunSink(run, records.Tasks)
	if err != nil {
		t.Fatal(err)
	}
	bound.Revision++
	// The protected task is running on a worker that has not yet stopped, so
	// the cancel cannot settle the sink in its own transaction.
	var running domain.Attempt
	for _, attempt := range records.Attempts {
		if attempt.ID == "attempt-protected" {
			running = attempt
		}
	}
	running.Progress, running.Control, running.AssignmentID = domain.ProgressActive, domain.ControlRunning, "assignment-protected"
	running.Revision++
	// The DAG names a dependency by task name; the shared fixture spells it
	// by id, which only the cancellation DAG reads.
	tasks := records.Tasks
	for i := range tasks {
		if tasks[i].ID == "task-protected" {
			tasks[i].Needs = []string{"producer"}
		}
	}
	if err := fixture.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{bound}, Attempts: []domain.Attempt{running}, Tasks: tasks,
	}); err != nil {
		t.Fatal(err)
	}
	service, err := backlogadmin.New(fixture.store, localAdminAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return fixture.now })
	if _, err := service.Mutate(ctx, backlogadmin.Mutation{
		Version: backlogadmin.Version, Principal: backlogadmin.Principal{ID: "operator", Roles: []string{"local-admin"}}, ID: "cancel-whole-run",
		Kind: domain.AdminCommandCancel, WorkflowRunID: activationLeaseRun, ExpectedRevision: running.Revision,
		Reason: "obsolete", Payload: json.RawMessage(`{"scope":"run"}`),
	}); err != nil {
		t.Fatal(err)
	}
	report, err := service.ExecutePendingCommands(ctx)
	if err != nil || len(report.Decisions) != 1 || report.Decisions[0].Command.State != domain.AdminCommandApplied {
		t.Fatalf("report = %#v, err = %v", report, err)
	}
	after, err := fixture.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.WorkflowRuns[0].Progress.Terminal() {
		t.Fatalf("the run settled at once; this test needs it waiting: %#v", after.WorkflowRuns[0])
	}
	fixture.coordinator.Tick(ctx)
	fixture.coordinator.DispatchActivations(ctx, admittingQuota())
	state, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if state.Record.ClosedByCancel == nil {
		t.Fatalf("record = %#v, want the closure recorded", state.Record)
	}
	if state.Activation.State == domain.ActivationPendingDispatch || state.Activation.State == domain.ActivationActive {
		t.Fatalf("an overseer was woken for a run cancelled whole: %+v", state.Activation)
	}
}
