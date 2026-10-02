package backlogadmin

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// supervisedCancelFixture is a supervised run with a sink: one escalated
// review incident and one active operator hold, the shape run-3ae2f87b was in
// when "campaign cancel" refused it. attempts decides which tasks are still
// live, and activation, when set, is written as the run's overseer activation.
func supervisedCancelFixture(t *testing.T, attempts []domain.Attempt, incidentState domain.IncidentState, activation *domain.Activation) (*sqlite.Store, *Service) {
	t.Helper()
	ctx := context.Background()
	now := adminTestNow
	store := openAdminTestStore(t)
	tasks := []domain.Task{
		{ID: "task-alpha", WorkflowID: "workflow-1", Name: "alpha", Class: domain.TaskClassSurplus},
		{ID: "task-beta", WorkflowID: "workflow-1", Name: "beta", Class: domain.TaskClassSurplus},
	}
	run, err := domain.BindRunSink(domain.WorkflowRun{
		ID: "run-1", WorkflowID: "workflow-1", GraphRevision: 1, Progress: domain.ProgressActive,
		Revision: 4, CreatedAt: now, UpdatedAt: now,
	}, tasks)
	if err != nil {
		t.Fatal(err)
	}
	records := sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: "workflow-1", Version: 1, Name: "workflow", Class: domain.TaskClassSurplus,
			TaskIDs: []string{"task-alpha", "task-beta"},
		}},
		WorkflowRuns: []domain.WorkflowRun{run},
		Tasks:        tasks,
		Attempts:     attempts,
		Incidents: []domain.ReviewIncident{{
			ID: "incident-1", RunID: "run-1", SourceEventID: "event-1", SourceTaskID: "task-alpha",
			Revision: 2, RequiredDisposition: domain.DispositionOperatorAction, State: incidentState,
			Reason: "the overseer ended without a decision", OpenedAt: now,
		}},
	}
	if activation != nil {
		records.Activations = []domain.Activation{*activation}
	}
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutSupervision(ctx, sqlite.SupervisionMaterialization{
		Record: domain.SupervisionRecord{RunID: "run-1", Config: domain.SupervisionConfig{
			Route:            domain.ProviderRoute{ProviderInstanceID: "codex", Model: "gpt-5.6-sol"},
			PromptArtifactID: "artifact-overseer", MaxActivations: 5, MaxTurnsPerActivation: 4,
			ActivationDeadline: time.Hour,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PlaceHold(ctx, sqlite.HoldRequest{
		RunID: "run-1", HoldID: "hold-1", RequestID: "hold-1",
		Actor: domain.Actor{Kind: domain.ActorOperator, Principal: "operator"},
		Scope: domain.HoldScope{Kind: domain.HoldScopeRun}, ExpectedGraphRevision: 1,
		Reason: "look before the next task", PlacedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now.Add(time.Minute) })
	return store, service
}

func terminalAttempt(id, task string, progress domain.ProgressState) domain.Attempt {
	done := adminTestNow
	return domain.Attempt{
		ID: id, WorkflowRunID: "run-1", TaskID: task, Number: 1, Progress: progress,
		Control: domain.ControlStopped, Revision: 3, UpdatedAt: adminTestNow, CompletedAt: &done,
	}
}

func loadedRun(t *testing.T, store *sqlite.Store) domain.WorkflowRun {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range records.WorkflowRuns {
		if run.ID == "run-1" {
			return run
		}
	}
	t.Fatal("run-1 is gone")
	return domain.WorkflowRun{}
}

// requireSupervisionClosedByCancel checks the three things a whole-run cancel
// must leave behind on a supervised run: the incident resolved as cancelled
// under the command's id, the hold released, and the sink settled.
func requireSupervisionClosedByCancel(t *testing.T, store *sqlite.Store, commandID string) {
	t.Helper()
	read, err := store.SupervisionReadSet(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if read == nil || len(read.Incidents) != 1 || len(read.Holds) != 1 {
		t.Fatalf("supervision read set = %#v", read)
	}
	incident := read.Incidents[0]
	if incident.State != domain.IncidentResolved || incident.Resolution == nil ||
		incident.Resolution.Outcome != domain.IncidentOutcomeCancelled || incident.Resolution.RequestID != commandID ||
		incident.Resolution.Actor.Kind != domain.ActorOperator {
		t.Fatalf("incident = %#v, want resolved as cancelled by the command", incident)
	}
	if hold := read.Holds[0]; hold.State != domain.HoldReleased || hold.ReleasedAt == nil {
		t.Fatalf("hold = %#v, want released", hold)
	}
	run := loadedRun(t, store)
	if run.Sink == nil || !run.Sink.Progress.Terminal() || run.Progress != domain.ProgressCancelled || run.CompletedAt == nil {
		t.Fatalf("run = %#v sink = %#v, want the sink settled and the run cancelled", run, run.Sink)
	}
	events, err := store.LoadAuditEvents(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]bool{}
	for _, event := range events {
		kinds[event.Kind] = true
	}
	if !kinds["run-supervision-closed"] || !kinds["sink-settled"] {
		t.Fatalf("audit kinds = %v, want run-supervision-closed and sink-settled", kinds)
	}
}

// run-3ae2f87b: every task was terminal, an escalated incident kept the sink
// open, and "campaign cancel" answered that there was nothing to cancel. The
// operator had to dig the incident and its revision out of --json. One cancel
// now closes the run: it targets the run itself, and its application resolves
// the incident, releases the hold and settles the sink in one transaction.
func TestCancelRunClosesTheSupervisionOfARunWhoseTasksAreAllTerminal(t *testing.T) {
	store, service := supervisedCancelFixture(t, []domain.Attempt{
		terminalAttempt("attempt-alpha", "task-alpha", domain.ProgressSucceeded),
		terminalAttempt("attempt-beta", "task-beta", domain.ProgressCancelled),
	}, domain.IncidentEscalated, nil)
	mutation := cancelRunMutation("cancel-closed-1")
	mutation.ExpectedRevision = loadedRun(t, store).Revision
	response, err := service.Mutate(context.Background(), mutation)
	if err != nil {
		t.Fatalf("a run whose supervision is open was refused: %v", err)
	}
	if response.Command.TargetType != domain.AdminTargetWorkflowRun || response.Command.TargetID != "run-1" {
		t.Fatalf("command = %#v, want it fenced on the run", response.Command)
	}
	report, err := service.ExecutePendingCommands(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Decisions) != 1 || report.Decisions[0].Command.State != domain.AdminCommandApplied {
		t.Fatalf("decisions = %#v", report.Decisions)
	}
	requireSupervisionClosedByCancel(t, store, "cancel-closed-1")

	// Settled is settled: a second cancel says so rather than queueing.
	again := cancelRunMutation("cancel-closed-2")
	again.ExpectedRevision = loadedRun(t, store).Revision
	if _, err := service.Mutate(context.Background(), again); err == nil || !strings.Contains(err.Error(), "settled") {
		t.Fatalf("cancel of a settled run: %v, want a refusal naming the settlement", err)
	}
}

// The usual case: a task is still queued and an incident is open. The same
// application that cancels the task closes the supervision, and because the
// queued task never ran the run is quiescent and the sink settles with it.
func TestCancelRunResolvesOpenIncidentsAndReleasesHoldsWithTheTasks(t *testing.T) {
	queued := domain.Attempt{
		ID: "attempt-beta", WorkflowRunID: "run-1", TaskID: "task-beta", Number: 1,
		Progress: domain.ProgressQueued, Control: domain.ControlUnassigned, Revision: 2, UpdatedAt: adminTestNow,
	}
	store, service := supervisedCancelFixture(t, []domain.Attempt{
		terminalAttempt("attempt-alpha", "task-alpha", domain.ProgressSucceeded), queued,
	}, domain.IncidentOpen, nil)
	if _, err := service.Mutate(context.Background(), cancelRunMutation("cancel-open-1")); err != nil {
		t.Fatal(err)
	}
	report, err := service.ExecutePendingCommands(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Decisions) != 1 || report.Decisions[0].Command.State != domain.AdminCommandApplied {
		t.Fatalf("decisions = %#v", report.Decisions)
	}
	requireSupervisionClosedByCancel(t, store, "cancel-open-1")
}

// A live overseer may be deciding a gate or placing a hold at this moment, and
// a cancel that resolved its incidents underneath it would race those
// decisions. The whole-run cancel is refused, durably, naming the activation
// and the commands to wait for it and to retry.
func TestCancelRunIsRefusedWhileAnOverseerActivationIsLive(t *testing.T) {
	queued := domain.Attempt{
		ID: "attempt-beta", WorkflowRunID: "run-1", TaskID: "task-beta", Number: 1,
		Progress: domain.ProgressQueued, Control: domain.ControlUnassigned, Revision: 2, UpdatedAt: adminTestNow,
	}
	lease := adminTestNow.Add(10 * time.Minute)
	store, service := supervisedCancelFixture(t, []domain.Attempt{
		terminalAttempt("attempt-alpha", "task-alpha", domain.ProgressSucceeded), queued,
	}, domain.IncidentOpen, &domain.Activation{
		ID: "activation-run-1-1", RunID: "run-1", Epoch: 1, DispatchIdentity: "dispatch-1",
		State: domain.ActivationActive, LeaseExpiresAt: &lease,
	})
	if _, err := service.Mutate(context.Background(), cancelRunMutation("cancel-live-1")); err != nil {
		t.Fatal(err)
	}
	report, err := service.ExecutePendingCommands(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Decisions) != 1 {
		t.Fatalf("decisions = %#v", report.Decisions)
	}
	command := report.Decisions[0].Command
	if command.State != domain.AdminCommandRejected {
		t.Fatalf("command = %#v, want it refused while the overseer is live", command)
	}
	for _, want := range []string{"activation-run-1-1", "t3-steward campaign supervision show run-1", "t3-steward campaign cancel run-1"} {
		if !strings.Contains(command.Failure, want) {
			t.Fatalf("failure %q does not name %q", command.Failure, want)
		}
	}
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range records.Attempts {
		if attempt.ID == "attempt-beta" && attempt.Progress != domain.ProgressQueued {
			t.Fatalf("a refused cancel changed %s to %s", attempt.ID, attempt.Progress)
		}
	}
	read, err := store.SupervisionReadSet(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if read.Incidents[0].State != domain.IncidentOpen || read.Holds[0].State != domain.HoldActive {
		t.Fatalf("a refused cancel changed supervision: %#v", read)
	}
}

// After a whole-run cancel the run may wait a few boundaries for a running
// worker to stop before its sink settles. No overseer is woken in that window:
// the run's supervision was closed by the operator, and a fresh activation
// would reopen what the cancel just resolved.
func TestRunsClosedByCancelNamesTheRunOfAnAppliedWholeRunCancel(t *testing.T) {
	records := cancelRunRecords(adminTestNow)
	records.AdminCommands = []domain.AdminCommand{
		{ID: "c-1", Kind: domain.AdminCommandCancel, TargetType: domain.AdminTargetAttempt, TargetID: "attempt-alpha",
			Payload: []byte(`{"scope":"run"}`), State: domain.AdminCommandApplied},
		{ID: "c-2", Kind: domain.AdminCommandCancel, TargetType: domain.AdminTargetWorkflowRun, TargetID: "run-2",
			Payload: []byte(`{"scope":"run"}`), State: domain.AdminCommandApplied},
		{ID: "c-3", Kind: domain.AdminCommandCancel, TargetType: domain.AdminTargetWorkflowRun, TargetID: "run-3",
			Payload: []byte(`{"scope":"run"}`), State: domain.AdminCommandRejected},
		{ID: "c-4", Kind: domain.AdminCommandCancel, TargetType: domain.AdminTargetAttempt, TargetID: "attempt-beta",
			State: domain.AdminCommandApplied},
	}
	closed := RunsClosedByCancel(records)
	if !closed["run-1"] || !closed["run-2"] || closed["run-3"] || len(closed) != 2 {
		t.Fatalf("closed = %v, want run-1 and run-2 only", closed)
	}
}
