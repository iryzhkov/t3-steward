package backlog

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

var projectionTestTime = time.Date(2026, time.September, 12, 12, 0, 0, 0, time.UTC)

// fakeProjectionStore is an in-memory ProjectionStore. It implements the
// optional supervision source only when supervision is set, so the same fixture
// exercises the supervised and the unsupervised path.
type fakeProjectionStore struct {
	records     sqlite.CoordinatorRecords
	supervision map[string]SupervisionProjection
}

func (store *fakeProjectionStore) LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error) {
	return store.records, nil
}

func (store *fakeProjectionStore) CommitWorkflowProjection(_ context.Context, _ sqlite.WorkflowProjectionSnapshot, run domain.WorkflowRun, attempts []domain.Attempt, _ time.Time) error {
	store.records.WorkflowRuns = []domain.WorkflowRun{run}
	store.records.Attempts = attempts
	return nil
}

func (store *fakeProjectionStore) SupervisionProjection(_ context.Context, runID string) (SupervisionProjection, error) {
	return store.supervision[runID], nil
}

func projectionFixture(t *testing.T, states ...domain.ProgressState) *fakeProjectionStore {
	t.Helper()
	tasks := []domain.Task{
		{ID: "a", Name: "a", WorkflowID: "w"},
		{ID: "b", Name: "b", WorkflowID: "w", Needs: []string{"a"}},
	}
	attempts := make([]domain.Attempt, len(tasks))
	for index, task := range tasks {
		attempts[index] = domain.Attempt{
			ID: task.ID + "1", TaskID: task.ID, WorkflowRunID: "r", Number: 1, Revision: 1,
			Progress: states[index], Control: domain.ControlStopped, UpdatedAt: projectionTestTime,
		}
		if states[index] == domain.ProgressBlocked {
			attempts[index].Control = domain.ControlUnassigned
		}
	}
	run, err := domain.BindRunSink(domain.WorkflowRun{
		ID: "r", WorkflowID: "w", Revision: 1, Progress: domain.ProgressActive,
		CreatedAt: projectionTestTime, UpdatedAt: projectionTestTime,
	}, tasks)
	if err != nil {
		t.Fatalf("bind sink: %v", err)
	}
	return &fakeProjectionStore{records: sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "w", Version: 2, Name: "w", Class: domain.TaskClassRequired, CreatedAt: projectionTestTime}},
		WorkflowRuns: []domain.WorkflowRun{run},
		Tasks:        tasks,
		Attempts:     attempts,
	}}
}

func projectedAttempt(t *testing.T, store *fakeProjectionStore, taskID string) domain.Attempt {
	t.Helper()
	for _, attempt := range store.records.Attempts {
		if attempt.TaskID == taskID {
			return attempt
		}
	}
	t.Fatalf("no attempt for task %q", taskID)
	return domain.Attempt{}
}

func TestProjectionSkipsAGateBlockedTaskOnlyWhenItIsUnsupervised(t *testing.T) {
	ctx := context.Background()
	unsupervised := projectionFixture(t, domain.ProgressFailed, domain.ProgressBlocked)
	if _, err := ProjectWorkflowRuns(ctx, unsupervised, projectionTestTime); err != nil {
		t.Fatalf("project unsupervised run: %v", err)
	}
	if got := projectedAttempt(t, unsupervised, "b").Progress; got != domain.ProgressSkipped {
		t.Fatalf("unsupervised descendant = %q, want %q", got, domain.ProgressSkipped)
	}
	if !unsupervised.records.WorkflowRuns[0].Sink.Progress.Terminal() {
		t.Fatal("unsupervised run did not settle")
	}

	supervised := projectionFixture(t, domain.ProgressFailed, domain.ProgressBlocked)
	supervised.supervision = map[string]SupervisionProjection{"r": {
		Snapshot: domain.SupervisionSnapshot{
			RunID: "r", Supervised: true, RouteAvailable: true,
			Gates: []domain.Gate{{
				Definition: domain.GateDefinition{
					ID: "gate-b", Name: "recovery review",
					ObservedTaskIDs: []string{"a"}, ProtectedTaskIDs: []string{"b"},
				},
				RunID: "r", State: domain.GateReadyForReview,
			}},
		},
	}}
	report, err := ProjectWorkflowRuns(ctx, supervised, projectionTestTime)
	if err != nil {
		t.Fatalf("project supervised run: %v", err)
	}
	if got := projectedAttempt(t, supervised, "b").Progress; got != domain.ProgressBlocked {
		t.Fatalf("gate-blocked descendant = %q, want %q", got, domain.ProgressBlocked)
	}
	if supervised.records.WorkflowRuns[0].Sink.Progress.Terminal() {
		t.Fatal("supervised run settled past an undecided gate")
	}
	if len(report.Supervision) != 1 || report.Supervision[0].Status != domain.SupervisedRunHeld {
		t.Fatalf("supervision views = %#v, want one held run", report.Supervision)
	}
	if len(report.Supervision[0].Gates) != 1 || report.Supervision[0].Gates[0].GateID != "gate-b" {
		t.Fatalf("gates = %#v, want the gate state in explain output", report.Supervision[0].Gates)
	}
}

func TestProjectionHoldsSettlementUntilIncidentsResolve(t *testing.T) {
	ctx := context.Background()
	incident := domain.ReviewIncident{
		ID: "incident-a", RunID: "r", SourceEventID: "event-a", SourceTaskID: "a",
		State: domain.IncidentOpen, RequiredDisposition: domain.DispositionConcludeFailure,
		Reason: "task a failed verification", OpenedAt: projectionTestTime,
	}
	store := projectionFixture(t, domain.ProgressSucceeded, domain.ProgressSucceeded)
	store.supervision = map[string]SupervisionProjection{"r": {
		Snapshot:  domain.SupervisionSnapshot{RunID: "r", Supervised: true, RouteAvailable: true},
		Incidents: []domain.ReviewIncident{incident},
	}}
	report, err := ProjectWorkflowRuns(ctx, store, projectionTestTime)
	if err != nil {
		t.Fatalf("project run with an open incident: %v", err)
	}
	if store.records.WorkflowRuns[0].Sink.Progress.Terminal() {
		t.Fatal("settled with an unresolved incident")
	}
	if len(report.Supervision) != 1 || len(report.Supervision[0].OpenIncidents) != 1 ||
		report.Supervision[0].OpenIncidents[0].IncidentID != "incident-a" ||
		report.Supervision[0].Barrier.Reason != domain.SinkBarrierUnresolvedIncident {
		t.Fatalf("supervision view = %#v, want the open incident and the barrier", report.Supervision)
	}

	resolved := incident
	resolved.State = domain.IncidentResolved
	resolved.Resolution = &domain.ResolutionReceipt{
		Actor:   domain.Actor{Kind: domain.ActorOperator, Principal: "operator"},
		Outcome: domain.IncidentOutcomeConcludeFailure, RequestID: "request-a", ResolvedAt: projectionTestTime,
	}
	store.supervision["r"] = SupervisionProjection{
		Snapshot:  domain.SupervisionSnapshot{RunID: "r", Supervised: true, RouteAvailable: true},
		Incidents: []domain.ReviewIncident{resolved},
	}
	if _, err := ProjectWorkflowRuns(ctx, store, projectionTestTime.Add(time.Minute)); err != nil {
		t.Fatalf("project run with a resolved incident: %v", err)
	}
	if got := store.records.WorkflowRuns[0].Sink.Progress; got != domain.ProgressSucceeded {
		t.Fatalf("sink = %q, want ordinary settlement once decisions resolve", got)
	}
}

func TestRunSupervisionViewStatusPrecedence(t *testing.T) {
	gate := func(state domain.GateState) domain.Gate {
		return domain.Gate{
			Definition: domain.GateDefinition{
				ID: "gate-b", Name: "review",
				ObservedTaskIDs: []string{"a"}, ProtectedTaskIDs: []string{"b"},
			},
			RunID: "r", State: state,
		}
	}
	attempt := func(progress domain.ProgressState, control domain.ControlState) domain.Attempt {
		return domain.Attempt{ID: "b1", TaskID: "b", WorkflowRunID: "r", Number: 1, Progress: progress, Control: control}
	}
	escalated := domain.ReviewIncident{ID: "incident-b", RunID: "r", State: domain.IncidentEscalated, RequiredDisposition: domain.DispositionOperatorAction}
	for _, test := range []struct {
		name      string
		attempt   domain.Attempt
		gates     []domain.Gate
		incidents []domain.ReviewIncident
		want      domain.SupervisedRunStatus
	}{
		{"active", attempt(domain.ProgressActive, domain.ControlRunning), []domain.Gate{gate(domain.GateReadyForReview)}, []domain.ReviewIncident{escalated}, domain.SupervisedRunActive},
		{"waiting external", attempt(domain.ProgressWaitingExternal, domain.ControlWaitingExternal), []domain.Gate{gate(domain.GateReadyForReview)}, []domain.ReviewIncident{escalated}, domain.SupervisedRunWaitingExternal},
		{"runnable", attempt(domain.ProgressReady, domain.ControlUnassigned), []domain.Gate{gate(domain.GateAccepted)}, []domain.ReviewIncident{escalated}, domain.SupervisedRunRunnable},
		{"escalated outranks held", attempt(domain.ProgressReady, domain.ControlUnassigned), []domain.Gate{gate(domain.GateHeld)}, []domain.ReviewIncident{escalated}, domain.SupervisedRunEscalated},
		{"held", attempt(domain.ProgressReady, domain.ControlUnassigned), []domain.Gate{gate(domain.GateReadyForReview)}, nil, domain.SupervisedRunHeld},
		{"blocked", attempt(domain.ProgressBlocked, domain.ControlUnassigned), nil, nil, domain.SupervisedRunBlocked},
	} {
		t.Run(test.name, func(t *testing.T) {
			projection := SupervisionProjection{
				Snapshot: domain.SupervisionSnapshot{
					RunID: "r", Supervised: true, RouteAvailable: true, Gates: test.gates,
				},
				Incidents: test.incidents,
			}
			attempts := []domain.Attempt{test.attempt}
			state := DAGState{Run: domain.WorkflowRun{ID: "r", WorkflowID: "w"}, Attempts: attempts}
			got := runSupervisionView(projection, state, attempts, domain.WorkflowRun{ID: "r", WorkflowID: "w"})
			if got.Status != test.want {
				t.Fatalf("status = %q, want %q", got.Status, test.want)
			}
		})
	}
}
