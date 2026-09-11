package backlog

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

var coordinatorTestTime = time.Date(2026, time.September, 10, 20, 0, 0, 0, time.UTC)

func TestFleetCoordinatorWithholdsNewWorkAtFinalQuotaBoundary(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	snapshot := coordinatorSnapshot(1)
	attempt := domain.Attempt{
		ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1,
		Progress: domain.ProgressActive, Control: domain.ControlPreparing,
		Revision: 1, AssignmentID: "assignment-1", UpdatedAt: coordinatorTestTime,
	}
	assignment := domain.Assignment{
		ID: "assignment-1", AttemptID: attempt.ID, WorkerID: snapshot.WorkerID,
		WorkerEpoch: snapshot.WorkerEpoch, State: domain.AssignmentClaimed, Epoch: 1,
		Route:      domain.ProviderRoute{QuotaPoolID: "pool"},
		LeaseToken: "lease-1", DispatchToken: "dispatch-1",
		LeaseExpiresAt: coordinatorTestTime.Add(time.Hour),
		CreatedAt:      coordinatorTestTime, UpdatedAt: coordinatorTestTime,
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	transport := &deduplicatingWorkerTransport{}
	coordinator := FleetCoordinator{Store: store, Now: func() time.Time {
		return coordinatorTestTime.Add(time.Minute)
	}}
	closed, err := coordinator.ReconcileWorkerCommandsWithAdmission(
		ctx, snapshot, transport, WorkerAdmissionPolicy{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(closed.Planned) != 0 || len(closed.Pending) != 0 ||
		len(closed.Withheld) != 1 || closed.Withheld[0].Kind != domain.WorkerCommandPrepare ||
		len(transport.executions) != 0 {
		t.Fatalf("closed report = %#v executions=%v", closed, transport.executions)
	}
	open, err := coordinator.ReconcileWorkerCommandsWithAdmission(
		ctx, snapshot, transport,
		WorkerAdmissionPolicy{OpenQuotaPools: map[string]struct{}{"pool": {}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(open.Pending) != 1 || open.Pending[0].Kind != domain.WorkerCommandPrepare ||
		len(open.Acknowledgements) != 1 || transport.executions[open.Pending[0].ID] != 1 {
		t.Fatalf("open report = %#v executions=%v", open, transport.executions)
	}
}

func TestFleetCoordinatorCommitsPlanAndReplaysLostCommandResponse(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	task := testTask("alpha")
	task.Routes = []domain.ProviderRoute{{ProviderInstanceID: "codex", Model: "gpt"}}
	input := plannerInput([]domain.Task{task}, nil)
	input.Now = coordinatorTestTime
	input.Workflows[0].State.Run.CreatedAt = coordinatorTestTime
	input.Workflows[0].State.Run.UpdatedAt = coordinatorTestTime
	input.Workflows[0].State.Attempts[0].UpdatedAt = coordinatorTestTime
	input.QuotaPools = []domain.QuotaPool{routingPool("pool", 2, 0, "codex")}
	input.RouteEstimates = []RouteEstimate{
		routingEstimate("alpha-1", "worker-a", "codex", "gpt", nil, 10),
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{input.Workflows[0].Workflow},
		WorkflowRuns: []domain.WorkflowRun{input.Workflows[0].State.Run},
		Tasks:        []domain.Task{task},
		Attempts:     input.Workflows[0].State.Attempts,
		QuotaPools:   input.QuotaPools,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot := coordinatorSnapshot(1)
	if err := store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}

	coordinator := FleetCoordinator{Store: store, Now: func() time.Time { return coordinatorTestTime }}
	planned, err := coordinator.PlanAndCommit(ctx, input)
	if err != nil {
		t.Fatalf("PlanAndCommit: %v", err)
	}
	if len(planned.Assignments) != 1 {
		t.Fatalf("assignments = %#v, plan = %#v, want one", planned.Assignments, planned.Plan)
	}
	assignment := planned.Assignments[0]
	if assignment.Estimate == nil || assignment.Estimate.RemainingCost != 10 {
		t.Fatalf("assignment durable estimate = %#v", assignment.Estimate)
	}
	restartedRecords, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(restartedRecords.Assignments) != 1 || restartedRecords.Assignments[0].Estimate == nil ||
		restartedRecords.Assignments[0].Estimate.RemainingCost != 10 {
		t.Fatalf("restarted durable assignments = %#v", restartedRecords.Assignments)
	}
	claimed, err := store.ClaimAssignment(ctx, domain.AssignmentClaimRequest{
		CoordinatorEpoch: 1,
		WorkerID:         "worker-a",
		WorkerEpoch:      "worker-epoch-1",
		AssignmentID:     assignment.ID,
		AssignmentEpoch:  assignment.Epoch,
		LeaseToken:       assignment.LeaseToken,
		ClaimedAt:        coordinatorTestTime.Add(time.Minute),
		LeaseExpiresAt:   coordinatorTestTime.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("ClaimAssignment: %v", err)
	}

	transport := &deduplicatingWorkerTransport{dropFirstResponse: true}
	coordinator.Now = func() time.Time { return coordinatorTestTime.Add(2 * time.Minute) }
	first, err := coordinator.ReconcileWorkerCommands(ctx, snapshot, transport)
	if !errors.Is(err, errLostWorkerResponse) {
		t.Fatalf("first delivery error = %v, want lost response", err)
	}
	if len(first.Pending) != 1 || first.Pending[0].Kind != domain.WorkerCommandPrepare {
		t.Fatalf("first pending = %#v, want prepare", first.Pending)
	}

	restarted := FleetCoordinator{Store: store, Now: func() time.Time { return coordinatorTestTime.Add(3 * time.Minute) }}
	second, err := restarted.ReconcileWorkerCommands(ctx, snapshot, transport)
	if err != nil {
		t.Fatalf("replayed delivery: %v", err)
	}
	if len(second.Acknowledgements) != 1 ||
		second.Acknowledgements[0].CommandID != first.Pending[0].ID {
		t.Fatalf("replay acknowledgements = %#v, want command %q", second.Acknowledgements, first.Pending[0].ID)
	}
	if transport.executions[first.Pending[0].ID] != 1 {
		t.Fatalf("prepare executions = %d, want one", transport.executions[first.Pending[0].ID])
	}

	third, err := restarted.ReconcileWorkerCommands(ctx, snapshot, transport)
	if err != nil {
		t.Fatalf("dispatch delivery: %v", err)
	}
	if len(third.Pending) != 1 || third.Pending[0].Kind != domain.WorkerCommandDispatch {
		t.Fatalf("third pending = %#v, want dispatch", third.Pending)
	}
	fourth, err := restarted.ReconcileWorkerCommands(ctx, snapshot, transport)
	if err != nil {
		t.Fatalf("settled delivery: %v", err)
	}
	if len(fourth.Pending) != 0 || len(fourth.Planned) != 0 {
		t.Fatalf("settled report = %#v, want no duplicate commands", fourth)
	}

	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Assignments) != 1 ||
		records.Assignments[0].ID != claimed.ID ||
		records.Assignments[0].DispatchToken == "" {
		t.Fatalf("durable assignments = %#v", records.Assignments)
	}
	commandRecords, err := store.LoadWorkerCommandRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(commandRecords) != 2 {
		t.Fatalf("command records = %#v, want prepare and dispatch", commandRecords)
	}
	for _, record := range commandRecords {
		if record.Acknowledgement == nil || !record.Acknowledgement.Accepted {
			t.Fatalf("command record = %#v, want accepted acknowledgement", record)
		}
	}
}

func TestFleetCoordinatorRecoversLostDispatchAcknowledgementWithoutDuplicateExecution(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	snapshot := coordinatorSnapshot(1)
	attempt := domain.Attempt{
		ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1,
		Progress: domain.ProgressActive, Control: domain.ControlPreparing,
		Revision: 1, AssignmentID: "assignment-1", UpdatedAt: coordinatorTestTime,
	}
	assignment := domain.Assignment{
		ID: "assignment-1", AttemptID: attempt.ID, WorkerID: snapshot.WorkerID,
		WorkerEpoch: snapshot.WorkerEpoch, State: domain.AssignmentClaimed, Epoch: 1,
		LeaseToken: "lease-1", DispatchToken: "dispatch-1",
		LeaseExpiresAt: coordinatorTestTime.Add(time.Hour),
		CreatedAt:      coordinatorTestTime, UpdatedAt: coordinatorTestTime,
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	transport := &deduplicatingWorkerTransport{}
	coordinator := FleetCoordinator{Store: store, Now: func() time.Time {
		return coordinatorTestTime.Add(2 * time.Minute)
	}}
	prepare, err := coordinator.ReconcileWorkerCommands(ctx, snapshot, transport)
	if err != nil {
		t.Fatal(err)
	}
	if len(prepare.Acknowledgements) != 1 ||
		prepare.Pending[0].Kind != domain.WorkerCommandPrepare {
		t.Fatalf("prepare report = %#v", prepare)
	}

	transport.dropFirstResponse = true
	coordinator.Now = func() time.Time { return coordinatorTestTime.Add(3 * time.Minute) }
	dispatch, err := coordinator.ReconcileWorkerCommands(ctx, snapshot, transport)
	if !errors.Is(err, errLostWorkerResponse) ||
		len(dispatch.Pending) != 1 || dispatch.Pending[0].Kind != domain.WorkerCommandDispatch {
		t.Fatalf("dispatch report = %#v, error = %v", dispatch, err)
	}
	dispatchID := dispatch.Pending[0].ID
	if transport.executions[dispatchID] != 1 {
		t.Fatalf("dispatch executions after lost response = %d", transport.executions[dispatchID])
	}

	reconnected := coordinatorSnapshot(2)
	reconnected.ObservedAt = coordinatorTestTime.Add(4 * time.Minute)
	reconnected.ValidUntil = coordinatorTestTime.Add(time.Hour)
	reconnected.Assignments = []domain.WorkerAssignmentObservation{{
		AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
		State: domain.AssignmentClaimed, Control: domain.ControlRunning,
		ThreadID: "thread-1", ObservedAt: reconnected.ObservedAt,
	}}
	if err := store.SaveWorkerSnapshot(ctx, reconnected); err != nil {
		t.Fatal(err)
	}
	coordinator.Now = func() time.Time { return coordinatorTestTime.Add(5 * time.Minute) }
	recovered, err := coordinator.ReconcileWorkerCommands(ctx, reconnected, transport)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered.Reconciled) != 1 || len(recovered.Pending) != 1 ||
		recovered.Pending[0].ID != dispatchID {
		t.Fatalf("recovered report = %#v", recovered)
	}
	if transport.executions[dispatchID] != 1 {
		t.Fatalf("dispatch executions after replay = %d, want one", transport.executions[dispatchID])
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if records.Attempts[0].Control != domain.ControlRunning ||
		records.Attempts[0].ThreadID != "thread-1" {
		t.Fatalf("reconciled attempt = %#v", records.Attempts[0])
	}
}

func TestPlanWorkerCommandsDerivesStableLifecycleCommands(t *testing.T) {
	snapshot := coordinatorSnapshot(7)
	assignment := domain.Assignment{
		ID: "assignment-1", AttemptID: "attempt-1", WorkerID: snapshot.WorkerID,
		WorkerEpoch: snapshot.WorkerEpoch, State: domain.AssignmentClaimed, Epoch: 2,
	}
	attempt := domain.Attempt{ID: "attempt-1", Control: domain.ControlPreparing}
	records := sqlite.CoordinatorRecords{
		Assignments: []domain.Assignment{assignment},
		Attempts:    []domain.Attempt{attempt},
	}
	prepare, err := PlanWorkerCommands(records, snapshot, nil, coordinatorTestTime)
	if err != nil {
		t.Fatal(err)
	}
	if len(prepare) != 1 || prepare[0].Kind != domain.WorkerCommandPrepare {
		t.Fatalf("prepare plan = %#v", prepare)
	}
	acknowledgement := workerAcknowledgement(snapshot, prepare[0])
	history := []domain.WorkerCommandRecord{{
		Command: prepare[0], Acknowledgement: &acknowledgement,
	}}
	dispatch, err := PlanWorkerCommands(records, snapshot, history, coordinatorTestTime.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(dispatch) != 1 || dispatch[0].Kind != domain.WorkerCommandDispatch {
		t.Fatalf("dispatch plan = %#v", dispatch)
	}

	stopped := records
	stopped.Attempts = []domain.Attempt{{ID: "attempt-1", Control: domain.ControlStopped}}
	stop, err := PlanWorkerCommands(stopped, snapshot, history, coordinatorTestTime)
	if err != nil {
		t.Fatal(err)
	}
	if len(stop) != 1 || stop[0].Kind != domain.WorkerCommandStop {
		t.Fatalf("stop plan = %#v", stop)
	}

	completedSnapshot := snapshot
	completedSnapshot.Assignments = []domain.WorkerAssignmentObservation{{
		AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
		State: domain.AssignmentCompleted, ObservedAt: coordinatorTestTime,
	}}
	collect, err := PlanWorkerCommands(records, completedSnapshot, history, coordinatorTestTime)
	if err != nil {
		t.Fatal(err)
	}
	if len(collect) != 1 || collect[0].Kind != domain.WorkerCommandCollect {
		t.Fatalf("collect plan = %#v", collect)
	}

	repeated, err := PlanWorkerCommands(records, snapshot, nil, coordinatorTestTime.Add(59*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if repeated[0].ID != prepare[0].ID {
		t.Fatalf("command ID changed: %q != %q", repeated[0].ID, prepare[0].ID)
	}
}

func coordinatorSnapshot(sequence int64) domain.WorkerSnapshot {
	return domain.WorkerSnapshot{
		WorkerID: "worker-a", WorkerEpoch: "worker-epoch-1",
		CoordinatorEpoch: 1, Sequence: sequence, Connected: true,
		Inventory: domain.WorkerInventory{
			ID: "worker-a", AcceptBacklog: true, Health: domain.WorkerHealthReady,
			Projects: []domain.WorkerProjectInventory{{
				Name: "project", Available: true, UpdatedAt: coordinatorTestTime,
			}},
			Providers: []domain.WorkerProviderInventory{
				routingProvider("codex", "pool", true, "gpt"),
			},
			ObservedAt: coordinatorTestTime,
		},
		ObservedAt: coordinatorTestTime,
		ValidUntil: coordinatorTestTime.Add(time.Hour),
	}
}

var errLostWorkerResponse = errors.New("lost worker response")

type deduplicatingWorkerTransport struct {
	dropFirstResponse bool
	executions        map[string]int
	acknowledgements  map[string]domain.WorkerAcknowledgement
}

func (t *deduplicatingWorkerTransport) DeliverWorkerCommands(
	_ context.Context,
	snapshot domain.WorkerSnapshot,
	commands []domain.WorkerCommand,
) ([]domain.WorkerAcknowledgement, error) {
	if t.executions == nil {
		t.executions = make(map[string]int)
		t.acknowledgements = make(map[string]domain.WorkerAcknowledgement)
	}
	acknowledgements := make([]domain.WorkerAcknowledgement, 0, len(commands))
	for _, command := range commands {
		acknowledgement, exists := t.acknowledgements[command.ID]
		if !exists {
			t.executions[command.ID]++
			acknowledgement = workerAcknowledgement(snapshot, command)
			t.acknowledgements[command.ID] = acknowledgement
		}
		acknowledgements = append(acknowledgements, acknowledgement)
	}
	if t.dropFirstResponse {
		t.dropFirstResponse = false
		return nil, errLostWorkerResponse
	}
	return acknowledgements, nil
}

func workerAcknowledgement(snapshot domain.WorkerSnapshot, command domain.WorkerCommand) domain.WorkerAcknowledgement {
	return domain.WorkerAcknowledgement{
		CommandID: command.ID, WorkerID: command.WorkerID,
		WorkerEpoch: command.WorkerEpoch, CoordinatorEpoch: command.CoordinatorEpoch,
		AssignmentID: command.AssignmentID, AssignmentEpoch: command.AssignmentEpoch,
		WorkerSequence: snapshot.Sequence, Accepted: true,
		AcknowledgedAt: coordinatorTestTime.Add(3 * time.Minute),
	}
}
