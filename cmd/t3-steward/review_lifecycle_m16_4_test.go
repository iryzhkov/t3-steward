package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type reviewLifecycleAuthorizer struct{}

func (reviewLifecycleAuthorizer) Authorize(context.Context, backlogadmin.Principal, backlogadmin.Action) error {
	return nil
}

// Exercise the same worker projection that calls completedWorkerState, retaining
// its store snapshot fence rather than manually installing completed fields.
func reviewLifecycleWorkerTransition(t *testing.T, store *sqlite.Store, control domain.ControlState, completed bool) {
	t.Helper()
	ctx := context.Background()
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var parent domain.Attempt
	var assignment domain.Assignment
	for _, a := range records.Attempts {
		if a.ID == "parent" {
			parent = a
		}
	}
	for _, a := range records.Assignments {
		if a.ID == parent.AssignmentID {
			assignment = a
		}
	}
	now := time.Now().UTC()
	state := domain.AssignmentClaimed
	if completed {
		state = domain.AssignmentCompleted
	}
	snapshot := domain.WorkerSnapshot{WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch, CoordinatorEpoch: 1, Sequence: 1, Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Hour), Inventory: domain.WorkerInventory{ID: assignment.WorkerID, Health: domain.WorkerHealthReady}, Assignments: []domain.WorkerAssignmentObservation{{AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, State: state, Control: control, ThreadID: parent.ThreadID, ObservedAt: now}}}
	if err = store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	transitions, err := backlog.PlanWorkerStateTransitions(records, snapshot, nil, now)
	if err != nil || len(transitions) != 1 {
		t.Fatalf("worker transition: %v %#v", err, transitions)
	}
	if _, err = store.CommitWorkerStateTransitions(ctx, transitions); err != nil {
		t.Fatal(err)
	}
}

func reviewLifecyclePlanner(store *sqlite.Store, epoch int64) coordinatorPlanner {
	return coordinatorPlanner{store: store, coordinator: backlog.FleetCoordinator{Store: store}, epoch: epoch, maxWorkerSnapshotAge: time.Hour, maxQuotaObservationAge: time.Hour, deadlineRiskWindow: time.Hour, checkpointMargin: time.Minute}
}

func TestReviewLifecycleCompletedWorkerDoneAndRestart(t *testing.T) {
	for _, success := range []bool{true, false} {
		t.Run(fmt.Sprint(success), func(t *testing.T) {
			ctx := context.Background()
			store, _, receipt, path := reviewRuntimeProductionFixture(t)
			recordsBefore, err := store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var offered []domain.Assignment
			var offeredAttempts []domain.Attempt
			for _, child := range recordsBefore.Attempts {
				if child.WorkflowRunID != receipt.Graph.Run.ID {
					continue
				}
				var route domain.ProviderRoute
				for _, task := range recordsBefore.Tasks {
					if task.ID == child.TaskID {
						route = task.Routes[0]
					}
				}
				child.AssignmentID = "offered-" + child.ID
				child.Revision++
				offeredAttempts = append(offeredAttempts, child)
				offered = append(offered, domain.Assignment{ID: child.AssignmentID, AttemptID: child.ID, WorkerID: "review-worker", WorkerEpoch: "review-worker-session", Epoch: 1, State: domain.AssignmentOffered, LeaseToken: "child-lease-" + child.ID, DispatchToken: "child-dispatch-" + child.ID, Route: route, Project: "repo", ExecutionRole: domain.ExecutionRoleExecutor})
			}
			if len(offered) == 0 {
				t.Fatal("fixture has no child attempts")
			}
			if err = store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Assignments: offered, Attempts: offeredAttempts}); err != nil {
				t.Fatal(err)
			}
			reviewLifecycleWorkerTransition(t, store, domain.ControlStopped, true)
			transitions, err := backlog.ReconcileTurnOutcomes(ctx, store, []domain.TurnOutcome{{ID: "done-parent", AttemptID: "parent", Marker: domain.TurnOutcomeDone, VerificationPassed: success, Failure: "executor verification failed", ObservedAt: time.Now().UTC()}}, time.Now().UTC())
			if err != nil || len(transitions) != 1 {
				t.Fatalf("done transition: %v %#v", err, transitions)
			}
			if err = store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = sqlite.OpenMigrated(path)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			planner := reviewLifecyclePlanner(store, 1)
			if _, err = planner.Tick(ctx, backlog.QuotaBridgeReport{ChecksDisabled: true}); err != nil {
				t.Fatal(err)
			}
			round, err := store.GetReviewRound(ctx, receipt.Checkpoint.RoundID)
			if err != nil {
				t.Fatal(err)
			}
			if !round.Terminal() {
				t.Fatal("first restart tick left round open")
			}
			for _, member := range round.Reviewers {
				if member.State != "failed" || member.Failure != "parent attempt ended" {
					t.Fatalf("member: %+v", member)
				}
			}
			records, err := store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, a := range records.Attempts {
				if a.WorkflowRunID == receipt.Graph.Run.ID && a.Progress != domain.ProgressCancelled {
					t.Fatalf("child not cancelled: %+v", a)
				}
			}
			for _, initial := range offered {
				released := false
				for _, assignment := range records.Assignments {
					if assignment.ID == initial.ID && assignment.State == domain.AssignmentReleased {
						released = true
					}
				}
				if !released {
					t.Fatalf("offered child assignment %s was not released", initial.ID)
				}
			}
			if _, err = planner.Tick(ctx, backlog.QuotaBridgeReport{ChecksDisabled: true}); err != nil {
				t.Fatal(err)
			}
			again, err := store.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			later, err := store.GetReviewRound(ctx, round.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(records.Attempts, again.Attempts) || !reflect.DeepEqual(records.Assignments, again.Assignments) || round.Revision != later.Revision {
				t.Fatal("second tick changed settled revisions")
			}
		})
	}
}

func TestReviewLifecyclePausedAndParkedPlannerAndAdmission(t *testing.T) {
	for _, control := range []domain.ControlState{domain.ControlDraining, domain.ControlPaused, domain.ControlPausedUncheckpointed, domain.ControlWaitingExternal} {
		t.Run(string(control), func(t *testing.T) {
			ctx := context.Background()
			store, frozen, receipt, _ := reviewRuntimeProductionFixture(t)
			reviewLifecycleWorkerTransition(t, store, control, false)
			before, err := store.GetReviewRound(ctx, receipt.Checkpoint.RoundID)
			if err != nil {
				t.Fatal(err)
			}
			result, err := store.ReconcileReviewChildCancellation(ctx, frozen, receipt.Checkpoint)
			if err != nil || result.Status != "no-action" {
				t.Fatalf("live suspension reconciliation: %+v %v", result, err)
			}
			planner := reviewLifecyclePlanner(store, 1)
			if _, err = planner.Tick(ctx, backlog.QuotaBridgeReport{ChecksDisabled: true}); err != nil {
				t.Fatalf("suspended parent stopped planner/admission: %v", err)
			}
			after, err := store.GetReviewRound(ctx, before.ID)
			if err != nil {
				t.Fatal(err)
			}
			if before.Revision != after.Revision || after.Terminal() {
				t.Fatal("live suspended parent settled round")
			}
		})
	}
}

func TestReviewLifecycleAdminTaskAndRunCancel(t *testing.T) {
	for _, scope := range []string{"task", "run"} {
		t.Run(scope, func(t *testing.T) {
			ctx := context.Background()
			store, _, receipt, _ := reviewRuntimeProductionFixture(t)
			service, err := backlogadmin.New(store, reviewLifecycleAuthorizer{})
			if err != nil {
				t.Fatal(err)
			}
			request := backlogadmin.Mutation{Version: backlogadmin.Version, Principal: backlogadmin.Principal{ID: "test-operator"}, ID: "cancel-parent", Kind: domain.AdminCommandCancel, WorkflowRunID: "parent-run", TaskID: "parent-task", ExpectedRevision: 1, Reason: "test cancellation"}
			if scope == "run" {
				request.TaskID = ""
				request.Payload = json.RawMessage(`{"scope":"run"}`)
			}
			if _, err = service.Mutate(ctx, request); err != nil {
				t.Fatal(err)
			}
			report, err := service.ExecutePendingCommands(ctx)
			if err != nil || len(report.Decisions) != 1 || report.Decisions[0].Command.State != domain.AdminCommandApplied {
				t.Fatalf("admin cancel: %+v %v", report, err)
			}
			if err = store.ReconcileMaterializedReviewChildren(ctx); err != nil {
				t.Fatal(err)
			}
			round, err := store.GetReviewRound(ctx, receipt.Checkpoint.RoundID)
			if err != nil {
				t.Fatal(err)
			}
			if !round.Terminal() {
				t.Fatal("admin cancel left round open")
			}
			reason := "parent attempt ended"
			// Admin run cancellation first settles each attempt; the sink then
			// completes the run. Both admin paths therefore observe the attempt reason.
			for _, member := range round.Reviewers {
				if member.State != "failed" || member.Failure != reason {
					t.Fatalf("member: %+v", member)
				}
			}
		})
	}
}

func TestReviewLifecyclePlannerStaleEpochWritesNothing(t *testing.T) {
	ctx := context.Background()
	store, _, receipt, _ := reviewRuntimeProductionFixture(t)
	reviewLifecycleWorkerTransition(t, store, domain.ControlStopped, true)
	if _, err := store.AcquireCoordinator(ctx, "next-coordinator"); err != nil {
		t.Fatal(err)
	}
	before, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	round, err := store.GetReviewRound(ctx, receipt.Checkpoint.RoundID)
	if err != nil {
		t.Fatal(err)
	}
	planner := reviewLifecyclePlanner(store, 1)
	_, err = planner.Tick(ctx, backlog.QuotaBridgeReport{ChecksDisabled: true})
	if !errors.Is(err, sqlite.ErrStaleCoordinatorEpoch) {
		t.Fatalf("stale planner: %v", err)
	}
	after, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	later, err := store.GetReviewRound(ctx, round.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(round, later) {
		t.Fatal("stale coordinator wrote cancellation")
	}
}
