package campaign

// The supervised path whose gate decision is made by an overseer activation.
//
// The sibling supervised_path_test.go drives every decision as an operator, so
// it says nothing about how a review is actually run. This file says it: the
// activation is planned by the activation lifecycle, placed on a worker that
// advertises the campaign supervision capability, committed as its own attempt
// and assignment, claimed as ordinary assigned work and executed in a T3 thread
// of its own. The decision then arrives through the supervisor principal, under
// the scoped capability the coordinator issued, and releases the gate.
//
// What the assertions are really about is the boundary: an activation is
// assigned work for the purposes of dispatch and recovery, and is not work for
// the purposes of the run. It is absent from the task graph, from the sink
// aggregate and from quiescence; it is never offered to a worker that cannot
// run it; a decision from a replaced epoch is refused; a turn that ends without
// a decision leaves the gate exactly as it found it.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// overseerPrincipal is the identity the overseer's CLI authenticates as. It is
// the relayed spelling rather than the bare client name, because every admin
// request reaches the coordinator through the local server, which rewrites a
// relayed client's principal: an activation recorded under the bare name would
// match nothing and its overseer would be refused every operation.
var overseerPrincipal = backlogadmin.RelayedPrincipalID("campaign-supervisor")

// incapableWorker is a second worker that hosts the overseer route and does not
// advertise the supervision capability. Its ID sorts before the capable
// worker's on purpose, so that a placement choosing the capable one has chosen
// it for its capability rather than for its position.
const incapableWorker = "worker-0"

// overseerRun is one supervised campaign driven through its activations, with
// the clock the activation lifecycle reads.
type overseerRun struct {
	fixture     supervisedFixture
	supervision backlog.CoordinatorSupervisionStore
	activations backlog.SupervisionActivationService
	now         time.Time
}

func newOverseerRun(t *testing.T) *overseerRun {
	t.Helper()
	run := &overseerRun{fixture: submitSupervisedCampaign(t), now: baselineTime}
	run.supervision = backlog.CoordinatorSupervisionStore{Store: run.fixture.store}
	run.activations = backlog.SupervisionActivationService{
		Store: run.supervision,
		Now:   func() time.Time { return run.now },
	}
	if err := run.fixture.store.SaveWorkerSnapshot(context.Background(), overseerIncapableSnapshot()); err != nil {
		t.Fatal(err)
	}
	return run
}

// overseerIncapableSnapshot is the fleet's second worker: same providers, same
// health, no campaign supervision capability.
func overseerIncapableSnapshot() domain.WorkerSnapshot {
	snapshot := baselineSnapshot()
	snapshot.WorkerID = incapableWorker
	snapshot.WorkerEpoch = "worker-epoch-0"
	snapshot.Inventory.ID = incapableWorker
	snapshot.Inventory.Capabilities = nil
	return snapshot
}

// observe appends one supervision event, which is what the coordinator's own
// boundary does when a gate becomes reviewable.
func (r *overseerRun) observe(t *testing.T, id, gateID, reason string) {
	t.Helper()
	if _, err := r.supervision.AppendSupervisionEvents(context.Background(), r.fixture.supervised,
		[]backlog.SupervisionEvent{{
			ID: id, RunID: r.fixture.supervised, Kind: backlog.TriggerGateReviewReady,
			Reason: reason, GateID: gateID, OccurredAt: r.now,
		}}); err != nil {
		t.Fatalf("append supervision event %q: %v", id, err)
	}
}

// advance applies one lifecycle event, fenced on the record as it stands now.
func (r *overseerRun) advance(t *testing.T, event domain.ActivationEvent, mutate ...func(*backlog.ActivationSignal)) backlog.ActivationPlan {
	t.Helper()
	plan, err := r.tryAdvance(event, mutate...)
	if err != nil {
		t.Fatalf("advance activation with %s: %v", event, err)
	}
	return plan
}

func (r *overseerRun) tryAdvance(event domain.ActivationEvent, mutate ...func(*backlog.ActivationSignal)) (backlog.ActivationPlan, error) {
	state, err := r.supervision.LoadSupervisionActivationState(context.Background(), r.fixture.supervised)
	if err != nil {
		return backlog.ActivationPlan{}, err
	}
	signal := backlog.ActivationSignal{
		Event:                  event,
		Actor:                  domain.Actor{Kind: domain.ActorOperator, Principal: "coordinator"},
		ExpectedEpoch:          state.Activation.Epoch,
		ExpectedRecordRevision: state.Record.Revision,
		Principal:              overseerPrincipal,
		Reason:                 string(event),
	}
	for _, apply := range mutate {
		apply(&signal)
	}
	return r.activations.Advance(context.Background(), r.fixture.supervised, signal)
}

// dispatch places one planned activation and commits its attempt and
// assignment, exactly as the coordinator's activation boundary does.
func (r *overseerRun) dispatch(t *testing.T, plan backlog.ActivationPlan, workers []domain.WorkerSnapshot) (domain.Attempt, domain.Assignment) {
	t.Helper()
	if plan.Dispatch == nil {
		t.Fatal("the activation plan carries no dispatch")
	}
	placement, err := backlog.PlaceActivation(r.placementRequest(t, workers))
	if err != nil {
		t.Fatalf("place the activation: %v", err)
	}
	attempt, assignment, err := backlog.ActivationAssignment(plan.Activation, *plan.Dispatch, placement,
		r.maxTurnsPerActivation(t), r.now)
	if err != nil {
		t.Fatalf("build the activation assignment: %v", err)
	}
	assignment.WorkerEpoch = placement.WorkerEpoch
	committed, err := r.fixture.store.CommitActivationAssignment(context.Background(), sqlite.ActivationAssignmentCommit{
		CoordinatorEpoch: 1, Attempt: attempt, Assignment: assignment,
		WorkerEpoch: placement.WorkerEpoch, WorkerSnapshotSequence: placement.SnapshotSequence,
		CommittedAt: r.now,
	})
	if err != nil {
		t.Fatalf("commit the activation assignment: %v", err)
	}
	return attempt, committed
}

// maxTurnsPerActivation is the run's own declared turn budget, which is what
// the activation's durable cost estimate is derived from.
func (r *overseerRun) maxTurnsPerActivation(t *testing.T) int {
	t.Helper()
	run := supervisedRun(t, reload(context.Background(), t, r.fixture.store), r.fixture.supervised)
	if run.Supervision == nil {
		t.Fatal("the supervised run carries no supervision record")
	}
	return run.Supervision.Config.MaxTurnsPerActivation
}

func (r *overseerRun) placementRequest(t *testing.T, workers []domain.WorkerSnapshot) backlog.ActivationPlacementRequest {
	t.Helper()
	run := supervisedRun(t, reload(context.Background(), t, r.fixture.store), r.fixture.supervised)
	if run.Supervision == nil {
		t.Fatal("the supervised run carries no supervision record")
	}
	return backlog.ActivationPlacementRequest{
		Route: run.Supervision.Config.Route, Workers: workers, Epoch: 1, Now: r.now,
		// The overseer obeys the same admission gate as every other route; its
		// pool is open here because starvation is a separate case.
		Admission: backlog.WorkerAdmissionPolicy{
			OpenQuotaPools: map[string]struct{}{baselineOverseerPool: {}, baselineSupervisedPool: {}},
		},
	}
}

// finishActivationTurn records what a worker reporting a finished turn leaves
// behind: a terminal attempt and a completed assignment, which is what releases
// the executor slot the activation held.
func (r *overseerRun) finishActivationTurn(t *testing.T, attemptID string) {
	t.Helper()
	ctx := context.Background()
	records := reload(ctx, t, r.fixture.store)
	var attempts []domain.Attempt
	var assignments []domain.Assignment
	for _, attempt := range records.Attempts {
		if attempt.ID != attemptID {
			continue
		}
		attempt.Progress = domain.ProgressSucceeded
		attempt.Control = domain.ControlStopped
		attempt.Revision++
		attempt.UpdatedAt = r.now
		attempts = append(attempts, attempt)
	}
	for _, assignment := range records.Assignments {
		if assignment.AttemptID != attemptID {
			continue
		}
		assignment.State = domain.AssignmentCompleted
		assignment.UpdatedAt = r.now
		assignments = append(assignments, assignment)
	}
	if len(attempts) != 1 || len(assignments) != 1 {
		t.Fatalf("activation attempt %q has %d attempts and %d assignments", attemptID, len(attempts), len(assignments))
	}
	if err := r.fixture.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Attempts: attempts, Assignments: assignments,
	}); err != nil {
		t.Fatalf("record the finished activation turn: %v", err)
	}
}

// completeDeclaredAssignments retires the assignments of the run's declared
// tasks and leaves every activation assignment exactly as it is, which is the
// state this file needs to ask whether settlement waits for an overseer.
func completeDeclaredAssignments(ctx context.Context, t *testing.T, store *sqlite.Store, runID string) {
	t.Helper()
	records := reload(ctx, t, store)
	var retired []domain.Assignment
	for _, assignment := range records.Assignments {
		attempt, known := attemptByID(records.Attempts, assignment.AttemptID)
		if !known || attempt.WorkflowRunID != runID || attempt.IsSupervisionActivation() {
			continue
		}
		assignment.State = domain.AssignmentCompleted
		retired = append(retired, assignment)
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Assignments: retired}); err != nil {
		t.Fatal(err)
	}
}

// taskThreads is every T3 thread the run's declared tasks were given.
func taskThreads(t *testing.T, records sqlite.CoordinatorRecords, runID string) map[string]string {
	t.Helper()
	threads := make(map[string]string)
	for _, assignment := range records.Assignments {
		attempt, known := attemptByID(records.Attempts, assignment.AttemptID)
		if !known || attempt.WorkflowRunID != runID || attempt.IsSupervisionActivation() {
			continue
		}
		threads[assignment.ThreadID] = attempt.TaskID
	}
	return threads
}

// overseerReadyGate runs the two analyses to success and makes the analysis
// review gate reviewable, which is the state every activation below starts in.
func overseerReadyGate(ctx context.Context, t *testing.T, run *overseerRun) sqlite.SupervisionGateFacts {
	t.Helper()
	coordinator := backlog.FleetCoordinator{Store: run.fixture.store, Now: func() time.Time { return run.now }}
	records := reload(ctx, t, run.fixture.store)
	supervisedCommit(ctx, t, coordinator, run.fixture.store, records, run.now)
	supervisedFinalize(ctx, t, run.fixture, []string{"interfaces", "tests"}, run.now)
	run.now = baselineTime.Add(2 * time.Minute)
	advanced, err := run.fixture.store.AdvanceSupervisionGates(ctx, run.fixture.supervised, run.now)
	if err != nil {
		t.Fatalf("advance gates: %v", err)
	}
	if len(advanced) != 1 {
		t.Fatalf("advanced gates = %#v, want only the analysis review", advanced)
	}
	gate := supervisionGateByName(t, supervisionSnapshot(ctx, t, run.fixture.store, run.fixture.supervised), "analysis_review")
	return gateFacts(t, adminState(ctx, t, run.fixture.store, run.fixture.supervised), gate.Definition.ID)
}

func TestSupervisedGateIsDecidedByAnActivationDispatchedAsAssignedWork(t *testing.T) {
	ctx := context.Background()
	run := newOverseerRun(t)
	store := run.fixture.store
	review := overseerReadyGate(ctx, t, run)

	// A reviewable gate is a supervision event, and a supervision event with no
	// live overseer is what plans an activation.
	run.observe(t, "event-analysis-review", review.Gate.Definition.ID, "the analysis review is ready")
	plan := run.advance(t, domain.ActivationEventTriggerFired)
	if plan.Dispatch == nil || plan.Activation.State != domain.ActivationPendingDispatch {
		t.Fatalf("a trigger with no live overseer must plan a dispatch: %#v", plan.Activation)
	}
	if plan.Activation.Principal != overseerPrincipal {
		t.Fatalf("activation principal = %q, want the relayed supervisor principal %q",
			plan.Activation.Principal, overseerPrincipal)
	}
	epoch := plan.Activation.Epoch

	// It is never offered to a worker that cannot run it. Placement excludes
	// the worker that does not advertise the capability, and the commit refuses
	// one for the same reason, so a fleet that changed after planning cannot
	// slip an activation onto a worker that would not understand it.
	workers := []domain.WorkerSnapshot{overseerIncapableSnapshot(), baselineSnapshot()}
	if _, err := backlog.PlaceActivation(run.placementRequest(t,
		[]domain.WorkerSnapshot{overseerIncapableSnapshot()})); !errors.Is(err, backlog.ErrActivationUnplaceable) ||
		!strings.Contains(err.Error(), workerproto.CapabilityCampaignSupervision) {
		t.Fatalf("placement error = %v, want an unplaceable activation naming the missing capability", err)
	}
	placement, err := backlog.PlaceActivation(run.placementRequest(t, workers))
	if err != nil {
		t.Fatalf("place the activation: %v", err)
	}
	if placement.WorkerID != baselineWorker {
		t.Fatalf("placed worker = %q, want the capable %q", placement.WorkerID, baselineWorker)
	}
	if len(placement.Excluded) != 1 || !strings.Contains(placement.Excluded[0], incapableWorker) {
		t.Fatalf("excluded = %#v, want the worker without the capability explained", placement.Excluded)
	}
	refusedAttempt, refusedAssignment, err := backlog.ActivationAssignment(
		plan.Activation, *plan.Dispatch, backlog.ActivationPlacement{
			WorkerID: incapableWorker, WorkerEpoch: "worker-epoch-0", SnapshotSequence: 1,
			Route: placement.Route,
		}, 2, run.now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitActivationAssignment(ctx, sqlite.ActivationAssignmentCommit{
		CoordinatorEpoch: 1, Attempt: refusedAttempt, Assignment: refusedAssignment,
		WorkerEpoch: "worker-epoch-0", WorkerSnapshotSequence: 1, CommittedAt: run.now,
	}); err == nil || !strings.Contains(err.Error(), workerproto.CapabilityCampaignSupervision) {
		t.Fatalf("commit error = %v, want the missing capability named", err)
	}

	// Offered as its own assignment, in a T3 thread no task holds.
	attempt, assignment := run.dispatch(t, plan, workers)
	if !attempt.IsSupervisionActivation() || attempt.SupervisionActivationEpoch != epoch {
		t.Fatalf("activation attempt = %#v", attempt)
	}
	if assignment.State != domain.AssignmentOffered || assignment.WorkerID != baselineWorker {
		t.Fatalf("activation assignment = %#v", assignment)
	}
	records := reload(ctx, t, store)
	threads := taskThreads(t, records, run.fixture.supervised)
	if len(threads) == 0 {
		t.Fatal("the run's declared tasks hold no threads, so nothing is being compared")
	}
	if task, shared := threads[assignment.ThreadID]; shared {
		t.Fatalf("the activation shares T3 thread %q with task %q", assignment.ThreadID, task)
	}

	// Not a node of the run. The task graph, the planner and the sink all see
	// the three declared tasks and nothing else.
	runAttempts := attemptsOfRun(records.Attempts, run.fixture.supervised)
	if declared := domain.DeclaredTaskAttempts(runAttempts); len(declared) != 3 ||
		len(domain.SupervisionActivationAttempts(runAttempts)) != 1 {
		t.Fatalf("run attempts = %d, declared = %d, want three declared tasks and one activation",
			len(runAttempts), len(declared))
	}
	for _, task := range records.Tasks {
		if task.ID == attempt.TaskID {
			t.Fatalf("the activation %q was materialized as a declared task", attempt.TaskID)
		}
	}

	// The worker claims it and starts its session, which is what confirms the
	// dispatch and renews the lease the decision authority hangs on.
	run.now = baselineTime.Add(3 * time.Minute)
	claimed := baselineClaim(ctx, t, store, assignment, run.now)
	if claimed.ThreadID != assignment.ThreadID {
		t.Fatalf("the claim changed the activation's session identity: %#v", claimed)
	}
	confirmed := run.advance(t, domain.ActivationEventDispatchConfirmed, func(signal *backlog.ActivationSignal) {
		signal.ExecutionObserved = true
	})
	if confirmed.Activation.State != domain.ActivationActive ||
		!backlog.ActivationLeaseValid(confirmed.Activation, run.now) {
		t.Fatalf("confirmed activation = %#v, want an active overseer holding a live lease", confirmed.Activation)
	}

	// While it is live, a second activation for the same run and epoch is not
	// created: the state machine refuses the trigger rather than starting a
	// competing overseer.
	run.observe(t, "event-second-trigger", review.Gate.Definition.ID, "the analysis review is still ready")
	if _, err := run.tryAdvance(domain.ActivationEventTriggerFired); !errors.Is(err, domain.ErrSupervisionIllegalTransition) {
		t.Fatalf("second trigger error = %v, want the transition refused while an activation is live", err)
	}
	after := reload(ctx, t, store)
	if len(domain.SupervisionActivationAttempts(attemptsOfRun(after.Attempts, run.fixture.supervised))) != 1 {
		t.Fatal("a second activation attempt exists for a run that already has a live one")
	}

	// A decision from a stale epoch is refused: an epoch that is not the
	// record's names an overseer the run has replaced.
	run.now = baselineTime.Add(4 * time.Minute)
	staleDecision := sqlite.GateDecisionRequest{
		RunID: run.fixture.supervised, GateID: review.Gate.Definition.ID, RequestID: "decide-stale-epoch",
		Actor:                 domain.Actor{Kind: domain.ActorOverseer, Principal: overseerPrincipal, ActivationEpoch: epoch + 1},
		ExpectedGraphRevision: review.Gate.GraphRevision, ExpectedGateRevision: review.Gate.Revision,
		Evidence: *review.Evidence, Outcome: domain.GateDecisionAccept,
		Reason: "a decision formed under another epoch", DecidedAt: run.now,
	}
	if _, err := store.DecideGate(ctx, staleDecision); !errors.Is(err, domain.ErrSupervisionUnauthorizedActor) ||
		!strings.Contains(err.Error(), "epoch") {
		t.Fatalf("stale-epoch decision error = %v, want the epoch refusal", err)
	}

	// The decision the live overseer submits through its own principal, under
	// the capability scoped to this run and this epoch, is accepted and releases
	// the protected task.
	accepted := staleDecision
	accepted.RequestID = "decide-accept"
	accepted.Actor = domain.Actor{Kind: domain.ActorOverseer, Principal: overseerPrincipal, ActivationEpoch: epoch}
	accepted.Reason = "both analyses meet the rubric"
	if _, err := store.DecideGate(ctx, accepted); err != nil {
		t.Fatalf("the live overseer's decision was refused: %v", err)
	}
	synthesis := supervisedTasksByName(reload(ctx, t, store), run.fixture.supervised)["synthesis"]
	run.now = baselineTime.Add(5 * time.Minute)
	coordinator := backlog.FleetCoordinator{Store: store, Now: func() time.Time { return run.now }}
	if offered := supervisedCommit(ctx, t, coordinator, store, reload(ctx, t, store), run.now); !offered[synthesis.ID] {
		t.Fatal("synthesis was still withheld after the overseer accepted its gate")
	}

	// The turn ends. The coordinator records the outcome from its own decision
	// rows, and the slot the activation held is released by the same
	// reconciliation that retires any assignment.
	run.now = baselineTime.Add(6 * time.Minute)
	run.finishActivationTurn(t, attempt.ID)
	spent := run.advance(t, domain.ActivationEventLimitReached, func(signal *backlog.ActivationSignal) {
		signal.ExecutionObserved = true
		signal.Outcome = domain.ActivationOutcomeDecided
	})
	if spent.Activation.State != domain.ActivationSpent ||
		spent.Activation.Outcome != domain.ActivationOutcomeDecided {
		t.Fatalf("finished activation = %#v, want spent and decided", spent.Activation)
	}
	if backlog.ActivationLeaseValid(spent.Activation, run.now) {
		t.Fatal("a spent activation still holds a live lease")
	}
	final := reload(ctx, t, store)
	for _, candidate := range final.Assignments {
		if candidate.AttemptID == attempt.ID && candidate.State != domain.AssignmentCompleted {
			t.Fatalf("the activation assignment is %q, want the slot released", candidate.State)
		}
	}
}

func TestSupervisedRunSettlesWhileAnOverseerActivationIsStillAssigned(t *testing.T) {
	ctx := context.Background()
	run := newOverseerRun(t)
	store := run.fixture.store
	operator := domain.Actor{Kind: domain.ActorOperator, Principal: "operator-a"}
	review := overseerReadyGate(ctx, t, run)

	// One activation is dispatched, claimed and still running. Nothing below
	// ends it: the question this test asks is whether the run's own settlement
	// waits for it, and the answer has to be no.
	run.observe(t, "event-analysis-review", review.Gate.Definition.ID, "the analysis review is ready")
	plan := run.advance(t, domain.ActivationEventTriggerFired)
	attempt, assignment := run.dispatch(t, plan,
		[]domain.WorkerSnapshot{overseerIncapableSnapshot(), baselineSnapshot()})
	run.now = baselineTime.Add(3 * time.Minute)
	baselineClaim(ctx, t, store, assignment, run.now)
	run.advance(t, domain.ActivationEventDispatchConfirmed, func(signal *backlog.ActivationSignal) {
		signal.ExecutionObserved = true
	})

	// The operator decides both gates and the declared graph runs to the end.
	if _, err := store.DecideGate(ctx, sqlite.GateDecisionRequest{
		RunID: run.fixture.supervised, GateID: review.Gate.Definition.ID, RequestID: "decide-accept",
		Actor: operator, ExpectedGraphRevision: review.Gate.GraphRevision,
		ExpectedGateRevision: review.Gate.Revision, Evidence: *review.Evidence,
		Outcome: domain.GateDecisionAccept, Reason: "both analyses meet the rubric", DecidedAt: run.now,
	}); err != nil {
		t.Fatalf("accept the analysis review: %v", err)
	}
	run.now = baselineTime.Add(5 * time.Minute)
	coordinator := backlog.FleetCoordinator{Store: store, Now: func() time.Time { return run.now }}
	supervisedCommit(ctx, t, coordinator, store, reload(ctx, t, store), run.now)
	supervisedFinalize(ctx, t, run.fixture, []string{"synthesis"}, run.now)
	completeDeclaredAssignments(ctx, t, store, run.fixture.supervised)
	run.now = baselineTime.Add(6 * time.Minute)
	if _, err := store.AdvanceSupervisionGates(ctx, run.fixture.supervised, run.now); err != nil {
		t.Fatalf("advance the final gate: %v", err)
	}
	finalGate := gateFacts(t, adminState(ctx, t, store, run.fixture.supervised), supervisionGateByName(t,
		supervisionSnapshot(ctx, t, store, run.fixture.supervised), "final_report").Definition.ID)
	// One projection while the final gate is still undecided, which is what
	// takes the declared graph to quiescence and leaves the run held by its
	// final-settlement gate alone.
	if _, err := backlog.ProjectWorkflowRuns(ctx,
		backlog.CoordinatorSupervisionStore{Store: store}, run.now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DecideGate(ctx, sqlite.GateDecisionRequest{
		RunID: run.fixture.supervised, GateID: finalGate.Gate.Definition.ID, RequestID: "decide-final",
		Actor: operator, ExpectedGraphRevision: finalGate.Gate.GraphRevision,
		ExpectedGateRevision: finalGate.Gate.Revision, Evidence: *finalGate.Evidence,
		Outcome: domain.GateDecisionAccept, Reason: "the synthesis is the report", DecidedAt: run.now,
	}); err != nil {
		t.Fatalf("accept the final gate: %v", err)
	}

	// The sink settles with the activation's attempt still running and its
	// assignment still claimed: an activation is outside the run's aggregate and
	// outside its quiescence question, which is what keeps a supervised run from
	// waiting forever on the thing that reviews it.
	run.now = baselineTime.Add(7 * time.Minute)
	if _, err := backlog.ProjectWorkflowRuns(ctx,
		backlog.CoordinatorSupervisionStore{Store: store}, run.now); err != nil {
		t.Fatal(err)
	}
	settled := reload(ctx, t, store)
	live, known := attemptByID(settled.Attempts, attempt.ID)
	if !known || live.Progress.Terminal() {
		t.Fatalf("the activation attempt is %#v, want it still running for this question to mean anything", live)
	}
	if state := assignmentByID(t, settled.Assignments, assignment.ID).State; state != domain.AssignmentClaimed {
		t.Fatalf("the activation assignment is %q, want it still claimed", state)
	}
	if run := supervisedRun(t, settled, run.fixture.supervised); !run.Sink.Progress.Terminal() {
		t.Fatalf("the run did not settle while an overseer activation was still assigned: %#v", run.Sink)
	}
}

func TestSupervisedActivationWithoutADecisionLeavesTheGateAwaitingReview(t *testing.T) {
	ctx := context.Background()
	run := newOverseerRun(t)
	store := run.fixture.store
	review := overseerReadyGate(ctx, t, run)
	workers := []domain.WorkerSnapshot{overseerIncapableSnapshot(), baselineSnapshot()}

	run.observe(t, "event-analysis-review", review.Gate.Definition.ID, "the analysis review is ready")
	plan := run.advance(t, domain.ActivationEventTriggerFired)
	attempt, assignment := run.dispatch(t, plan, workers)
	run.now = baselineTime.Add(3 * time.Minute)
	baselineClaim(ctx, t, store, assignment, run.now)
	active := run.advance(t, domain.ActivationEventDispatchConfirmed, func(signal *backlog.ActivationSignal) {
		signal.ExecutionObserved = true
	})
	if active.Activation.State != domain.ActivationActive {
		t.Fatalf("activation = %#v, want active", active.Activation)
	}

	// The overseer's session ends cleanly and the coordinator recorded no
	// decision for it. A clean exit is not an acceptance: the outcome is
	// no-decision and the gate is exactly where it was.
	run.now = baselineTime.Add(4 * time.Minute)
	run.finishActivationTurn(t, attempt.ID)
	spent := run.advance(t, domain.ActivationEventLimitReached, func(signal *backlog.ActivationSignal) {
		signal.ExecutionObserved = true
	})
	if spent.Activation.State != domain.ActivationSpent ||
		spent.Activation.Outcome != domain.ActivationOutcomeNoDecision {
		t.Fatalf("finished activation = %#v, want spent with no decision", spent.Activation)
	}
	afterGate := gateFacts(t, adminState(ctx, t, store, run.fixture.supervised), review.Gate.Definition.ID)
	if afterGate.Gate.State != domain.GateReadyForReview || afterGate.Gate.Revision != review.Gate.Revision {
		t.Fatalf("gate after a no-decision activation = %#v, want it still awaiting review", afterGate.Gate)
	}
	projection, err := store.LoadSupervisionProjection(ctx, run.fixture.supervised)
	if err != nil {
		t.Fatal(err)
	}
	if len(projection.Decisions) != 0 {
		t.Fatalf("decisions = %#v, want none from a turn that decided nothing", projection.Decisions)
	}

	// The synthesis task is still withheld, and the run is still nonterminal,
	// because nothing was decided. The activation that ran is nowhere in that
	// answer: the sink's barrier is the gate.
	records := reload(ctx, t, store)
	synthesis := supervisedTasksByName(records, run.fixture.supervised)["synthesis"]
	run.now = baselineTime.Add(5 * time.Minute)
	blockers := supervisedBlockers(t, ctx, store, records, run.fixture.supervised, synthesis.ID, run.now)
	if !hasSupervisionCode(blockers, domain.SupervisionBlockerGateAwaitingReview) {
		t.Fatalf("synthesis blockers = %#v, want the gate still awaiting review", blockers)
	}
}
