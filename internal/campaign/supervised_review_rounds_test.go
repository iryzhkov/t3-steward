package campaign

// Two review rounds on one supervised run, served by two activations.
//
// The sibling supervised_overseer_test.go runs one activation from wake to
// spent. This file asks the question that follows it: what happens when the
// first review says no. A rejection holds the branch, the producer is replaced,
// the gate is reconsidered against the new evidence, and a second overseer has
// to review it.
//
// The thing being pinned is that the second review is a second activation and
// not the first one again. Every identity an activation carries -- its own ID,
// its dispatch identity, the attempt that runs it, that attempt's assignment
// and the T3 thread the assignment names -- is derived from the run and the
// epoch. A spent activation that returned to idle at the same epoch would
// therefore recompute exactly what the first round already used, and the run
// could not get a second overseer turn without an operator takeover. The epoch
// is raised when a spent activation returns to idle, and these assertions are
// what that sentence means in records.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestSupervisedRunGetsTwoReviewRoundsFromTwoActivations(t *testing.T) {
	ctx := context.Background()
	run := newOverseerRun(t)
	store := run.fixture.store
	operator := domain.Actor{Kind: domain.ActorOperator, Principal: "operator-a"}
	workers := []domain.WorkerSnapshot{overseerIncapableSnapshot(), baselineSnapshot()}
	review := overseerReadyGate(ctx, t, run)
	synthesis := supervisedTasksByName(reload(ctx, t, store), run.fixture.supervised)["synthesis"]

	// Round one. One activation is woken, dispatched, claimed and confirmed.
	run.observe(t, "event-round-one", review.Gate.Definition.ID, "the analysis review is ready")
	firstPlan := run.advance(t, domain.ActivationEventTriggerFired)
	if firstPlan.Dispatch == nil {
		t.Fatalf("the first trigger planned no dispatch: %#v", firstPlan.Activation)
	}
	firstEpoch := firstPlan.Activation.Epoch
	firstAttempt, firstAssignment := run.dispatch(t, firstPlan, workers)
	run.now = baselineTime.Add(3 * time.Minute)
	baselineClaim(ctx, t, store, firstAssignment, run.now)
	run.advance(t, domain.ActivationEventDispatchConfirmed, func(signal *backlog.ActivationSignal) {
		signal.ExecutionObserved = true
	})

	// The first overseer rejects. The branch is held, and the receipt says
	// exactly what the rejection did to the start boundary: nothing was
	// released and nothing was already claimed, because the protected task was
	// never offered in the first place. A rejection that reported a release
	// here would mean the gate had been leaking dispatch before the review.
	run.now = baselineTime.Add(4 * time.Minute)
	rejection, err := store.DecideGate(ctx, sqlite.GateDecisionRequest{
		RunID: run.fixture.supervised, GateID: review.Gate.Definition.ID, RequestID: "round-one-reject",
		Actor: domain.Actor{
			Kind: domain.ActorOverseer, Principal: overseerPrincipal, ActivationEpoch: firstEpoch,
		},
		ExpectedGraphRevision: review.Gate.GraphRevision, ExpectedGateRevision: review.Gate.Revision,
		Evidence: *review.Evidence, Outcome: domain.GateDecisionReject,
		Reason: "the two analyses disagree about the interface boundary", DecidedAt: run.now,
	})
	if err != nil {
		t.Fatalf("the first overseer's rejection was refused: %v", err)
	}
	if rejection.Gate.State != domain.GateHeld {
		t.Fatalf("rejected gate = %q, want held", rejection.Gate.State)
	}
	if len(rejection.ReleasedAssignmentIDs) != 0 || len(rejection.ClaimedTaskIDs) != 0 ||
		rejection.AttemptRevision != 0 {
		t.Fatalf("rejection receipt = %#v, want no offer released and no started work reported", rejection)
	}
	blockers := supervisedBlockers(t, ctx, store, reload(ctx, t, store),
		run.fixture.supervised, synthesis.ID, run.now)
	if !hasSupervisionCode(blockers, domain.SupervisionBlockerGateHeld) {
		t.Fatalf("synthesis blockers after the rejection = %#v, want the held gate", blockers)
	}

	// The first activation finishes its turn and is spent.
	run.now = baselineTime.Add(5 * time.Minute)
	run.finishActivationTurn(t, firstAttempt.ID)
	spent := run.advance(t, domain.ActivationEventLimitReached, func(signal *backlog.ActivationSignal) {
		signal.ExecutionObserved = true
		signal.Outcome = domain.ActivationOutcomeDecided
	})
	if spent.Activation.State != domain.ActivationSpent || spent.Activation.Epoch != firstEpoch {
		t.Fatalf("spent activation = %#v, want spent at epoch %d", spent.Activation, firstEpoch)
	}

	// New producer evidence: one analysis is redone and succeeds again as a
	// later attempt, so the gate's recomputed snapshot names a different
	// producer identity than the one round one rejected.
	run.now = baselineTime.Add(6 * time.Minute)
	correctedProducerAttempt(ctx, t, run, "interfaces")
	if _, err := store.ReconsiderGate(ctx, sqlite.GateReconsiderRequest{
		RunID: run.fixture.supervised, GateID: review.Gate.Definition.ID, RequestID: "round-two-reconsider",
		Actor: operator, Reason: "the interfaces analysis was redone against the rubric", At: run.now,
	}); err != nil {
		t.Fatalf("reconsider the held gate: %v", err)
	}
	secondReview := gateFacts(t, adminState(ctx, t, store, run.fixture.supervised), review.Gate.Definition.ID)
	if secondReview.Gate.State != domain.GateReadyForReview {
		t.Fatalf("reconsidered gate = %q, want ready for review", secondReview.Gate.State)
	}
	if secondReview.Evidence.ID == review.Evidence.ID {
		t.Fatalf("round two is reviewing evidence %q, the same identity round one rejected",
			secondReview.Evidence.ID)
	}

	// The events that arrived for the spent activation return the run to idle
	// at a fresh epoch. This is the transition the second round hangs on: no
	// dispatch is planned here, and the epoch has moved.
	run.observe(t, "event-round-two", review.Gate.Definition.ID, "the analysis review is ready again")
	run.now = baselineTime.Add(7 * time.Minute)
	idle := run.advance(t, domain.ActivationEventEventsArrived)
	if idle.Activation.State != domain.ActivationIdle || idle.Dispatch != nil {
		t.Fatalf("spent activation with new events = %#v, want idle and undispatched", idle.Activation)
	}
	if idle.Activation.Epoch != firstEpoch+1 {
		t.Fatalf("idle epoch = %d, want %d: a spent activation must not return to idle at its own epoch",
			idle.Activation.Epoch, firstEpoch+1)
	}
	if idle.Record.ActivationEpoch != idle.Activation.Epoch {
		t.Fatalf("record epoch = %d, activation epoch = %d, want the fence and the activation to agree",
			idle.Record.ActivationEpoch, idle.Activation.Epoch)
	}

	// Round two. The trigger dispatches a second activation, and every identity
	// it carries differs from the first's.
	secondPlan := run.advance(t, domain.ActivationEventTriggerFired)
	if secondPlan.Dispatch == nil {
		t.Fatalf("the second trigger planned no dispatch: %#v", secondPlan.Activation)
	}
	secondEpoch := secondPlan.Activation.Epoch
	if secondEpoch != firstEpoch+1 {
		t.Fatalf("second activation epoch = %d, want %d", secondEpoch, firstEpoch+1)
	}
	if secondPlan.Activation.ID == firstPlan.Activation.ID ||
		secondPlan.Dispatch.Identity == firstPlan.Dispatch.Identity {
		t.Fatalf("the second activation reuses the first's identity: %#v", secondPlan.Activation)
	}
	secondAttempt, secondAssignment := run.dispatch(t, secondPlan, workers)
	if secondAttempt.ID == firstAttempt.ID {
		t.Fatalf("both review rounds ran as attempt %q", secondAttempt.ID)
	}
	if secondAssignment.ID == firstAssignment.ID || secondAssignment.ThreadID == firstAssignment.ThreadID {
		t.Fatalf("the second round shares assignment %q / thread %q with the first",
			secondAssignment.ID, secondAssignment.ThreadID)
	}
	if secondAttempt.SupervisionActivationEpoch != secondEpoch {
		t.Fatalf("second activation attempt = %#v, want epoch %d", secondAttempt, secondEpoch)
	}
	run.now = baselineTime.Add(8 * time.Minute)
	baselineClaim(ctx, t, store, secondAssignment, run.now)
	run.advance(t, domain.ActivationEventDispatchConfirmed, func(signal *backlog.ActivationSignal) {
		signal.ExecutionObserved = true
	})

	// The fence moved with the epoch: a decision naming the first round's epoch
	// is refused, even though that overseer was legitimately authorized for it
	// one round ago.
	run.now = baselineTime.Add(9 * time.Minute)
	late := sqlite.GateDecisionRequest{
		RunID: run.fixture.supervised, GateID: review.Gate.Definition.ID, RequestID: "round-one-late-accept",
		Actor: domain.Actor{
			Kind: domain.ActorOverseer, Principal: overseerPrincipal, ActivationEpoch: firstEpoch,
		},
		ExpectedGraphRevision: secondReview.Gate.GraphRevision,
		ExpectedGateRevision:  secondReview.Gate.Revision,
		Evidence:              *secondReview.Evidence, Outcome: domain.GateDecisionAccept,
		Reason: "the first overseer changed its mind after its turn ended", DecidedAt: run.now,
	}
	if _, err := store.DecideGate(ctx, late); !errors.Is(err, domain.ErrSupervisionStaleRevision) &&
		!errors.Is(err, domain.ErrSupervisionUnauthorizedActor) {
		t.Fatalf("late decision from the replaced epoch error = %v, want it fenced out", err)
	}

	// The second overseer accepts, and the protected task is finally offered.
	accept := late
	accept.RequestID = "round-two-accept"
	accept.Actor = domain.Actor{
		Kind: domain.ActorOverseer, Principal: overseerPrincipal, ActivationEpoch: secondEpoch,
	}
	accept.Reason = "the redone analysis meets the rubric"
	accepted, err := store.DecideGate(ctx, accept)
	if err != nil {
		t.Fatalf("the second overseer's acceptance was refused: %v", err)
	}
	if accepted.Gate.State != domain.GateAccepted {
		t.Fatalf("accepted gate = %q", accepted.Gate.State)
	}
	run.now = baselineTime.Add(10 * time.Minute)
	coordinator := backlog.FleetCoordinator{Store: store, Now: func() time.Time { return run.now }}
	if offered := supervisedCommit(ctx, t, coordinator, store,
		reload(ctx, t, store), run.now); !offered[synthesis.ID] {
		t.Fatal("synthesis was still withheld after the second round accepted its gate")
	}

	// Both rounds are on the record: two activation attempts, two decisions,
	// two activations counted against the run's budget.
	final := reload(ctx, t, store)
	activationAttempts := domain.SupervisionActivationAttempts(attemptsOfRun(final.Attempts, run.fixture.supervised))
	if len(activationAttempts) != 2 {
		t.Fatalf("activation attempts = %d, want one per review round", len(activationAttempts))
	}
	projection, err := store.LoadSupervisionProjection(ctx, run.fixture.supervised)
	if err != nil {
		t.Fatal(err)
	}
	var overseerDecisions int
	for _, decision := range projection.Decisions {
		if decision.Actor.Kind == domain.ActorOverseer {
			overseerDecisions++
		}
	}
	if overseerDecisions != 2 {
		t.Fatalf("overseer decisions = %d, want the rejection and the acceptance", overseerDecisions)
	}
	state, err := run.supervision.LoadSupervisionActivationState(ctx, run.fixture.supervised)
	if err != nil {
		t.Fatal(err)
	}
	if state.Record.ActivationsUsed != 2 {
		t.Fatalf("activations used = %d, want one per round", state.Record.ActivationsUsed)
	}
}

// correctedProducerAttempt records a later succeeded attempt for one producer
// task, which is what "new producer evidence" looks like in the rows a gate
// recomputes its snapshot from: supervision reads the task's latest attempt, so
// a redone producer changes the evidence identity without any gate being told.
func correctedProducerAttempt(ctx context.Context, t *testing.T, run *overseerRun, taskName string) {
	t.Helper()
	records := reload(ctx, t, run.fixture.store)
	task := supervisedTasksByName(records, run.fixture.supervised)[taskName]
	if task.ID == "" {
		t.Fatalf("the supervised run declares no task %q", taskName)
	}
	var latest domain.Attempt
	for _, attempt := range attemptsOfRun(records.Attempts, run.fixture.supervised) {
		if attempt.TaskID == task.ID && attempt.Number >= latest.Number {
			latest = attempt
		}
	}
	if latest.ID == "" {
		t.Fatalf("task %q has no attempt to redo", taskName)
	}
	redone := latest
	redone.ID = latest.ID + "-redone"
	redone.Number = latest.Number + 1
	redone.AssignmentID = ""
	redone.Progress = domain.ProgressSucceeded
	redone.Control = domain.ControlStopped
	redone.Revision = latest.Revision + 1
	redone.UpdatedAt = run.now
	if err := run.fixture.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Attempts: []domain.Attempt{redone},
	}); err != nil {
		t.Fatalf("record the redone producer attempt of %q: %v", taskName, err)
	}
}
