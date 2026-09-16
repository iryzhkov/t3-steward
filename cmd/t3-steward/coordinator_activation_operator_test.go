package main

// What an activation's own receipt says when somebody else decided, and what
// closes an activation once its run has settled.
//
// Both are failures observed on a live supervised qualification. An operator
// decided the gate the overseer had been woken for, which the coordinator
// accepted and the activation then recorded as "the activation's turn ended
// without recording a decision": true about the overseer, and read by everyone
// as a review that produced nothing. And the last activation of the run stayed
// active with a live lease after the run settled, because the dispatch pass
// skips a terminal run entirely and nothing else observes an activation.

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// decideAsOperator records the same gate decision the overseer was woken to
// make, signed by an ordinary remote admin client. That is exactly what a CLI
// with no supervisor identity does, and what an operator taking the decision by
// hand does.
func (f *activationLeaseFixture) decideAsOperator(t *testing.T, requestID string) {
	t.Helper()
	ctx := context.Background()
	state, err := f.store.LoadSupervisionAdminState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	var gate sqlite.SupervisionGateFacts
	for _, candidate := range state.Gates {
		if candidate.Gate.Definition.ID == activationLeaseGate {
			gate = candidate
		}
	}
	if _, err := f.store.DecideGate(ctx, sqlite.GateDecisionRequest{
		RunID: activationLeaseRun, GateID: activationLeaseGate, RequestID: requestID,
		Actor:                 domain.Actor{Kind: domain.ActorOperator, Principal: "remote:admin:homelab"},
		ExpectedGraphRevision: gate.Gate.GraphRevision, ExpectedGateRevision: gate.Gate.Revision,
		Evidence: domain.EvidenceSnapshot{
			ID: activationLeaseEvidence, GraphRevision: 1, TakenAt: f.now,
			Producers: []domain.ProducerEvidence{{
				TaskID: "task-producer", AttemptID: "attempt-producer", ResultRevision: 7,
			}},
		},
		Outcome: domain.GateDecisionAccept, Reason: "the operator accepted the gate by hand", DecidedAt: f.now,
	}); err != nil {
		t.Fatalf("record the operator's decision: %v", err)
	}
}

// finishActivationTurn writes what a worker reporting a finished turn leaves
// behind: a terminal attempt and a completed assignment.
func (f *activationLeaseFixture) finishActivationTurn(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	records, err := f.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var attempts []domain.Attempt
	var assignments []domain.Assignment
	for _, attempt := range records.Attempts {
		if !attempt.IsSupervisionActivation() {
			continue
		}
		attempt.Progress = domain.ProgressSucceeded
		attempt.Control = domain.ControlStopped
		attempt.Revision++
		attempt.UpdatedAt = f.now
		attempts = append(attempts, attempt)
		for _, assignment := range records.Assignments {
			if assignment.AttemptID != attempt.ID {
				continue
			}
			assignment.State = domain.AssignmentCompleted
			assignment.UpdatedAt = f.now
			assignments = append(assignments, assignment)
		}
	}
	if len(attempts) != 1 || len(assignments) != 1 {
		t.Fatalf("the fixture holds %d activation attempts and %d assignments", len(attempts), len(assignments))
	}
	if err := f.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Attempts: attempts, Assignments: assignments,
	}); err != nil {
		t.Fatal(err)
	}
}

// An operator keeps full authority over a supervised run, so its decision
// stands. What changes is the activation's own receipt: it records that the
// decision was not the overseer's, and its outcome says decided-by-operator
// rather than the no-decision that reads as a review which produced nothing.
func TestOperatorDecisionIsAcceptedAndRecordedOnTheLiveActivation(t *testing.T) {
	ctx := context.Background()
	fixture := newActivationLeaseFixture(t)
	activation := fixture.activate(t)

	fixture.now = activationLeaseTime.Add(2 * time.Minute)
	fixture.decideAsOperator(t, "request-operator-accept")

	admin, err := fixture.store.LoadSupervisionAdminState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if admin.Activation.Epoch != activation.Epoch {
		t.Fatalf("activation epoch = %d, want the live %d", admin.Activation.Epoch, activation.Epoch)
	}
	if admin.Activation.OperatorDecisions != 1 {
		t.Fatalf("operator decisions on the activation = %d, want 1", admin.Activation.OperatorDecisions)
	}
	var decided bool
	for _, gate := range admin.Gates {
		if gate.Gate.Definition.ID != activationLeaseGate {
			continue
		}
		decided = true
		if gate.Gate.State != domain.GateAccepted {
			t.Fatalf("gate state = %q, want the operator's acceptance to stand", gate.Gate.State)
		}
		// show reads this: an overseer deciding as itself and an operator
		// deciding for it leave the same accepted gate behind.
		if gate.LastDecision == nil || gate.LastDecision.Actor.Kind != domain.ActorOperator ||
			gate.LastDecision.Actor.Principal != "remote:admin:homelab" {
			t.Fatalf("last decision = %#v, want the operator principal recorded", gate.LastDecision)
		}
	}
	if !decided {
		t.Fatal("the fixture holds no reviewed gate")
	}

	// The turn then ends. The overseer recorded nothing of its own, and the
	// receipt says why rather than calling the review empty.
	fixture.now = activationLeaseTime.Add(3 * time.Minute)
	fixture.finishActivationTurn(t)
	plan := fixture.advance(t)
	if plan.Activation.State != domain.ActivationSpent {
		t.Fatalf("activation state = %q, want spent once its turn ended", plan.Activation.State)
	}
	if plan.Activation.Outcome != domain.ActivationOutcomeDecidedByOperator {
		t.Fatalf("activation outcome = %q, want %q",
			plan.Activation.Outcome, domain.ActivationOutcomeDecidedByOperator)
	}
	if !plan.CursorAdvanced {
		t.Fatal("a settled review left its inbox unconsumed, so the same events would wake a replacement")
	}
}

// An overseer that decides as itself is still recorded as deciding: the
// operator path above adds a case rather than replacing this one.
func TestOverseerDecisionStaysDecided(t *testing.T) {
	fixture := newActivationLeaseFixture(t)
	activation := fixture.activate(t)
	fixture.now = activationLeaseTime.Add(2 * time.Minute)
	fixture.decide(t, activation.Epoch, "request-overseer-accept")
	fixture.now = activationLeaseTime.Add(3 * time.Minute)
	fixture.finishActivationTurn(t)
	plan := fixture.advance(t)
	if plan.Activation.Outcome != domain.ActivationOutcomeDecided {
		t.Fatalf("activation outcome = %q, want decided", plan.Activation.Outcome)
	}
}

// A run that settles closes its activation. Without this the last activation of
// every supervised run stayed active, holding a lease nobody may use and
// claiming an overseer nobody is waiting for.
func TestRunSettlementClosesALiveActivation(t *testing.T) {
	ctx := context.Background()
	fixture := newActivationLeaseFixture(t)
	fixture.activate(t)

	records, err := fixture.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	state, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	run := records.WorkflowRuns[0]
	// The dispatch pass reads supervision from the run record, which is how a
	// submitted campaign carries it.
	record := state.Record
	run.Supervision = &record
	run.Progress = domain.ProgressSucceeded
	run.Revision++
	run.UpdatedAt = fixture.now
	if err := fixture.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{run},
	}); err != nil {
		t.Fatal(err)
	}

	fixture.now = activationLeaseTime.Add(5 * time.Minute)
	fixture.coordinator.DispatchActivations(ctx, backlog.WorkerAdmissionPolicy{})

	state, err = fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.State != domain.ActivationClosed {
		t.Fatalf("activation state = %q, want closed once the run settled", state.Activation.State)
	}
	if state.Activation.LeaseToken != "" || state.Activation.LeaseExpiresAt != nil {
		t.Fatalf("a closed activation still holds lease %#v", state.Activation)
	}
	if state.Activation.ClosedAt == nil {
		t.Fatal("a closed activation records no closing time")
	}

	// The boundary is idempotent: a second pass over the same settled run does
	// not try to close an activation that is already closed.
	fixture.now = activationLeaseTime.Add(6 * time.Minute)
	fixture.coordinator.DispatchActivations(ctx, backlog.WorkerAdmissionPolicy{})
	again, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if again.Activation.State != domain.ActivationClosed ||
		!again.Activation.ClosedAt.Equal(*state.Activation.ClosedAt) {
		t.Fatalf("a second settlement pass changed the closed activation: %#v", again.Activation)
	}
}
