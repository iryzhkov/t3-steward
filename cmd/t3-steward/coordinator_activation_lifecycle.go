package main

// Reading the durable assignment back into the activation lifecycle.
//
// An activation runs as ordinary assigned work, so everything that happens to
// it after dispatch is already recorded by the machinery a task uses: the claim
// moves the attempt, lease expiry moves the assignment to unknown, worker state
// reconciliation completes or releases it, and unknown-assignment recovery
// decides what an ambiguous runtime meant. This file reads exactly those rows
// and turns them into the one lifecycle event each of them is.
//
// It deliberately observes rather than asks. A signal derived from durable
// state is the same on every boundary and after every restart, which is what
// keeps a reconnect or a repeated delivery from producing two overseers; a
// signal derived from a live probe would not be.
//
// The rule that matters most is the one about ending: a turn that finished
// without the coordinator recording a decision is a no-decision activation.
// Never an acceptance. The count below comes from the run's own decision rows,
// never from the transcript, which is why ActivationTurnOutcome takes it as an
// argument.

import (
	"context"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// activationLifecycleSignal derives the lifecycle event one dispatched
// activation's durable records imply, or reports that they imply none.
//
// now is the moment lease liveness is judged at.
func (c coordinatorSupervision) activationLifecycleSignal(
	ctx context.Context,
	records sqlite.CoordinatorRecords,
	state backlog.SupervisionActivationState,
) (backlog.ActivationSignal, bool, error) {
	activation := state.Activation
	signal := backlog.ActivationSignal{
		Actor:                  domain.Actor{Kind: domain.ActorOperator, Principal: coordinatorSupervisionPrincipal},
		ExpectedEpoch:          activation.Epoch,
		ExpectedRecordRevision: state.Record.Revision,
		Principal:              c.settings.principalID(),
		IncidentID:             activation.IncidentID,
	}
	assignment, offered := activationAssignmentOf(records, activation)
	if !offered {
		return signal, false, nil
	}
	attempt, known := activationAttemptOf(records, assignment.AttemptID)
	switch activation.State {
	case domain.ActivationPendingDispatch:
		switch {
		case assignment.State == domain.AssignmentClaimed:
			// The worker took it. The activation is running from here, and its
			// lease is renewed with this transition rather than on a timer.
			signal.Event = domain.ActivationEventDispatchConfirmed
			signal.ExecutionObserved = true
			signal.Reason = "the worker claimed the activation assignment"
			return signal, true, nil
		case assignment.State == domain.AssignmentReleased:
			// Released before it was ever claimed: provably undelivered, so the
			// retry reuses the original dispatch identity and spends no budget.
			signal.Event = domain.ActivationEventDispatchUndelivered
			signal.Reason = "the activation assignment was released before any worker claimed it"
			return signal, true, nil
		case assignment.State == domain.AssignmentUnknown:
			// The lease expired with the worker silent. Nothing here can prove
			// whether it started, so this is ambiguous rather than undelivered
			// and it asks for recovery instead of authorizing a replacement.
			signal.Event = domain.ActivationEventDispatchAmbiguous
			signal.Reason = "the activation assignment lease expired without an observed outcome"
			return signal, true, nil
		}
		return signal, false, nil
	case domain.ActivationActive:
		if !backlog.ActivationLeaseValid(activation, c.at()) ||
			backlog.ActivationExpired(activation, c.at()) {
			signal.Event = domain.ActivationEventLeaseExpired
			signal.ExecutionObserved = true
			signal.Reason = "the activation lease expired, so its decision authority is revoked"
			return signal, true, nil
		}
		switch assignment.State {
		case domain.AssignmentClaimed:
			// The overseer is still working and the coordinator's own decision
			// rows are the only evidence that it is. Each decision it records
			// renews the lease, which is why the lease is short and the deadline
			// is not: a renewal extends how long the coordinator believes in this
			// overseer, never how long the review may take.
			decisions, err := c.activationDecisionCount(ctx, state.Record.RunID, activation.Epoch)
			if err != nil {
				return signal, false, err
			}
			if decisions <= activation.TurnsUsed {
				return signal, false, nil
			}
			signal.Event = domain.ActivationEventDecisionRecorded
			signal.ExecutionObserved = true
			signal.Reason = fmt.Sprintf("the activation recorded %d decision(s)", decisions)
			return signal, true, nil
		case domain.AssignmentCompleted:
			// The turn is over and the slot is released, both acknowledged by
			// the assignment reaching a terminal state. What it decided is the
			// coordinator's own record.
			decisions, err := c.activationDecisionCount(ctx, state.Record.RunID, activation.Epoch)
			if err != nil {
				return signal, false, err
			}
			outcome, reason, err := c.activationTurnOutcome(attempt, known, decisions, activation.OperatorDecisions)
			if err != nil {
				return signal, false, err
			}
			signal.Event = domain.ActivationEventLimitReached
			signal.ExecutionObserved = true
			signal.Outcome = outcome
			signal.Reason = reason
			return signal, true, nil
		case domain.AssignmentReleased:
			// Started and then released by reconciliation: the runtime is proven
			// stopped, so a replacement at the next epoch is safe. It counts
			// toward the budget because it ran.
			signal.Event = domain.ActivationEventThreadLost
			signal.ExecutionObserved = true
			signal.RuntimeProvenStopped = true
			signal.Reason = "the activation assignment was released after it started"
			return signal, true, nil
		case domain.AssignmentUnknown:
			// Started and then vanished. Nothing proved it stopped, so no
			// replacement is granted execution resources.
			signal.Event = domain.ActivationEventThreadLost
			signal.ExecutionObserved = true
			signal.Reason = "the activation's worker stopped reporting and its runtime is unproven"
			return signal, true, nil
		}
		return signal, false, nil
	}
	return signal, false, nil
}

// activationTurnOutcome maps one finished activation turn onto its outcome.
//
// A successful exit with no recorded decision is no-decision. It is never
// acceptance, and the count that could make it "decided" is supplied by the
// caller from durable decision rows.
//
// operatorDecisions is the same question asked of the other actor: gates the
// activation was woken for that an operator decided while it was live. Such an
// activation decided nothing itself, so calling it decided would credit it with
// a decision it did not make, and calling it no-decision reads as a review that
// left the gate where it found it. Neither is what happened, which is why the
// outcome says decided-by-operator instead.
func (c coordinatorSupervision) activationTurnOutcome(
	attempt domain.Attempt,
	known bool,
	decisions int,
	operatorDecisions int,
) (domain.ActivationOutcome, string, error) {
	if decisions > 0 {
		return domain.ActivationOutcomeDecided,
			fmt.Sprintf("the activation recorded %d decision(s)", decisions), nil
	}
	if operatorDecisions > 0 {
		return domain.ActivationOutcomeDecidedByOperator,
			fmt.Sprintf("an operator recorded %d decision(s) at this activation's epoch; the overseer recorded none",
				operatorDecisions), nil
	}
	if known && attempt.Progress == domain.ProgressSucceeded {
		return domain.ActivationOutcomeNoDecision,
			"the activation's turn ended cleanly and recorded no decision", nil
	}
	return domain.ActivationOutcomeNoDecision,
		"the activation's turn ended without recording a decision", nil
}

// activationDecisionCount counts what this activation actually decided.
//
// Gate decisions carry the epoch they were made under, so a decision from a
// replaced overseer is not counted for its replacement.
func (c coordinatorSupervision) activationDecisionCount(ctx context.Context, runID string, epoch int64) (int, error) {
	projection, err := c.store.LoadSupervisionProjection(ctx, runID)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, decision := range projection.Decisions {
		if decision.ActivationEpoch == epoch && decision.Actor.Kind == domain.ActorOverseer {
			count++
		}
	}
	return count, nil
}

func activationAttemptOf(records sqlite.CoordinatorRecords, attemptID string) (domain.Attempt, bool) {
	for _, attempt := range records.Attempts {
		if attempt.ID == attemptID {
			return attempt, true
		}
	}
	return domain.Attempt{}, false
}
