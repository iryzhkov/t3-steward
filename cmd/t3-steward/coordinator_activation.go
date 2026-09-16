package main

// The coordinator boundary that turns a woken overseer into running work.
//
// The supervision tick above observes: it makes a gate reviewable and raises
// the incident a decision closes. This is what makes the decision possible: it
// asks the activation lifecycle whether an overseer should wake, and when the
// answer carries a dispatch it places that activation on a capable worker and
// commits its attempt and assignment as ordinary assigned work.
//
// It decides nothing about the run. Every gate, hold and incident is still
// decided through the admin supervision operation, by the overseer's own CLI or
// by an operator.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// coordinatorActivationSettings is the deployment-supplied half of an
// activation package: who the overseer authenticates as, and the limits and
// catalog identity every execution package carries.
type coordinatorActivationSettings struct {
	CoordinatorID    string
	CoordinatorEpoch int64
	CatalogRevision  string
	// SupervisorClient is the admin client name the overseer's CLI authenticates
	// as on the worker host, exactly as backlog_v2.coordinator.admin_clients
	// spells it.
	//
	// An empty value disables activation dispatch entirely: without a principal
	// there is no capability to scope, and dispatching an overseer that could
	// decide nothing would only consume budget.
	SupervisorClient string
	// SupervisorCredentialReference names the credential the CLI resolves. See
	// the co-tenancy limitation recorded on workerproto.SupervisionActivation:
	// this is placement, not isolation.
	SupervisorCredentialReference string
}

// configured reports whether activation dispatch can run at all.
func (s coordinatorActivationSettings) configured() bool {
	return s.SupervisorClient != "" && s.CoordinatorEpoch >= 1
}

// principalID is the identity the coordinator records on an activation, and the
// one its own authorizer resolves scope under.
//
// It is the relayed form, not the bare client name. Every admin request reaches
// the coordinator through the local server, which rewrites a relayed client's
// principal, so an activation recorded under the bare name would match nothing
// and the overseer would be refused every operation it was woken to perform.
func (s coordinatorActivationSettings) principalID() string {
	return backlogadmin.RelayedPrincipalID(s.SupervisorClient)
}

// coordinatorSupervisorClient resolves the one admin client this coordinator
// will dispatch overseers as.
//
// Exactly one, or none. Two supervisor clients would make the principal an
// overseer authenticates as ambiguous, and the authorizer resolves scope by
// principal: picking one of them would silently decide which credential may act
// on a run. Naming the ambiguity and dispatching nothing is the safe answer.
func coordinatorSupervisorClient(clients map[string]config.V2AdminClient) (string, string, error) {
	var principal, credential string
	for name, client := range clients {
		if !client.Supervisor {
			continue
		}
		if principal != "" {
			return "", "", fmt.Errorf(
				"backlog_v2.coordinator.admin_clients names more than one supervisor (%q and %q); an overseer must authenticate as exactly one principal",
				principal, name)
		}
		principal, credential = name, client.Credential
	}
	return principal, credential, nil
}

// DispatchActivations wakes every supervised run that has pending supervision
// events and no live overseer, and re-offers every activation whose dispatch
// was planned but never committed.
//
// It runs after quota reconciliation and only when that reconciliation
// succeeded, because an overseer obeys the same automatic admission gates as
// every other route. With no current quota answer the honest action is to leave
// the gate closed and say so, not to dispatch a review the fleet cannot afford.
func (c coordinatorSupervision) DispatchActivations(ctx context.Context, admission backlog.WorkerAdmissionPolicy) {
	if c.activations.Store == nil || !c.settings.configured() {
		return
	}
	records, err := c.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		c.logger.Error("load records for activation dispatch", "error", err)
		return
	}
	var workers []domain.WorkerSnapshot
	if c.workers != nil {
		if workers, err = c.workers(ctx); err != nil {
			c.logger.Error("load worker snapshots for activation dispatch", "error", err)
			return
		}
	}
	now := c.at()
	for _, run := range records.WorkflowRuns {
		if run.Supervision == nil || run.Progress.Terminal() {
			continue
		}
		if err := c.dispatchRun(ctx, records, run, workers, admission, now); err != nil {
			if errors.Is(err, backlog.ErrActivationUnplaceable) {
				// No worker can run this overseer right now. The gate stays
				// closed, unrelated branches keep being scheduled, and the route
				// escalation above is what tells a human when it persists.
				c.logger.Info("supervision activation is waiting for a capable worker",
					"run", run.ID, "reason", err)
				continue
			}
			c.logger.Error("supervision activation dispatch failed", "run", run.ID, "error", err)
		}
	}
}

func (c coordinatorSupervision) dispatchRun(
	ctx context.Context,
	records sqlite.CoordinatorRecords,
	run domain.WorkflowRun,
	workers []domain.WorkerSnapshot,
	admission backlog.WorkerAdmissionPolicy,
	now time.Time,
) error {
	state, err := c.activations.Store.LoadSupervisionActivationState(ctx, run.ID)
	if errors.Is(err, backlog.ErrSupervisionNotConfigured) {
		return nil
	}
	if err != nil {
		return err
	}
	// A dispatched activation's own durable records come first. Reading what
	// already happened to it before asking whether to wake another one is what
	// keeps a finished, revoked or lost activation from being left behind while
	// its successor is planned. Exactly one transition per run per boundary, so
	// that every signal is derived from records this pass actually read.
	signal, wanted, err := c.activationLifecycleSignal(ctx, records, state)
	if err != nil {
		return err
	}
	if !wanted {
		if signal, wanted, err = c.activationSignal(records, state); err != nil || !wanted {
			return err
		}
	}
	// Placement runs before the lifecycle is advanced. An activation whose
	// worker does not exist would otherwise spend one of the run's bounded
	// activations on a dispatch that was never possible.
	placement, err := backlog.PlaceActivation(backlog.ActivationPlacementRequest{
		Route:     run.Supervision.Config.Route,
		Workers:   workers,
		Epoch:     c.settings.CoordinatorEpoch,
		Now:       now,
		Admission: admission,
	})
	if err != nil {
		return err
	}
	plan, err := c.activations.Advance(ctx, run.ID, signal)
	if err != nil {
		return err
	}
	c.logger.Info("supervision activation advanced",
		"run", run.ID, "event", signal.Event, "state", plan.Activation.State,
		"outcome", plan.Activation.Outcome, "reason", signal.Reason)
	if plan.Dispatch == nil {
		return nil
	}
	attempt, assignment, err := backlog.ActivationAssignment(plan.Activation, *plan.Dispatch, placement, now)
	if err != nil {
		return err
	}
	assignment.WorkerEpoch = placement.WorkerEpoch
	// The execution package itself is rendered where every other offer is
	// rendered, by the per-worker offer builder at delivery time, so that the
	// lease and the epoch the worker is handed are the ones that are still true
	// when it is handed them rather than the ones planning saw.
	committed, err := c.store.CommitActivationAssignment(ctx, sqlite.ActivationAssignmentCommit{
		CoordinatorEpoch: c.settings.CoordinatorEpoch,
		Attempt:          attempt, Assignment: assignment,
		WorkerEpoch: placement.WorkerEpoch, WorkerSnapshotSequence: placement.SnapshotSequence,
		CommittedAt: now,
	})
	if err != nil {
		return err
	}
	c.logger.Info("supervision activation offered as assigned work",
		"run", run.ID, "activation", plan.Activation.ID, "epoch", plan.Activation.Epoch,
		"worker", committed.WorkerID, "assignment", committed.ID, "thread", committed.ThreadID,
		"retry", plan.Dispatch.Retry)
	return nil
}

// activationSignal decides which lifecycle event this boundary carries, or that
// it carries none.
//
// Two conditions wake an overseer here. Pending events with no live activation
// are a trigger. A pending-dispatch activation whose assignment was never
// committed is a provably undelivered dispatch: the coordinator planned it and
// no durable record of an offer exists, so the retry reuses the original
// dispatch identity and spends no budget.
func (c coordinatorSupervision) activationSignal(
	records sqlite.CoordinatorRecords,
	state backlog.SupervisionActivationState,
) (backlog.ActivationSignal, bool, error) {
	actor := domain.Actor{Kind: domain.ActorOperator, Principal: coordinatorSupervisionPrincipal}
	signal := backlog.ActivationSignal{
		Actor:                  actor,
		ExpectedRecordRevision: state.Record.Revision,
		Principal:              c.settings.principalID(),
	}
	if state.Activation.State == domain.ActivationPendingDispatch {
		if _, offered := activationAssignmentOf(records, state.Activation); offered {
			// The dispatch is durable. Whether it started is the worker
			// reconciliation's answer, not this boundary's.
			return signal, false, nil
		}
		signal.Event = domain.ActivationEventDispatchUndelivered
		signal.ExpectedEpoch = state.Activation.Epoch
		signal.IncidentID = state.Activation.IncidentID
		signal.Reason = "the planned activation dispatch left no durable assignment"
		return signal, true, nil
	}
	inbox := backlog.CoalesceSupervisionEvents(state.Record.RunID, state.Record.EventCursor, state.Pending)
	if !inbox.NonEmpty() || state.OtherValidActivation {
		return signal, false, nil
	}
	switch state.Activation.State {
	case "", domain.ActivationIdle, domain.ActivationSpent:
	default:
		// Active, revoked, recovery-required, escalated and closed all mean
		// something other than this boundary owns the next move.
		return signal, false, nil
	}
	signal.Event = domain.ActivationEventTriggerFired
	signal.ExpectedEpoch = state.Activation.Epoch
	signal.IncidentID = firstIncidentOfInbox(inbox)
	signal.Reason = fmt.Sprintf("%d supervision event(s) are waiting for review", len(inbox.EventIDs()))
	return signal, true, nil
}

// firstIncidentOfInbox names the incident the wake belongs to, which is what
// bounds automatic recovery per incident.
func firstIncidentOfInbox(inbox backlog.ActivationInbox) string {
	for _, trigger := range inbox.Triggers {
		for _, incident := range trigger.IncidentIDs {
			if incident != "" {
				return incident
			}
		}
	}
	return ""
}

// activationAssignmentOf finds the durable assignment of one activation.
func activationAssignmentOf(records sqlite.CoordinatorRecords, activation domain.Activation) (domain.Assignment, bool) {
	attemptID := backlog.ActivationAttemptID(activation.DispatchIdentity)
	for _, assignment := range records.Assignments {
		if assignment.AttemptID == attemptID {
			return assignment, true
		}
	}
	return domain.Assignment{}, false
}
