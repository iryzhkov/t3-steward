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
	"sort"
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
// It runs after quota reconciliation even when that reconciliation failed,
// because already assigned activations still need lifecycle reconciliation.
// The caller supplies a fail-closed policy in that case, so no new overseer
// work is dispatched without a current admission answer.
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
			// Worker evidence is needed only for a new dispatch. Continue with
			// no candidates so completed, lost and expired activations still
			// reconcile, while every dispatch remains fail-closed.
			c.logger.Error("load worker snapshots for activation dispatch; continuing lifecycle reconciliation", "error", err)
			workers = nil
		}
	}
	now := c.at()
	// Order activation contenders by their durable per-epoch trigger age. Runs
	// that only need lifecycle reconciliation are still all visited, and an
	// ineligible older dispatch cannot block a later placeable one.
	type runOrder struct {
		at time.Time
		id string
	}
	orders := make(map[string]runOrder, len(records.WorkflowRuns))
	for _, run := range records.WorkflowRuns {
		if run.Supervision == nil || run.Progress.Terminal() {
			continue
		}
		state, stateErr := c.activations.Store.LoadSupervisionActivationState(ctx, run.ID)
		if stateErr != nil {
			continue
		}
		if !state.Activation.ReadyAt.IsZero() && state.Activation.ReadyTieID != "" {
			orders[run.ID] = runOrder{state.Activation.ReadyAt.UTC(), state.Activation.ReadyTieID}
			continue
		}
		inbox := backlog.CoalesceSupervisionEvents(run.ID, state.Record.EventCursor, state.Pending)
		if len(inbox.Events) != 0 {
			orders[run.ID] = runOrder{inbox.Events[0].OccurredAt.UTC(), inbox.Events[0].ID}
		}
	}
	sort.SliceStable(records.WorkflowRuns, func(i, j int) bool {
		left, lok := orders[records.WorkflowRuns[i].ID]
		right, rok := orders[records.WorkflowRuns[j].ID]
		if lok != rok {
			return lok
		}
		if !lok {
			return records.WorkflowRuns[i].ID < records.WorkflowRuns[j].ID
		}
		return left.at.Before(right.at) || left.at.Equal(right.at) && left.id < right.id
	})
	for _, run := range records.WorkflowRuns {
		if run.Supervision == nil {
			continue
		}
		if run.Progress.Terminal() {
			// A settled run wakes no overseer, and skipping it entirely is what
			// left the last activation of a settled run active with a live lease
			// forever: nothing else observes it, because every other lifecycle
			// signal is derived from an assignment this loop no longer reads.
			// Settlement is therefore the event that closes it.
			if err := c.closeSettledActivation(ctx, run); err != nil {
				c.logger.Error("closing the activation of a settled run failed", "run", run.ID, "error", err)
			}
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

type activationFairnessCandidate struct {
	ReadyAt time.Time
	ID      string
	Worker  string
	Pool    string
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
	// Placement gates only transitions that can create or retry a dispatch.
	// Completion, revocation and other reconciliation consume no new provider
	// or worker capacity, so closed admission or an unhealthy worker must not
	// prevent those durable lifecycle facts from being committed.
	var placement backlog.ActivationPlacement
	if activationSignalMayDispatch(signal.Event) {
		capacityWorkers := make([]domain.WorkerSnapshot, 0, len(workers))
		for _, worker := range workers {
			available, capacityErr := c.store.ExecutorSlotAvailable(ctx, worker.WorkerID, now)
			if capacityErr != nil {
				if errors.Is(capacityErr, sqlite.ErrExecutorCapacityEvidence) {
					continue
				}
				return fmt.Errorf("read activation executor capacity: %w", capacityErr)
			}
			if available {
				capacityWorkers = append(capacityWorkers, worker)
			}
		}
		placement, err = backlog.PlaceActivation(backlog.ActivationPlacementRequest{
			Route:     run.Supervision.Config.Route,
			Workers:   capacityWorkers,
			Epoch:     c.settings.CoordinatorEpoch,
			Now:       now,
			Admission: admission,
		})
		if err != nil {
			return err
		}
		if c.yieldToOlderWork != nil {
			readyAt, id, ageErr := activationDispatchAge(state, signal)
			if ageErr != nil {
				return ageErr
			}
			yield, yieldErr := c.yieldToOlderWork(ctx, activationFairnessCandidate{
				ReadyAt: readyAt, ID: id, Worker: placement.WorkerID, Pool: placement.Route.QuotaPoolID,
			})
			if yieldErr != nil {
				return fmt.Errorf("arbitrate activation admission: %w", yieldErr)
			}
			if yield {
				return nil
			}
		}
	}
	plan, err := c.activations.Advance(ctx, run.ID, signal)
	if err != nil {
		return err
	}
	c.logger.Info("supervision activation advanced",
		"run", run.ID, "event", signal.Event, "state", plan.Activation.State,
		"outcome", plan.Activation.Outcome, "reason", signal.Reason)
	if err := c.escalateNoDecision(ctx, run, plan, now); err != nil {
		return err
	}
	if plan.Dispatch == nil {
		return nil
	}
	attempt, assignment, err := backlog.ActivationAssignment(plan.Activation, *plan.Dispatch, placement,
		run.Supervision.Config.MaxTurnsPerActivation, now)
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
		CommittedAt: now, QuotaMaxConcurrent: c.quotaMaxConcurrent[placement.Route.QuotaPoolID],
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

// activationDispatchAge returns the durable age and stable identity of the
// dispatch being considered. A retry keeps the original activation identity and
// the triggering event's occurrence time, so restart cannot make it younger.
func activationDispatchAge(state backlog.SupervisionActivationState, signal backlog.ActivationSignal) (time.Time, string, error) {
	if !state.Activation.ReadyAt.IsZero() && state.Activation.ReadyTieID != "" {
		return state.Activation.ReadyAt.UTC(), state.Activation.ReadyTieID, nil
	}
	// Legacy records predate persisted ordering. Reconstruct only from events
	// selected after the durable cursor; otherwise yield conservatively at the
	// current boundary rather than borrowing a consumed incident's old age.
	var readyAt time.Time
	var id string
	for _, event := range state.Pending {
		if event.Sequence <= state.Record.EventCursor {
			continue
		}
		if readyAt.IsZero() || event.OccurredAt.Before(readyAt) || event.OccurredAt.Equal(readyAt) && event.ID < id {
			readyAt, id = event.OccurredAt.UTC(), event.ID
		}
	}
	if readyAt.IsZero() {
		return time.Time{}, "", fmt.Errorf("activation %q has no durable trigger age", state.Activation.DispatchIdentity)
	}
	return readyAt, id, nil
}

// activationSignalMayDispatch identifies the two lifecycle inputs that can
// produce assigned work. Keeping this check before Advance preserves the rule
// that an impossible dispatch spends no activation budget.
func activationSignalMayDispatch(event domain.ActivationEvent) bool {
	switch event {
	case domain.ActivationEventTriggerFired, domain.ActivationEventDispatchUndelivered:
		return true
	default:
		return false
	}
}

// escalateNoDecision asks a human to act when an overseer's turn ended without
// deciding anything.
//
// docs/plans/campaign-supervision.md forbids an automatic spin, so the run is
// not woken again on the same evidence: repeating the turn would spend the run's
// bounded activations on a review that already declined to decide, and it would
// do so without anything new to decide about. The activation's own bookkeeping
// consumes the inbox it reviewed, so nothing wakes a replacement either, and
// before this the run simply stopped with its review incident open and nobody
// told. One escalation is what turns that silence into a request for an
// operator; re-arming is the operator's own "campaign supervision reassess",
// which is the operator-reassessment trigger of seams section 3.3.
//
// It escalates once, because a spent activation produces no further lifecycle
// signal, and because both halves of the escalation are already idempotent: the
// incident resolution is keyed on the incident and the outbox entry's identity
// is derived from the run and the incident.
func (c coordinatorSupervision) escalateNoDecision(
	ctx context.Context,
	run domain.WorkflowRun,
	plan backlog.ActivationPlan,
	now time.Time,
) error {
	if run.Supervision == nil || plan.Activation.Outcome != domain.ActivationOutcomeNoDecision {
		return nil
	}
	incidentID := plan.Activation.IncidentID
	if incidentID == "" {
		// An activation woken for no particular incident has no open review to
		// escalate, and inventing one would report an incident nothing raised.
		c.logger.Warn("supervision activation ended without a decision and names no incident",
			"run", run.ID, "activation", plan.Activation.ID, "epoch", plan.Activation.Epoch)
		return nil
	}
	reason := fmt.Sprintf("overseer activation %s ended without a decision", plan.Activation.ID)
	c.logger.Warn("supervision activation ended without a decision; escalating",
		"run", run.ID, "activation", plan.Activation.ID, "epoch", plan.Activation.Epoch,
		"incident", incidentID)
	return c.escalate(ctx, *run.Supervision, incidentID, plan.Inbox.EventIDs(), reason, now)
}

// closeSettledActivation closes the activation of a run that has settled.
//
// A settled run revokes every mutating supervision capability, so an activation
// still recorded as pending-dispatch or active holds a lease nobody may use and
// claims an overseer nobody is waiting for. Its own lifecycle never notices,
// because every other signal is read from an assignment that is gone by then.
// Closing it here clears the lease and records the outcome, and the transition
// is idempotent: a closed activation is skipped on the next boundary rather
// than closed twice.
func (c coordinatorSupervision) closeSettledActivation(ctx context.Context, run domain.WorkflowRun) error {
	state, err := c.activations.Store.LoadSupervisionActivationState(ctx, run.ID)
	if errors.Is(err, backlog.ErrSupervisionNotConfigured) {
		return nil
	}
	if err != nil {
		return err
	}
	switch state.Activation.State {
	case domain.ActivationPendingDispatch, domain.ActivationActive:
	default:
		// Idle, spent, revoked, escalated, recovery-required and closed all
		// hold no live overseer, so settlement has nothing to close.
		return nil
	}
	plan, err := c.activations.Advance(ctx, run.ID, backlog.ActivationSignal{
		Event:                  domain.ActivationEventRunSettled,
		Actor:                  domain.Actor{Kind: domain.ActorOperator, Principal: coordinatorSupervisionPrincipal},
		ExpectedEpoch:          state.Activation.Epoch,
		ExpectedRecordRevision: state.Record.Revision,
		Principal:              c.settings.principalID(),
		IncidentID:             state.Activation.IncidentID,
		Reason:                 "the run settled, so this activation holds no decision authority",
	})
	if err != nil {
		return err
	}
	c.logger.Info("supervision activation closed on run settlement",
		"run", run.ID, "activation", plan.Activation.ID, "epoch", plan.Activation.Epoch,
		"state", plan.Activation.State, "outcome", plan.Activation.Outcome)
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
	case "", domain.ActivationIdle:
		signal.Event = domain.ActivationEventTriggerFired
	case domain.ActivationSpent:
		// A spent activation has finished its turn, and a trigger aimed at one
		// is an illegal transition rather than a second wake. What new events
		// do to a spent activation is return the run to idle at a fresh epoch;
		// the trigger that dispatches the replacement is then the next
		// boundary's, against an identity the spent activation never used.
		signal.Event = domain.ActivationEventEventsArrived
	default:
		// Active, revoked, recovery-required, escalated and closed all mean
		// something other than this boundary owns the next move.
		return signal, false, nil
	}
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
