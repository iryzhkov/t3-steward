package backlog

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Supervision activation lifecycle: triggers, coalescing, dispatch identity and
// the bookkeeping around domain.ActivationTransition.
//
// The state machine itself is frozen in internal/domain (Lane B1). Nothing here
// re-decides a transition; this file supplies the transaction-time guards that
// machine asks for, and applies the durable consequences it reports: which
// epoch the activation is now at, whether the transition spent budget, and
// whether an operator authorized a fresh one.
//
// Three rules shape the design.
//
// Correctness comes from the durable record, not from conversation history. A
// replacement overseer is started from a compact snapshot at a new epoch, so
// every fact an activation needs is either in the activation record or in the
// bounded envelope built for it.
//
// The high-water mark and the outcome are recorded together. An activation
// consumes the inbox as it stood when it was dispatched; events that arrive
// while it reviews stay pending behind that mark and wake the next activation.
// An activation that was revoked or expired consumed nothing, so its inbox
// survives it untouched.
//
// Delivery is at least once. A duplicate trigger, a duplicate dispatch and a
// duplicate wake acknowledgement are all idempotent, and a provably undelivered
// dispatch is retried with the identity it already had rather than a new one,
// so a retry can never be mistaken for a second activation.

// DefaultActivationLeaseTTL is how long a coordinator-issued activation lease is
// valid before it must be renewed. It is deliberately much shorter than the
// activation deadline: the deadline bounds the review, the lease bounds how long
// a coordinator that stops hearing from the overseer keeps believing in it.
const DefaultActivationLeaseTTL = 15 * time.Minute

// SupervisionTriggerKind names five emitted conditions and one compatibility-
// reserved value. The set is closed on purpose. A scheduler tick is not in it, and
// neither is an ordinary task success: waking on either would make the overseer
// a poller, and its budget is bounded.
type SupervisionTriggerKind string

const (
	// TriggerGateReviewReady fires when a gate reaches ready-for-review.
	TriggerGateReviewReady SupervisionTriggerKind = "gate-review-ready"
	// TriggerTaskJudgmentRequired fires when a task fails or needs input in a
	// way that requires judgment rather than an ordinary retry.
	TriggerTaskJudgmentRequired SupervisionTriggerKind = "task-judgment-required"
	// TriggerRouteBlockPersistent fires when a capacity or route block has
	// persisted past its configured threshold. Ordinary brief quota waiting
	// never produces it; see SupervisionBlockObservation.
	TriggerRouteBlockPersistent SupervisionTriggerKind = "route-block-persistent"
	// TriggerOperatorReassessment fires when an operator asks for one.
	TriggerOperatorReassessment SupervisionTriggerKind = "operator-reassessment"
	// TriggerReviewTimeout fires when a pending review times out.
	TriggerReviewTimeout SupervisionTriggerKind = "review-timeout"
	// TriggerTerminationFinalReport is reserved for compatibility. No current
	// producer emits it, and campaign plans must not promise a final-report
	// activation. Terminal reporting cannot restore mutation authority.
	TriggerTerminationFinalReport SupervisionTriggerKind = "termination-final-report"
)

// Valid reports whether the kind is one of the six.
func (k SupervisionTriggerKind) Valid() bool {
	switch k {
	case TriggerGateReviewReady, TriggerTaskJudgmentRequired, TriggerRouteBlockPersistent,
		TriggerOperatorReassessment, TriggerReviewTimeout, TriggerTerminationFinalReport:
		return true
	}
	return false
}

// SupervisionEvent is one durable inbox entry: a trigger that fired, with the
// evidence references an overseer needs to act on it. It carries identities and
// digests, never bodies; a large artifact is fetched on demand.
type SupervisionEvent struct {
	ID    string                 `json:"id"`
	RunID string                 `json:"runId"`
	Kind  SupervisionTriggerKind `json:"kind"`
	// Sequence is the run-local ordering and the high-water mark unit. It is
	// assigned by the store when the event is appended.
	Sequence int64 `json:"sequence"`
	// Reason is the normalized reason text. For a route block it is the
	// normalized block reason, so repeated waiting under one reason is one
	// reason rather than a stream of distinct strings.
	Reason string `json:"reason"`
	// The evidence references. Each is optional; which ones are set depends on
	// the kind.
	GateID        string                  `json:"gateId,omitempty"`
	TaskID        string                  `json:"taskId,omitempty"`
	AttemptID     string                  `json:"attemptId,omitempty"`
	IncidentID    string                  `json:"incidentId,omitempty"`
	GraphRevision int64                   `json:"graphRevision,omitempty"`
	Artifacts     []domain.ArtifactDigest `json:"artifacts,omitempty"`
	OccurredAt    time.Time               `json:"occurredAt"`
	// AcknowledgedPurposes is store metadata, never part of the immutable event.
	AcknowledgedPurposes []domain.RecoveryActivationPurpose `json:"-"`
}

// Validate rejects an event that cannot be ordered, deduplicated or explained.
func (e SupervisionEvent) Validate() error {
	switch {
	case strings.TrimSpace(e.ID) == "":
		return errors.New("supervision event requires an id")
	case strings.TrimSpace(e.RunID) == "":
		return errors.New("supervision event requires a run id")
	case !e.Kind.Valid():
		return fmt.Errorf("supervision event has unknown trigger kind %q", e.Kind)
	case strings.TrimSpace(e.Reason) == "":
		return errors.New("supervision event requires a normalized reason")
	case e.OccurredAt.IsZero():
		return errors.New("supervision event requires an occurrence time")
	}
	return nil
}

// subject is what the event is about. Coalescing groups by it, so two reports
// about one gate fold together while reports about two gates never do.
func (e SupervisionEvent) subject() string {
	switch {
	case e.GateID != "":
		return "gate:" + e.GateID
	case e.IncidentID != "":
		return "incident:" + e.IncidentID
	case e.TaskID != "":
		return "task:" + e.TaskID
	default:
		return "run:" + e.RunID
	}
}

// CoalescedTrigger is one subject an activation must consider, with every
// evidence reference that was folded into it. Coalescing reduces how many
// things the overseer is told about; it never reduces what it can look at.
type CoalescedTrigger struct {
	Kind          SupervisionTriggerKind  `json:"kind"`
	Subject       string                  `json:"subject"`
	Reasons       []string                `json:"reasons"`
	EventIDs      []string                `json:"eventIds"`
	TaskIDs       []string                `json:"taskIds,omitempty"`
	AttemptIDs    []string                `json:"attemptIds,omitempty"`
	IncidentIDs   []string                `json:"incidentIds,omitempty"`
	Artifacts     []domain.ArtifactDigest `json:"artifacts,omitempty"`
	GraphRevision int64                   `json:"graphRevision,omitempty"`
	FirstSequence int64                   `json:"firstSequence"`
	LastSequence  int64                   `json:"lastSequence"`
	OccurredAt    time.Time               `json:"occurredAt"`
}

// ActivationInbox is the pending work of one run, as it stood at one moment.
type ActivationInbox struct {
	RunID    string             `json:"runId"`
	Events   []SupervisionEvent `json:"events"`
	Triggers []CoalescedTrigger `json:"triggers"`
	// HighWaterMark is the largest sequence in Events. An activation dispatched
	// from this inbox consumes exactly up to it.
	HighWaterMark int64 `json:"highWaterMark"`
	// Duplicates counts repeated deliveries of an event ID already present. It
	// exists so at-least-once delivery is observable rather than silent.
	Duplicates int `json:"duplicates"`
}

// NonEmpty reports whether anything is waiting to be consumed.
func (i ActivationInbox) NonEmpty() bool { return len(i.Events) > 0 }

// EventIDs lists every event reference in the inbox, in order.
func (i ActivationInbox) EventIDs() []string {
	ids := make([]string, 0, len(i.Events))
	for _, event := range i.Events {
		ids = append(ids, event.ID)
	}
	return ids
}

// CoalesceSupervisionEvents folds a run's pending events into the inbox one
// activation will consume.
//
// Events at or below the cursor were consumed by an earlier activation and are
// dropped. A repeated event ID is counted and dropped, which is what makes a
// duplicate delivery idempotent. What survives is grouped by kind and subject,
// and every group keeps every event ID, reason and artifact digest it absorbed,
// so a coalesced trigger is a shorter statement of the same evidence.
//
// The result is deterministic: events are ordered by sequence and then by ID,
// and groups are ordered by first sequence.
func CoalesceSupervisionEvents(runID string, cursor int64, events []SupervisionEvent) ActivationInbox {
	inbox := ActivationInbox{RunID: runID}
	ordered := append([]SupervisionEvent(nil), events...)
	sort.SliceStable(ordered, func(left, right int) bool {
		if ordered[left].Sequence != ordered[right].Sequence {
			return ordered[left].Sequence < ordered[right].Sequence
		}
		return ordered[left].ID < ordered[right].ID
	})
	seen := make(map[string]struct{}, len(ordered))
	index := make(map[string]int, len(ordered))
	for _, event := range ordered {
		if event.RunID != runID || event.Sequence <= cursor {
			continue
		}
		if _, duplicate := seen[event.ID]; duplicate {
			inbox.Duplicates++
			continue
		}
		seen[event.ID] = struct{}{}
		inbox.Events = append(inbox.Events, event)
		if event.Sequence > inbox.HighWaterMark {
			inbox.HighWaterMark = event.Sequence
		}
		key := string(event.Kind) + "\x00" + event.subject()
		at, found := index[key]
		if !found {
			inbox.Triggers = append(inbox.Triggers, CoalescedTrigger{
				Kind: event.Kind, Subject: event.subject(),
				FirstSequence: event.Sequence, LastSequence: event.Sequence,
				GraphRevision: event.GraphRevision, OccurredAt: event.OccurredAt.UTC(),
			})
			at = len(inbox.Triggers) - 1
			index[key] = at
		}
		trigger := &inbox.Triggers[at]
		trigger.EventIDs = append(trigger.EventIDs, event.ID)
		trigger.Reasons = appendDistinct(trigger.Reasons, event.Reason)
		trigger.TaskIDs = appendDistinct(trigger.TaskIDs, event.TaskID)
		trigger.AttemptIDs = appendDistinct(trigger.AttemptIDs, event.AttemptID)
		trigger.IncidentIDs = appendDistinct(trigger.IncidentIDs, event.IncidentID)
		trigger.Artifacts = appendDistinctArtifacts(trigger.Artifacts, event.Artifacts)
		if event.Sequence > trigger.LastSequence {
			trigger.LastSequence = event.Sequence
			trigger.OccurredAt = event.OccurredAt.UTC()
		}
		if event.GraphRevision > trigger.GraphRevision {
			trigger.GraphRevision = event.GraphRevision
		}
	}
	return inbox
}

func supervisionEventsForPurpose(events []SupervisionEvent, purpose domain.RecoveryActivationPurpose) []SupervisionEvent {
	selected := make([]SupervisionEvent, 0, len(events))
	for _, event := range events {
		eventPurpose := domain.RecoveryActivationPurpose("")
		if event.Kind == TriggerTaskJudgmentRequired {
			eventPurpose = domain.RecoveryActivationRepair
		}
		if eventPurpose != purpose || eventAcknowledgedFor(event, purpose) {
			continue
		}
		selected = append(selected, event)
	}
	return selected
}

func eventAcknowledgedFor(event SupervisionEvent, purpose domain.RecoveryActivationPurpose) bool {
	for _, acknowledged := range event.AcknowledgedPurposes {
		if acknowledged == purpose {
			return true
		}
	}
	return false
}

func appendDistinct(values []string, value string) []string {
	if strings.TrimSpace(value) == "" {
		return values
	}
	for _, current := range values {
		if current == value {
			return values
		}
	}
	return append(values, value)
}

func appendDistinctArtifacts(values []domain.ArtifactDigest, additions []domain.ArtifactDigest) []domain.ArtifactDigest {
	for _, addition := range additions {
		if addition.ArtifactID == "" {
			continue
		}
		duplicate := false
		for _, current := range values {
			if current == addition {
				duplicate = true
				break
			}
		}
		if !duplicate {
			values = append(values, addition)
		}
	}
	return values
}

// SupervisionBlockObservation is the running state of one normalized block
// reason for a run. It exists so a capacity or route block raises a trigger only
// when the reason changes or the threshold is crossed, and never on every tick.
type SupervisionBlockObservation struct {
	RunID    string    `json:"runId"`
	Reason   string    `json:"reason"`
	Since    time.Time `json:"since"`
	Reported bool      `json:"reported"`
}

// ObserveSupervisionBlock folds one block report into the observation and
// reports whether a trigger should be emitted.
//
// A changed reason restarts the clock and is not itself an event: ordinary
// brief quota or resource waiting changes reason all the time. An event is
// emitted once, when one reason has held continuously for the threshold. An
// empty reason clears the observation, which is how recovery is recorded.
func ObserveSupervisionBlock(
	state SupervisionBlockObservation,
	runID, reason string,
	threshold time.Duration,
	now time.Time,
) (SupervisionBlockObservation, bool) {
	now = now.UTC()
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return SupervisionBlockObservation{RunID: runID}, false
	}
	if state.Reason != reason || state.RunID != runID {
		return SupervisionBlockObservation{RunID: runID, Reason: reason, Since: now}, false
	}
	if state.Reported || threshold <= 0 || now.Sub(state.Since) < threshold {
		return state, false
	}
	state.Reported = true
	return state, true
}

// ActivationID is the deterministic identity of the activation of one run at one
// epoch.
func ActivationID(runID string, epoch int64) string {
	return stableCoordinatorID("activation", fmt.Sprintf("%s@%d", runID, epoch))
}

// ActivationDispatchIdentity is the deterministic dispatch identity of one
// activation. A provably undelivered dispatch is retried with this same value,
// so the retry is recognisably the same dispatch rather than a second one.
func ActivationDispatchIdentity(runID string, epoch int64) string {
	return stableCoordinatorID("activation-dispatch", fmt.Sprintf("%s@%d", runID, epoch))
}

// ActivationLeaseValid reports whether the coordinator-issued lease is still
// live at the given moment. Expiry revokes decision authority immediately: it is
// not a grace period and not a warning.
func ActivationLeaseValid(activation domain.Activation, now time.Time) bool {
	return domain.ActivationLeaseLive(activation, now)
}

// ActivationExpired reports whether the activation is past its maximum elapsed
// time.
func ActivationExpired(activation domain.Activation, now time.Time) bool {
	return domain.ActivationPastDeadline(activation, now)
}

// SupervisionActivationState is everything one run's activation decision reads.
// It is loaded once, inside the caller's transaction, and handed here whole, so
// the planning below performs no I/O and can be tested as a function.
type SupervisionActivationState struct {
	Record     domain.SupervisionRecord
	Activation domain.Activation
	// Pending is the durable inbox, including events that arrived during a
	// review and events already consumed by an earlier activation.
	Pending []SupervisionEvent
	// OtherValidActivation reports another valid activation for this run. At
	// most one may exist, and the domain machine refuses a second.
	OtherValidActivation bool
	// Outbox is this run's live delivery intents, used to deduplicate a
	// re-escalation of an incident that was already notified.
	Outbox []SupervisionOutboxEntry
	// SubmitterThreadID is the thread the submission asked to be woken, when it
	// asked for one. It is the escalation destination of a manifest that
	// requests a notification without naming a thread; see
	// SupervisionEscalationThread.
	SubmitterThreadID string
}

// ActivationSignal is one event applied to one run's activation, with the facts
// only the caller's transaction can establish.
type ActivationSignal struct {
	Event   domain.ActivationEvent
	Actor   domain.Actor
	Purpose domain.RecoveryActivationPurpose
	// IncidentID names the incident this signal belongs to. Escalation
	// deduplicates on it, and automatic recovery is bounded per incident.
	IncidentID string
	// ExpectedEpoch and ExpectedRecordRevision are the fences a decision names.
	ExpectedEpoch          int64
	ExpectedRecordRevision int64
	// ExecutionObserved reports that the dispatched activation was ever seen
	// executing. A started-then-vanished activation counts toward the budget; a
	// provably undelivered dispatch does not.
	ExecutionObserved bool
	// RuntimeProvenStopped reports a reconciled runtime proven stopped, and
	// RecoveryComplete an effect-safe recovery that finished. An ambiguous live
	// runtime authorizes no replacement.
	RuntimeProvenStopped bool
	RecoveryComplete     bool
	// OperatorAuthorized and GrantActivations carry an operator continuation.
	OperatorAuthorized bool
	GrantActivations   int
	// Principal is the admin principal the dispatched overseer will
	// authenticate as. It is recorded on the activation when one is created, so
	// that the coordinator's own authorizer resolves that principal's scope from
	// the activation it issued rather than from the credential's own claims.
	// Between activations the same principal resolves to no run at all.
	Principal string
	// Outcome overrides the recorded outcome when the caller knows it, for
	// example decided rather than no-decision on a spent activation.
	Outcome   domain.ActivationOutcome
	Reason    string
	RequestID string
}

// ContinuationReceipt records an operator-authorized continuation. Counters are
// never silently reset; a continuation raises the granted budget and says who
// raised it, when, and by how much.
type ContinuationReceipt struct {
	RunID              string       `json:"runId"`
	RequestID          string       `json:"requestId"`
	Actor              domain.Actor `json:"actor"`
	Epoch              int64        `json:"epoch"`
	GrantedActivations int          `json:"grantedActivations"`
	Reason             string       `json:"reason,omitempty"`
	IssuedAt           time.Time    `json:"issuedAt"`
}

// ActivationDispatch is what the dispatcher must do after the plan commits.
type ActivationDispatch struct {
	// Identity is the deterministic dispatch identity. On a retry it is the
	// identity the original dispatch had.
	Identity string
	Epoch    int64
	// Retry reports a retry of a provably undelivered dispatch. It spends no
	// budget and issues no new identity.
	Retry          bool
	LeaseToken     string
	LeaseExpiresAt time.Time
	Deadline       time.Time
	// RequiredCapability is the worker capability placement must gate on.
	RequiredCapability string
}

// ActivationPlan is the complete durable consequence of one signal. Nothing in
// it has been written yet: a caller commits it in one transaction, or commits
// none of it.
type ActivationPlan struct {
	RunID      string
	Result     domain.ActivationTransitionResult
	Record     domain.SupervisionRecord
	Activation domain.Activation
	Inbox      ActivationInbox
	// ConsumedThrough is the high-water mark the activation is bound to, and
	// CursorAdvanced reports that this commit records it as consumed. They are
	// written with the outcome or not at all.
	ConsumedThrough        int64
	CursorAdvanced         bool
	AcknowledgedEventIDs   []string
	AcknowledgementPurpose domain.RecoveryActivationPurpose
	Outbox                 []SupervisionOutboxEntry
	Dispatch               *ActivationDispatch
	Receipt                *ContinuationReceipt
	// Escalated reports that this plan asks a human to act.
	Escalated bool
}

// PlanActivation applies one signal to one run's supervision and returns the
// durable consequence, without performing any I/O.
//
// The transition itself is domain.ActivationTransition. What this adds is the
// bookkeeping the machine deliberately leaves to its caller: budget counters,
// lease issuance and revocation, the deterministic dispatch identity, the bound
// high-water mark, the outcome, the bounded automatic recovery per incident and
// the escalation delivery intent.
func PlanActivation(state SupervisionActivationState, signal ActivationSignal, now time.Time) (ActivationPlan, error) {
	record := state.Record
	if strings.TrimSpace(record.RunID) == "" {
		return ActivationPlan{}, errors.New("supervision activation planning requires a supervision record")
	}
	now = now.UTC()
	activation := state.Activation
	if activation.ID == "" && activation.Epoch == 0 {
		// The first activation of a run inherits the record's epoch, so the
		// identity it derives is the one every other reader of the record
		// derives for the same epoch.
		activation.Epoch = record.ActivationEpoch
		if activation.Epoch <= 0 {
			activation.Epoch = 1
		}
	}
	purpose := activation.Purpose
	if signal.Event == domain.ActivationEventTriggerFired {
		purpose = signal.Purpose
	}
	cursor := int64(0)
	if purpose == "" {
		// EventCursor remains the compatibility projection for legacy reviewer
		// activations. Repair ownership is tracked only by explicit acknowledgements.
		cursor = record.EventCursor
	}
	selected := supervisionEventsForPurpose(state.Pending, purpose)
	if signal.Event != domain.ActivationEventTriggerFired && activation.ConsumedEventCursor > 0 {
		bound := selected[:0]
		for _, event := range selected {
			if event.Sequence <= activation.ConsumedEventCursor {
				bound = append(bound, event)
			}
		}
		selected = bound
	}
	inbox := CoalesceSupervisionEvents(record.RunID, cursor, selected)
	result, err := domain.ActivationTransition(domain.ActivationTransitionInput{
		Activation:                activation,
		Event:                     signal.Event,
		Actor:                     signal.Actor,
		Supervised:                true,
		InboxNonEmpty:             inbox.NonEmpty(),
		ActivationBudgetRemaining: record.ActivationBudgetRemaining(),
		TurnsRemaining:            activation.TurnsUsed < record.Config.MaxTurnsPerActivation,
		OtherValidActivation:      state.OtherValidActivation,
		LeaseValid:                ActivationLeaseValid(activation, now),
		ExpectedEpoch:             signal.ExpectedEpoch,
		RevisionMatches:           signal.ExpectedRecordRevision == record.Revision,
		ExecutionObserved:         signal.ExecutionObserved,
		RuntimeProvenStopped:      signal.RuntimeProvenStopped,
		RecoveryComplete:          signal.RecoveryComplete,
		OperatorAuthorized:        signal.OperatorAuthorized,
	})
	if err != nil {
		return ActivationPlan{}, err
	}
	plan := ActivationPlan{RunID: record.RunID, Result: result, Inbox: inbox}
	next := activation
	next.RunID = record.RunID
	next.Epoch = result.Epoch
	next.State = result.State
	if result.CountsTowardBudget {
		record.ActivationsUsed++
	}

	switch signal.Event {
	case domain.ActivationEventTriggerFired:
		if result.State == domain.ActivationPendingDispatch {
			// The recovery budget belongs to one incident. A wake for a different
			// incident starts a fresh one; a wake for the same incident keeps the
			// count, which is what bounds automatic recovery per incident.
			recovered := activation.RecoveredCount
			if signal.IncidentID != activation.IncidentID {
				recovered = 0
			}
			next = newActivation(record, result.Epoch, inbox.HighWaterMark, recovered)
			next.Purpose = purpose
			if len(inbox.Events) != 0 {
				next.ReadyAt = inbox.Events[0].OccurredAt.UTC()
				next.ReadyTieID = inbox.Events[0].ID
			}
			next.IncidentID = signal.IncidentID
			next.Principal = signal.Principal
			plan.Dispatch = issueActivationLease(&next, record, now, false)
		}
	case domain.ActivationEventDispatchUndelivered:
		// The retry keeps the original dispatch identity and the original bound
		// high-water mark, and spends no budget.
		plan.Dispatch = issueActivationLease(&next, record, now, true)
	case domain.ActivationEventDispatchConfirmed:
		started := now
		if activation.StartedAt != nil {
			started = activation.StartedAt.UTC()
		}
		next.StartedAt = &started
		issueActivationLease(&next, record, now, true)
	case domain.ActivationEventDecisionRecorded:
		next.TurnsUsed = activation.TurnsUsed + 1
		issueActivationLease(&next, record, now, true)
	case domain.ActivationEventThreadLost:
		if result.State == domain.ActivationIdle {
			// A replacement is automatic only twice per incident. The third
			// failure escalates instead of respawning, per the plan.
			recovered := activation.RecoveredCount + 1
			if recovered > domain.MaxAutoRecoveredActivationsPerIncident {
				next.State = domain.ActivationEscalated
				next.RecoveredCount = activation.RecoveredCount
				plan.Result.State = domain.ActivationEscalated
				break
			}
			next = newActivation(record, result.Epoch, activation.ConsumedEventCursor, recovered)
			next.IncidentID = activation.IncidentID
			next.State = domain.ActivationIdle
			next.LeaseToken, next.LeaseExpiresAt = "", nil
		}
	case domain.ActivationEventEventsArrived, domain.ActivationEventReconciliationAcknowledged:
		if result.State == domain.ActivationIdle {
			// The machine raised the epoch, so this is a new activation rather
			// than the old one at a new number. Minting the record here is what
			// keeps the derived identities and the epoch in agreement: carrying
			// the spent or revoked row forward would leave an activation whose
			// ID and dispatch identity name the previous epoch, and the next
			// dispatch would be planned against them.
			//
			// The recovery counter follows the incident. A reconciled revocation
			// is still the same incident being retried, so its count carries; a
			// spent activation ended its own turn and whatever arrives next
			// starts a fresh count, which the trigger path re-derives from the
			// incident it wakes for anyway.
			recovered := 0
			if signal.Event == domain.ActivationEventReconciliationAcknowledged {
				recovered = activation.RecoveredCount
			}
			next = newActivation(record, result.Epoch, activation.ConsumedEventCursor, recovered)
			next.IncidentID = activation.IncidentID
			next.State = domain.ActivationIdle
			next.LeaseToken, next.LeaseExpiresAt = "", nil
		}
	case domain.ActivationEventOperatorContinuation:
		granted := signal.GrantActivations
		if granted <= 0 {
			granted = 1
		}
		record.BudgetGrantedActivations = record.ActivationsUsed + granted
		next = newActivation(record, result.Epoch, activation.ConsumedEventCursor, 0)
		next.State = domain.ActivationIdle
		next.LeaseToken, next.LeaseExpiresAt = "", nil
		plan.Receipt = &ContinuationReceipt{
			RunID: record.RunID, RequestID: signal.RequestID, Actor: signal.Actor,
			Epoch: result.Epoch, GrantedActivations: granted, Reason: signal.Reason, IssuedAt: now,
		}
	}

	// The outcome and the consumed high-water mark are written together, and
	// only an activation that actually reviewed consumes anything. A revoked or
	// expired activation leaves its unacknowledged inbox exactly as it found it.
	switch next.State {
	case domain.ActivationSpent:
		next.Outcome = signal.Outcome
		if next.Outcome == domain.ActivationOutcomeNone {
			next.Outcome = domain.ActivationOutcomeNoDecision
			if ActivationExpired(activation, now) {
				next.Outcome = domain.ActivationOutcomeExpired
			}
		}
		closed := now
		next.ClosedAt = &closed
		next.LeaseToken, next.LeaseExpiresAt = "", nil
	case domain.ActivationRevoked:
		// Authority is gone immediately; the inbox is preserved for whoever
		// replaces this activation after reconciliation.
		next.Outcome = domain.ActivationOutcomeRevoked
		next.LeaseToken, next.LeaseExpiresAt = "", nil
	case domain.ActivationClosed:
		next.Outcome = domain.ActivationOutcomeClosed
		closed := now
		next.ClosedAt = &closed
		next.LeaseToken, next.LeaseExpiresAt = "", nil
	case domain.ActivationRecoveryRequired, domain.ActivationEscalated:
		next.LeaseToken, next.LeaseExpiresAt = "", nil
	}
	if next.Outcome == domain.ActivationOutcomeDecided || next.Outcome == domain.ActivationOutcomeNoDecision ||
		next.Outcome == domain.ActivationOutcomeDecidedByOperator || next.Outcome == domain.ActivationOutcomeExpired {
		if next.Purpose == "" {
			record.EventCursor = next.ConsumedEventCursor
		}
		plan.CursorAdvanced = true
		plan.AcknowledgedEventIDs = inbox.EventIDs()
		plan.AcknowledgementPurpose = next.Purpose
	}
	plan.ConsumedThrough = next.ConsumedEventCursor

	if next.State == domain.ActivationEscalated {
		plan.Escalated = true
		incident := signal.IncidentID
		if strings.TrimSpace(incident) == "" {
			// An exhausted budget is an incident of the run itself, and it has a
			// stable identity so repeated exhaustion notifies once.
			incident = stableCoordinatorID("supervision-incident", record.RunID+"\x00activation-budget")
		}
		reason := signal.Reason
		if strings.TrimSpace(reason) == "" {
			reason = fmt.Sprintf("supervision activation escalated after %s", signal.Event)
		}
		threadID, _ := SupervisionEscalationThread(record, state.SubmitterThreadID)
		if entry, ok := EscalationOutboxEntry(record, threadID, incident, reason, inbox.EventIDs(), now); ok {
			if _, added := AppendSupervisionOutbox(state.Outbox, entry); added {
				plan.Outbox = append(plan.Outbox, entry)
			}
		}
	}
	if plan.Dispatch != nil {
		if entry, ok := ActivationWakeOutboxEntry(next, signal.Reason, inbox.EventIDs(), now); ok {
			if _, added := AppendSupervisionOutbox(state.Outbox, entry); added {
				plan.Outbox = append(plan.Outbox, entry)
			}
		}
	}

	record.ActivationEpoch = next.Epoch
	record.UpdatedAt = now
	record.Revision++
	plan.Record = record
	plan.Activation = next
	return plan, nil
}

// newActivation builds the activation record for one epoch. Identity and
// dispatch identity are derived, never generated, so a coordinator that
// restarts mid-dispatch recomputes the same ones.
func newActivation(record domain.SupervisionRecord, epoch, consumed int64, recovered int) domain.Activation {
	return domain.Activation{
		ID:                  ActivationID(record.RunID, epoch),
		RunID:               record.RunID,
		Epoch:               epoch,
		DispatchIdentity:    ActivationDispatchIdentity(record.RunID, epoch),
		State:               domain.ActivationPendingDispatch,
		ConsumedEventCursor: consumed,
		RecoveredCount:      recovered,
		Outcome:             domain.ActivationOutcomeNone,
	}
}

// issueActivationLease issues or renews the coordinator-issued lease and
// describes the dispatch. The deadline is set once per activation: renewing a
// lease extends how long the coordinator believes in the overseer, never how
// long the overseer may take.
func issueActivationLease(activation *domain.Activation, record domain.SupervisionRecord, now time.Time, retry bool) *ActivationDispatch {
	expiry := now.Add(DefaultActivationLeaseTTL)
	if deadline := record.Config.ActivationDeadline; deadline > 0 && deadline < DefaultActivationLeaseTTL {
		expiry = now.Add(deadline)
	}
	activation.LeaseToken = stableCoordinatorID("supervision-lease",
		fmt.Sprintf("%s@%s", activation.DispatchIdentity, expiry.Format(time.RFC3339Nano)))
	activation.LeaseExpiresAt = &expiry
	if activation.Deadline == nil && record.Config.ActivationDeadline > 0 {
		deadline := now.Add(record.Config.ActivationDeadline)
		activation.Deadline = &deadline
	}
	dispatch := &ActivationDispatch{
		Identity:           activation.DispatchIdentity,
		Epoch:              activation.Epoch,
		Retry:              retry,
		LeaseToken:         activation.LeaseToken,
		LeaseExpiresAt:     expiry,
		RequiredCapability: SupervisionWorkerCapability,
	}
	if activation.Deadline != nil {
		dispatch.Deadline = activation.Deadline.UTC()
	}
	return dispatch
}

// AuthorizeActivationDecision is the guard every supervision decision passes
// before it is written.
//
// It is separate from the state machine because a decision arrives on the admin
// transport, not as a lifecycle event, and it must be refused for the same
// reasons the machine would refuse it: an expired lease, a stale epoch, a stale
// record revision, an activation that is not active, or an overseer acting for
// a different run.
func AuthorizeActivationDecision(state SupervisionActivationState, actor domain.Actor, expectedEpoch, expectedRevision int64, now time.Time) error {
	if err := domain.AuthorizeSupervisionActor(state.Record, state.Activation, actor, now); err != nil {
		return err
	}
	if actor.Kind == domain.ActorOverseer && expectedEpoch != state.Activation.Epoch {
		return fmt.Errorf("%w: decision names epoch %d, the activation is at %d",
			domain.ErrSupervisionStaleRevision, expectedEpoch, state.Activation.Epoch)
	}
	if expectedRevision != state.Record.Revision {
		return fmt.Errorf("%w: decision names record revision %d, the record is at %d",
			domain.ErrSupervisionStaleRevision, expectedRevision, state.Record.Revision)
	}
	return nil
}

// SupervisionActivationCommit is one atomic durable write of a plan. The
// consumed high-water mark, the outcome, the counters and the outbox entries
// land together or not at all, fenced on the record revision the plan read.
type SupervisionActivationCommit struct {
	RunID                  string
	ExpectedRecordRevision int64
	Record                 domain.SupervisionRecord
	Activation             domain.Activation
	ConsumedThrough        int64
	CursorAdvanced         bool
	AcknowledgedEventIDs   []string
	AcknowledgementPurpose domain.RecoveryActivationPurpose
	Outbox                 []SupervisionOutboxEntry
	Receipt                *ContinuationReceipt
	RequestID              string
	CommittedAt            time.Time
}

// SupervisionActivationStore is the durable surface this package needs. Lane B3
// owns the tables; Lane B7 binds this interface to them.
//
// It is deliberately narrow: one read that returns everything a decision needs,
// one revision-fenced write that commits a whole plan, and one append for
// observed triggers. Anything wider would let a caller write half a plan.
type SupervisionActivationStore interface {
	// LoadSupervisionActivationState reads one run's supervision record, its
	// current activation, its pending inbox and its live outbox. It reports
	// ErrSupervisionNotConfigured for an unsupervised run, because absence of a
	// record is the unsupervised case rather than an empty record.
	LoadSupervisionActivationState(ctx context.Context, runID string) (SupervisionActivationState, error)
	// CommitSupervisionActivation writes one plan atomically, refusing when the
	// record has moved since the plan read it.
	CommitSupervisionActivation(ctx context.Context, commit SupervisionActivationCommit) error
	// AppendSupervisionEvents appends observed triggers and assigns their
	// sequences, ignoring an event ID that is already present. It returns how
	// many rows it wrote, so at-least-once delivery is observable.
	AppendSupervisionEvents(ctx context.Context, runID string, events []SupervisionEvent) (int, error)
}

// ErrSupervisionNotConfigured reports a run with no supervision record. It is
// not an error condition of the run: it is the unsupervised case.
var ErrSupervisionNotConfigured = errors.New("run has no supervision record")

// SupervisionActivationService drives the activation lifecycle against a store.
type SupervisionActivationService struct {
	Store SupervisionActivationStore
	Now   func() time.Time
}

func (s SupervisionActivationService) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

// Observe records triggers that fired. It is safe to call with an event that
// was already recorded: the store ignores a known event ID, so a repeated
// observation adds nothing and wakes nobody twice.
func (s SupervisionActivationService) Observe(ctx context.Context, runID string, events ...SupervisionEvent) (int, error) {
	if s.Store == nil {
		return 0, errors.New("supervision activation service requires a store")
	}
	for _, event := range events {
		if err := event.Validate(); err != nil {
			return 0, err
		}
		if event.RunID != runID {
			return 0, fmt.Errorf("supervision event %q belongs to run %q, not %q", event.ID, event.RunID, runID)
		}
	}
	return s.Store.AppendSupervisionEvents(ctx, runID, events)
}

// Advance applies one signal to one run and commits its plan.
//
// It reads, plans and commits under the record revision it read. A concurrent
// change makes the commit fail rather than overwrite, which is what keeps a
// restart, a reconnect and a duplicate delivery from producing two competing
// overseers or repeating a release.
func (s SupervisionActivationService) Advance(ctx context.Context, runID string, signal ActivationSignal) (ActivationPlan, error) {
	if s.Store == nil {
		return ActivationPlan{}, errors.New("supervision activation service requires a store")
	}
	state, err := s.Store.LoadSupervisionActivationState(ctx, runID)
	if err != nil {
		return ActivationPlan{}, err
	}
	now := s.now()
	plan, err := PlanActivation(state, signal, now)
	if err != nil {
		return ActivationPlan{}, err
	}
	commit := SupervisionActivationCommit{
		RunID:                  runID,
		ExpectedRecordRevision: state.Record.Revision,
		Record:                 plan.Record,
		Activation:             plan.Activation,
		ConsumedThrough:        plan.ConsumedThrough,
		CursorAdvanced:         plan.CursorAdvanced,
		AcknowledgedEventIDs:   plan.AcknowledgedEventIDs,
		AcknowledgementPurpose: plan.AcknowledgementPurpose,
		Outbox:                 plan.Outbox,
		Receipt:                plan.Receipt,
		RequestID:              signal.RequestID,
		CommittedAt:            now,
	}
	if err := s.Store.CommitSupervisionActivation(ctx, commit); err != nil {
		return ActivationPlan{}, err
	}
	return plan, nil
}
