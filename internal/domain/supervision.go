// Campaign supervision domain types, state machines and the shared readiness
// predicate.
//
// # Frozen interface for the other supervision lanes
//
// Everything below is published by Lane B1 and frozen. Other lanes read this
// block instead of guessing names.
//
// Durable records (one Go type per durable table of the seam review, section
// 6.2):
//
//	SupervisionRecord  the run's supervision row: config, event cursor,
//	                   activation epoch, budget counters, revision.
//	SupervisionConfig  route, prompt artifact, max activations, max turns per
//	                   activation, activation deadline, idle escalation after,
//	                   escalation destination.
//	GateDefinition     gate id and name, observed task IDs, protected task IDs,
//	                   rubric artifact, Final (the final-settlement gate form,
//	                   which protects no downstream task).
//	Gate               a gate definition plus its state, graph revision and
//	                   evidence snapshot identity.
//	GateDecision       an append-only decision record: actor, activation epoch,
//	                   graph revision, evidence snapshot, outcome, request key,
//	                   reason, incident.
//	EvidenceSnapshot   the bound evidence: graph revision plus, per observed
//	                   task, the exact attempt ID, result revision, artifact
//	                   digests and code commit identities.
//	Hold               id, scope (run or branch root), resolved task set and
//	                   graph revision at creation, owner Actor, reason, state.
//	Activation         epoch, state, lease token and expiry, deadline, budget
//	                   counters, dispatch identity, consumed event high-water
//	                   mark, outcome.
//	ReviewIncident     id, source event and task-attempt identity, revision,
//	                   required disposition, state, resolution receipt.
//	Actor              actor kind (overseer or operator), principal, activation
//	                   epoch. Authority is a property of the actor kind.
//
// Pure transition functions, one per state machine of the seam review, section
// 3. Each returns a typed error a caller can distinguish with errors.Is:
//
//	GateTransition(GateTransitionInput) (GateState, error)
//	HoldTransition(HoldTransitionInput) (HoldState, error)
//	ActivationTransition(ActivationTransitionInput) (ActivationTransitionResult, error)
//	IncidentTransition(IncidentTransitionInput) (IncidentState, error)
//
// The guard errors are ErrSupervisionStaleRevision, ErrSupervisionPrerequisite,
// ErrSupervisionUnauthorizedActor, ErrSupervisionBudgetExhausted,
// ErrSupervisionTerminal and ErrSupervisionIllegalTransition.
//
// Readiness, status and invalidation (see supervision_readiness.go):
//
//	SupervisionAdmits(SupervisionSnapshot, SupervisionQuery) SupervisionVerdict
//	SupervisedRunStatusLabel(SupervisedRunStatusInput) SupervisedRunStatus
//	GateAcceptanceStands(GateDefinition, GateDecision, GateChange) AcceptanceVerdict
//
// SupervisionSnapshot is the compact, I/O-free state the predicate consumes:
// run ID, graph revision, Supervised, RunTerminal, RunCancelled,
// RouteAvailable, Gates and Holds. SupervisionQuery is the task being asked
// about: TaskID and an optional ExpectedGraphRevision fence. SupervisionVerdict
// is Admitted plus ordered SupervisionBlocker values carrying a stable
// SupervisionBlockerCode, which is one of:
//
//	supervision-gate-pending, supervision-gate-awaiting-review,
//	supervision-gate-held, supervision-gate-escalated,
//	supervision-gate-cancelled, supervision-branch-hold, supervision-run-hold,
//	supervision-route-unavailable, supervision-run-cancelled,
//	supervision-run-terminal, supervision-stale-snapshot.
//
// Two things this package deliberately does not add. A held task is not a
// ProgressState: it is a derived label produced by SupervisedRunStatusLabel, so
// Terminal, RunExecutionsQuiescent, the planner and the sink projection need to
// learn nothing. And supervision is not a PlanningConstraint: it is a property
// of the task rather than of a candidate route, so the planner calls
// SupervisionAdmits inline beside its dependency blockers.
package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Guard errors. Every refusal from a transition function wraps exactly one of
// these, so a caller can tell a race (stale revision) from a missing
// precondition, from an actor acting outside its kind, from an exhausted
// budget, from a record that has already settled.
var (
	// ErrSupervisionStaleRevision reports a decision that lost a race: the
	// graph revision, activation epoch or record revision it names is not the
	// one the record now has.
	ErrSupervisionStaleRevision = errors.New("supervision decision names a stale revision")
	// ErrSupervisionPrerequisite reports an unmet precondition: evidence that
	// is not yet in coordinator custody, a branch root that does not exist, a
	// closure that was not recomputed, a reconsideration without a fresh
	// snapshot.
	ErrSupervisionPrerequisite = errors.New("supervision transition has an unmet prerequisite")
	// ErrSupervisionUnauthorizedActor reports an actor kind that may not make
	// this transition. An overseer cannot release an operator's hold, cannot
	// resolve an escalated incident and cannot authorize its own continuation.
	ErrSupervisionUnauthorizedActor = errors.New("supervision transition is not permitted to this actor kind")
	// ErrSupervisionBudgetExhausted reports an activation or turn budget that
	// has no headroom left. It is never silently reset; an operator-authorized
	// continuation writes a fresh budget with a new receipt.
	ErrSupervisionBudgetExhausted = errors.New("supervision budget is exhausted")
	// ErrSupervisionTerminal reports a mutation attempted after the record or
	// the run settled. After terminal sink settlement every mutating
	// supervision capability is revoked.
	ErrSupervisionTerminal = errors.New("supervision record is terminal")
	// ErrSupervisionIllegalTransition reports an event the state machine has no
	// row for. It is distinct from a guard failure: the guard was never
	// reached.
	ErrSupervisionIllegalTransition = errors.New("supervision state machine has no such transition")
)

// Bounds on declared supervision configuration. They exist so a manifest
// cannot declare a budget large enough to be indistinguishable from none.
const (
	MaxSupervisionActivations        = 100
	MaxSupervisionTurnsPerActivation = 20
	MaxSupervisionActivationDeadline = 24 * time.Hour
	MaxSupervisionIdleEscalation     = 30 * 24 * time.Hour
	// MaxAutoRecoveredActivationsPerIncident bounds automatic recovery, per
	// the plan: further failures escalate rather than respawn.
	MaxAutoRecoveredActivationsPerIncident = 2
)

// ActorKind separates the two authorities that can act on supervision. It is
// not a role name and not a principal: it is the distinction every guard in
// section 3 turns on.
type ActorKind string

const (
	ActorOverseer ActorKind = "overseer"
	ActorOperator ActorKind = "operator"
)

// Actor is the authenticated identity recorded on every decision. An overseer
// actor is scoped to one run and one activation epoch; an operator actor is a
// human principal authenticated by the admin transport.
type Actor struct {
	Kind            ActorKind `json:"kind"`
	Principal       string    `json:"principal"`
	ActivationEpoch int64     `json:"activationEpoch,omitempty"`
}

// SameIdentity reports whether two actors are the same owner. An overseer is
// the same owner only at the same epoch: a replacement overseer does not
// inherit the previous epoch's holds.
func (a Actor) SameIdentity(other Actor) bool {
	if a.Kind != other.Kind || a.Principal != other.Principal {
		return false
	}
	if a.Kind == ActorOverseer {
		return a.ActivationEpoch == other.ActivationEpoch
	}
	return true
}

// Validate checks that an actor is one of the two known kinds and names a
// principal.
func (a Actor) Validate() error {
	switch a.Kind {
	case ActorOverseer, ActorOperator:
	default:
		return fmt.Errorf("%w: unknown actor kind %q", ErrSupervisionUnauthorizedActor, a.Kind)
	}
	if strings.TrimSpace(a.Principal) == "" {
		return fmt.Errorf("%w: actor has no principal", ErrSupervisionUnauthorizedActor)
	}
	return nil
}

// SupervisionEscalation is where an escalation is delivered. It reuses the
// existing notify-thread configuration and adds no messaging integration and no
// recipient discovery.
type SupervisionEscalation struct {
	NotifyThread bool   `json:"notifyThread,omitempty"`
	ThreadID     string `json:"threadId,omitempty"`
}

// SupervisionEscalationDelivery is one escalation waiting to reach the
// configured notify thread.
//
// It is declared here, in the shared vocabulary, because the store that records
// escalations and the wait runner that delivers them cannot name each other's
// types. It carries no body: an escalation says which incident of which run
// needs a human and why, and the operator reads the rest with
// "campaign supervision show".
type SupervisionEscalationDelivery struct {
	// ID is the outbox entry, and DeliveryID is what a delivered message is
	// recognised by when a send is interrupted and has to be reconciled.
	ID         string `json:"id"`
	DeliveryID string `json:"deliveryId"`
	RunID      string `json:"runId"`
	IncidentID string `json:"incidentId"`
	ThreadID   string `json:"threadId"`
	Reason     string `json:"reason,omitempty"`
	// Delivery is the outbox delivery state as the store holds it, which is the
	// value a compare-and-set claim must name.
	Delivery string `json:"delivery"`
	Attempts int    `json:"attempts,omitempty"`
}

// SupervisionConfig is the declared supervision of one workflow, carried
// unchanged from the manifest into the run. A clone or rerun inherits this
// record and inherits no acceptances, no holds and no incidents.
type SupervisionConfig struct {
	// Route is exactly one provider route, independently configured from the
	// task routes. "Independently configured" is the requirement; a stronger
	// model is an operator's choice, not a scheduling privilege.
	Route ProviderRoute `json:"route"`
	// PromptArtifactID is the retained overseer prompt.
	PromptArtifactID string `json:"promptArtifactId"`
	// MaxActivations bounds the whole run; MaxTurnsPerActivation bounds one
	// activation. Both are durable, so a restart cannot spin.
	MaxActivations        int                   `json:"maxActivations"`
	MaxTurnsPerActivation int                   `json:"maxTurnsPerActivation"`
	ActivationDeadline    time.Duration         `json:"activationDeadline"`
	IdleEscalationAfter   time.Duration         `json:"idleEscalationAfter,omitempty"`
	Escalation            SupervisionEscalation `json:"escalation,omitempty"`
}

// Validate checks a declared supervision configuration.
func (c SupervisionConfig) Validate() error {
	switch {
	case strings.TrimSpace(c.Route.ProviderInstanceID) == "" || strings.TrimSpace(c.Route.Model) == "":
		return errors.New("supervision needs one route with a provider instance and a model")
	case strings.TrimSpace(c.PromptArtifactID) == "":
		return errors.New("supervision needs an overseer prompt artifact")
	case c.MaxActivations <= 0 || c.MaxActivations > MaxSupervisionActivations:
		return fmt.Errorf("supervision needs max activations above zero and at most %d", MaxSupervisionActivations)
	case c.MaxTurnsPerActivation <= 0 || c.MaxTurnsPerActivation > MaxSupervisionTurnsPerActivation:
		return fmt.Errorf("supervision needs max turns per activation above zero and at most %d", MaxSupervisionTurnsPerActivation)
	case c.ActivationDeadline <= 0 || c.ActivationDeadline > MaxSupervisionActivationDeadline:
		return fmt.Errorf("supervision needs an activation deadline above zero and at most %s", MaxSupervisionActivationDeadline)
	case c.IdleEscalationAfter < 0 || c.IdleEscalationAfter > MaxSupervisionIdleEscalation:
		return fmt.Errorf("supervision needs an idle escalation of at most %s", MaxSupervisionIdleEscalation)
	}
	return nil
}

// SupervisionRecord is the durable coordinator-owned supervision of one run. It
// lives beside the sink, outside the worker DAG, so the overseer can observe a
// failure without waiting behind its own review gate.
//
// Absence of this record is the unsupervised case, which is exactly today's
// behaviour. No empty record is ever created.
type SupervisionRecord struct {
	RunID  string            `json:"runId"`
	Config SupervisionConfig `json:"config"`
	// EventCursor is the high-water mark of supervision events an activation
	// has consumed. Events arriving during a review stay pending behind it.
	EventCursor int64 `json:"eventCursor"`
	// ActivationEpoch is the current epoch. A replacement overseer gets
	// epoch+1 and decisions from earlier epochs are fenced out.
	ActivationEpoch int64 `json:"activationEpoch"`
	// ActivationsUsed counts activations that ever started. An activation that
	// started and then vanished counts; a provably undelivered dispatch does
	// not.
	ActivationsUsed int `json:"activationsUsed"`
	// BudgetGrantedActivations is the current budget. An operator-authorized
	// continuation raises it with a receipt rather than resetting the counter.
	BudgetGrantedActivations int `json:"budgetGrantedActivations"`
	// Revision fences supervision mutations against offers and claims in the
	// same coordinator transaction.
	Revision  int64     `json:"revision"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// ActivationBudgetRemaining reports whether another activation may start.
func (r SupervisionRecord) ActivationBudgetRemaining() bool {
	budget := r.BudgetGrantedActivations
	if budget <= 0 {
		budget = r.Config.MaxActivations
	}
	return r.ActivationsUsed < budget
}

// ArtifactDigest binds one reviewed output to its exact content.
type ArtifactDigest struct {
	ArtifactID string `json:"artifactId"`
	Digest     string `json:"digest"`
}

// CommitIdentity binds one reviewed Git commit. A commit output is evidence
// only when both the repository and the commit are named.
type CommitIdentity struct {
	Repository string `json:"repository,omitempty"`
	Commit     string `json:"commit"`
}

// ProducerEvidence is what one observed task contributed to a review. The
// attempt ID is the exact attempt, not the task's current one: that is what
// makes a retry detectable as invalidation rather than as an update.
type ProducerEvidence struct {
	TaskID           string           `json:"taskId"`
	AttemptID        string           `json:"attemptId"`
	ResultRevision   int64            `json:"resultRevision"`
	ArtifactDigests  []ArtifactDigest `json:"artifactDigests,omitempty"`
	CommitIdentities []CommitIdentity `json:"commitIdentities,omitempty"`
}

// EvidenceSnapshot is the immutable identity of what was reviewed. A decision
// binds it, and readiness later compares against it, which is why it carries
// its own ID as well as its contents.
type EvidenceSnapshot struct {
	ID            string             `json:"id"`
	GraphRevision int64              `json:"graphRevision"`
	Producers     []ProducerEvidence `json:"producers,omitempty"`
	TakenAt       time.Time          `json:"takenAt"`
}

// GateState is the state of one gate. A gate is closed before any producer
// runs, so the zero-value initial state is pending-evidence.
type GateState string

const (
	GatePendingEvidence GateState = "pending-evidence"
	GateReadyForReview  GateState = "ready-for-review"
	GateAccepted        GateState = "accepted"
	GateHeld            GateState = "held"
	GateEscalated       GateState = "escalated"
	GateCancelled       GateState = "cancelled"
)

// GateDefinition is the declared gate, part of the effective graph rather than
// of any task definition. Gate membership is graph-level: putting it on a task
// would make every gate change a task-definition amendment.
type GateDefinition struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	// ObservedTaskIDs are the producers whose results are reviewed. Never
	// empty: a gate with nothing to observe is a permanent hold.
	ObservedTaskIDs []string `json:"observedTaskIds"`
	// ProtectedTaskIDs are the tasks this gate holds. Empty exactly when Final
	// is true.
	ProtectedTaskIDs []string `json:"protectedTaskIds,omitempty"`
	RubricArtifactID string   `json:"rubricArtifactId,omitempty"`
	// Final marks the final-settlement gate: it guards run settlement instead
	// of a downstream task, modelled explicitly rather than as a dummy agent
	// task.
	Final bool `json:"final,omitempty"`
}

// Validate checks a gate definition's shape. Name resolution against the task
// set, cycles and reachability belong to the manifest validator.
func (d GateDefinition) Validate() error {
	switch {
	case strings.TrimSpace(d.ID) == "" || strings.TrimSpace(d.Name) == "":
		return errors.New("gate needs an id and a name")
	case len(d.ObservedTaskIDs) == 0:
		return errors.New("gate needs at least one observed task")
	case d.Final && len(d.ProtectedTaskIDs) > 0:
		return errors.New("a final-settlement gate protects no downstream task")
	case !d.Final && len(d.ProtectedTaskIDs) == 0:
		return errors.New("gate protects no task and is not marked final")
	}
	for _, observed := range d.ObservedTaskIDs {
		for _, protected := range d.ProtectedTaskIDs {
			if observed == protected {
				return fmt.Errorf("gate observes and protects the same task %q", observed)
			}
		}
	}
	return nil
}

// Observes reports whether the gate reviews this task's results.
func (d GateDefinition) Observes(taskID string) bool {
	return containsID(d.ObservedTaskIDs, taskID)
}

// Protects reports whether the gate holds this task's dispatch.
func (d GateDefinition) Protects(taskID string) bool {
	return containsID(d.ProtectedTaskIDs, taskID)
}

// Gate is one declared gate and its current state in one run.
type Gate struct {
	Definition GateDefinition `json:"definition"`
	RunID      string         `json:"runId"`
	State      GateState      `json:"state"`
	// GraphRevision is the revision the current state was computed against.
	GraphRevision int64 `json:"graphRevision"`
	// EvidenceSnapshotID identifies the snapshot the current state refers to:
	// what is under review, or what was accepted.
	EvidenceSnapshotID string    `json:"evidenceSnapshotId,omitempty"`
	Revision           int64     `json:"revision"`
	UpdatedAt          time.Time `json:"updatedAt"`
}

// GateDecisionOutcome is what a decision did. A supervisor process exiting
// successfully is not one of these; only a structured decision is.
type GateDecisionOutcome string

const (
	GateDecisionAccept GateDecisionOutcome = "accept"
	GateDecisionReject GateDecisionOutcome = "reject"
)

// GateDecision is one append-only decision record. History survives
// reconsideration: a later decision adds a row, it never rewrites this one.
type GateDecision struct {
	ID     string `json:"id"`
	RunID  string `json:"runId"`
	GateID string `json:"gateId"`
	Actor  Actor  `json:"actor"`
	// ActivationEpoch is the epoch the decision was made under. A decision
	// from a revoked epoch is fenced out even if it arrives later.
	ActivationEpoch int64 `json:"activationEpoch"`
	// GraphRevision and Evidence are the binding: graph revision, exact
	// upstream attempt IDs, result revisions, artifact digests and commit
	// identities.
	GraphRevision int64               `json:"graphRevision"`
	Evidence      EvidenceSnapshot    `json:"evidence"`
	Outcome       GateDecisionOutcome `json:"outcome"`
	// RequestID is the idempotency key. The same key with the same payload
	// replays the answer; the same key with a different payload is refused.
	RequestID string `json:"requestId"`
	Reason    string `json:"reason"`
	// IncidentID is the review incident this decision closes, when it closes
	// one. Ordinary gate acceptance closes only its matching incident.
	IncidentID string    `json:"incidentId,omitempty"`
	DecidedAt  time.Time `json:"decidedAt"`
}

// HoldScopeKind distinguishes a run-wide hold from a branch hold.
type HoldScopeKind string

const (
	HoldScopeRun    HoldScopeKind = "run"
	HoldScopeBranch HoldScopeKind = "branch"
)

// HoldScope is what a hold covers. A branch means an explicit root task and its
// dependency closure at the recorded graph revision.
type HoldScope struct {
	Kind             HoldScopeKind `json:"kind"`
	BranchRootTaskID string        `json:"branchRootTaskId,omitempty"`
}

// HoldState is the state of one hold. Holds have exactly two states: a released
// hold is terminal, and releasing one hold never releases another.
type HoldState string

const (
	HoldActive   HoldState = "active"
	HoldReleased HoldState = "released"
)

// Hold is one dispatch hold owned by one actor. Different actors' holds retain
// separate identities and ownership: an overseer cannot clear an operator hold.
//
// A hold prevents a start that has not been authorized yet. It does not
// interrupt, roll back or reach into an attempt that already passed
// authorization; such an attempt is reported, not stopped.
type Hold struct {
	ID    string    `json:"id"`
	RunID string    `json:"runId"`
	Scope HoldScope `json:"scope"`
	Owner Actor     `json:"owner"`
	// GraphRevision and ResolvedTaskIDs are the closure as computed when the
	// hold was placed, recorded so an amendment can be seen to change it.
	GraphRevision   int64      `json:"graphRevision"`
	ResolvedTaskIDs []string   `json:"resolvedTaskIds,omitempty"`
	State           HoldState  `json:"state"`
	Reason          string     `json:"reason"`
	CreatedAt       time.Time  `json:"createdAt"`
	ReleasedAt      *time.Time `json:"releasedAt,omitempty"`
}

// Covers reports whether this hold, while active, covers a task. A run hold
// covers every worker task in the run; a branch hold covers its resolved set.
func (h Hold) Covers(taskID string) bool {
	if h.State != HoldActive {
		return false
	}
	if h.Scope.Kind == HoldScopeRun {
		return true
	}
	return containsID(h.ResolvedTaskIDs, taskID)
}

// ActivationState is the state of one supervision activation. At most one
// activation per run is valid at a time.
type ActivationState string

const (
	ActivationIdle             ActivationState = "idle"
	ActivationPendingDispatch  ActivationState = "pending-dispatch"
	ActivationActive           ActivationState = "active"
	ActivationSpent            ActivationState = "spent"
	ActivationRevoked          ActivationState = "revoked"
	ActivationRecoveryRequired ActivationState = "recovery-required"
	ActivationEscalated        ActivationState = "escalated"
	ActivationClosed           ActivationState = "closed"
)

// ActivationOutcome is how an activation ended, recorded atomically with its
// consumed high-water mark.
type ActivationOutcome string

const (
	ActivationOutcomeNone       ActivationOutcome = ""
	ActivationOutcomeDecided    ActivationOutcome = "decided"
	ActivationOutcomeNoDecision ActivationOutcome = "no-decision"
	ActivationOutcomeExpired    ActivationOutcome = "expired"
	ActivationOutcomeRevoked    ActivationOutcome = "revoked"
	ActivationOutcomeClosed     ActivationOutcome = "closed"
	// ActivationOutcomeDecidedByOperator records that the gate this activation
	// was woken for was decided while it was live, but by an operator rather
	// than by the overseer itself.
	//
	// It exists because the two honest statements were indistinguishable. An
	// activation whose gate an operator decided had recorded no decision of its
	// own, so it was written down as no-decision, which reads as a review that
	// produced nothing; and calling it decided would credit the overseer with a
	// decision it did not make. This says what happened instead.
	ActivationOutcomeDecidedByOperator ActivationOutcome = "decided-by-operator"
)

// Activation is one bounded, schedulable supervision unit. Correctness comes
// from this record, not from conversation history: a replacement thread is
// started from a compact snapshot at a new epoch.
type Activation struct {
	ID    string `json:"id"`
	RunID string `json:"runId"`
	Epoch int64  `json:"epoch"`
	// DispatchIdentity is deterministic. A provably undelivered dispatch is
	// retried with this same identity rather than a new one.
	DispatchIdentity string `json:"dispatchIdentity"`
	// ReadyAt and ReadyTieID persist the selected trigger ordering for this epoch.
	// Undelivered retries reuse them, so restart never changes fairness age.
	ReadyAt    time.Time `json:"readyAt,omitempty"`
	ReadyTieID string    `json:"readyTieId,omitempty"`
	// Principal is the admin principal this activation's supervisor capability
	// authenticates as, recorded when the activation is dispatched. The
	// authorizer reads the run and epoch a principal may act on from here, so
	// the scope is the coordinator's own conclusion rather than a property of
	// the credential.
	Principal string          `json:"principal,omitempty"`
	State     ActivationState `json:"state"`
	// LeaseToken and LeaseExpiresAt carry the coordinator-issued renewable
	// lease. Expiry revokes decision authority immediately.
	LeaseToken     string     `json:"leaseToken,omitempty"`
	LeaseExpiresAt *time.Time `json:"leaseExpiresAt,omitempty"`
	// Deadline is the maximum elapsed time for this activation.
	Deadline *time.Time `json:"deadline,omitempty"`
	// IncidentID is the review incident this activation was woken for, when it
	// was woken for one. It is what makes
	// MaxAutoRecoveredActivationsPerIncident enforceable rather than merely
	// declared: the recovery counter belongs to one incident, so a fresh
	// incident starts a fresh recovery budget and a repeatedly failing one does
	// not respawn forever.
	IncidentID string `json:"incidentId,omitempty"`
	// TurnsUsed counts turns against MaxTurnsPerActivation; RecoveredCount
	// counts automatic recoveries of this incident against
	// MaxAutoRecoveredActivationsPerIncident.
	TurnsUsed      int `json:"turnsUsed"`
	RecoveredCount int `json:"recoveredCount"`
	// OperatorDecisions counts the gate decisions recorded at this activation's
	// epoch by an operator rather than by the overseer.
	//
	// An operator keeps full authority over a run it supervises, so such a
	// decision is accepted rather than fenced out. Recording it here is what
	// keeps the activation's own receipt honest: the overseer did not decide,
	// and the review it was woken for is nonetheless settled.
	OperatorDecisions int `json:"operatorDecisions,omitempty"`
	// ConsumedEventCursor is the high-water mark this activation consumed,
	// recorded atomically with Outcome. Events arriving meanwhile stay pending.
	ConsumedEventCursor int64             `json:"consumedEventCursor"`
	Outcome             ActivationOutcome `json:"outcome,omitempty"`
	StartedAt           *time.Time        `json:"startedAt,omitempty"`
	ClosedAt            *time.Time        `json:"closedAt,omitempty"`
}

// IncidentState is the state of one review incident. Escalation stays
// unresolved until operator action.
type IncidentState string

const (
	IncidentOpen      IncidentState = "open"
	IncidentEscalated IncidentState = "escalated"
	IncidentResolved  IncidentState = "resolved"
)

// IncidentDisposition is what must happen before an incident can close.
type IncidentDisposition string

const (
	// DispositionGateDecision closes when the matching gate is decided.
	DispositionGateDecision IncidentDisposition = "gate-decision"
	// DispositionConcludeFailure closes when an authorized actor acknowledges
	// an observed terminal task failure for settlement. It changes no task and
	// cancels no unrelated live work.
	DispositionConcludeFailure IncidentDisposition = "conclude-failure"
	// DispositionOperatorAction closes only on operator action after
	// documented remediation.
	DispositionOperatorAction IncidentDisposition = "operator-action"
)

// IncidentOutcome is the recorded result of a resolution.
type IncidentOutcome string

const (
	IncidentOutcomeGateAccepted    IncidentOutcome = "gate-accepted"
	IncidentOutcomeConcludeFailure IncidentOutcome = "conclude-failure"
	IncidentOutcomeRemediated      IncidentOutcome = "remediated"
	IncidentOutcomeCancelled       IncidentOutcome = "cancelled"
)

// ResolutionReceipt binds evidence, actor and outcome to one resolution.
type ResolutionReceipt struct {
	Actor              Actor           `json:"actor"`
	Outcome            IncidentOutcome `json:"outcome"`
	EvidenceSnapshotID string          `json:"evidenceSnapshotId,omitempty"`
	ExpectedRevision   int64           `json:"expectedRevision"`
	RequestID          string          `json:"requestId"`
	Reason             string          `json:"reason"`
	ResolvedAt         time.Time       `json:"resolvedAt"`
}

// ReviewIncident is one decision-requiring condition with a stable identity.
// Decisions name incident IDs explicitly; closing one incident never dismisses
// a newer one.
type ReviewIncident struct {
	ID    string `json:"id"`
	RunID string `json:"runId"`
	// SourceEventID, SourceTaskID and SourceAttemptID are the observation that
	// raised the incident.
	SourceEventID   string `json:"sourceEventId"`
	SourceTaskID    string `json:"sourceTaskId,omitempty"`
	SourceAttemptID string `json:"sourceAttemptId,omitempty"`
	// GateID is set when the incident is a gate review.
	GateID string `json:"gateId,omitempty"`
	// Revision fences a resolution against a later observation.
	Revision            int64               `json:"revision"`
	RequiredDisposition IncidentDisposition `json:"requiredDisposition"`
	State               IncidentState       `json:"state"`
	// Reason is the normalized block reason. Only reason transitions and
	// threshold crossings raise an incident, so ordinary brief quota waiting
	// never does.
	Reason     string             `json:"reason"`
	Resolution *ResolutionReceipt `json:"resolution,omitempty"`
	OpenedAt   time.Time          `json:"openedAt"`
}

// containsID is a small shared membership test. The sets involved are task and
// gate ID lists of a single run, which are small enough that a map would cost
// more than it saves.
func containsID(ids []string, id string) bool {
	for _, candidate := range ids {
		if candidate == id {
			return true
		}
	}
	return false
}

// GateEvent names one row of the gate transition table.
type GateEvent string

const (
	GateEventProducersSucceeded        GateEvent = "producers-succeeded"
	GateEventProducerNotSucceeded      GateEvent = "producer-not-succeeded"
	GateEventAccept                    GateEvent = "accept"
	GateEventReject                    GateEvent = "reject"
	GateEventReviewDeadlineExpired     GateEvent = "review-deadline-expired"
	GateEventProducerReplaced          GateEvent = "producer-replaced"
	GateEventActivationBudgetExhausted GateEvent = "activation-budget-exhausted"
	GateEventAmendmentTouchesScope     GateEvent = "amendment-touches-scope"
	GateEventAmendmentOutsideScope     GateEvent = "amendment-outside-scope"
	GateEventReconsider                GateEvent = "reconsider"
	GateEventNewProducerEvidence       GateEvent = "new-producer-evidence"
	GateEventRunCancelled              GateEvent = "run-cancelled"
)

// GateTransitionInput carries the gate, the event and every guard the seam
// review marks as evaluated inside the committing transaction. The guards are
// plain values so the function stays pure: the caller reads them in its own
// transaction and passes what it read.
type GateTransitionInput struct {
	Gate  Gate
	Event GateEvent
	Actor Actor
	// ExpectedGraphRevision is the revision the decision names.
	ExpectedGraphRevision int64
	// EvidenceMatches reports that the decision's evidence snapshot still
	// matches the upstream attempt IDs, result revisions, artifact digests and
	// commit identities.
	EvidenceMatches bool
	// ProducersVerified reports that every observed task succeeded, its
	// outputs verified and in coordinator custody.
	ProducersVerified bool
	// SuccessorOfferedOrStarted reports that a protected task has already been
	// offered or started, which makes invalidation a refusal rather than a
	// transition.
	SuccessorOfferedOrStarted bool
	// FreshSnapshot reports that a reconsideration took a fresh evidence
	// snapshot.
	FreshSnapshot bool
	// RevalidationReceiptID is the audit receipt that lets an acceptance be
	// carried forward across an amendment outside the gate's scope.
	RevalidationReceiptID string
	// SinkSettled reports that the run has settled terminally, after which
	// every mutating supervision capability is revoked.
	SinkSettled bool
}

// GateTransition is the pure gate state machine of the seam review, section
// 3.1. It returns the next state, or a guard error wrapping one of the
// supervision sentinels.
func GateTransition(in GateTransitionInput) (GateState, error) {
	if in.SinkSettled {
		return "", fmt.Errorf("%w: the run settled; supervision capabilities are revoked", ErrSupervisionTerminal)
	}
	state := in.Gate.State
	if state == "" {
		state = GatePendingEvidence
	}
	if state == GateCancelled {
		return "", fmt.Errorf("%w: the gate is cancelled", ErrSupervisionTerminal)
	}
	if in.Event == GateEventRunCancelled {
		return GateCancelled, nil
	}
	switch state {
	case GatePendingEvidence:
		switch in.Event {
		case GateEventProducersSucceeded:
			if !in.ProducersVerified {
				return "", fmt.Errorf("%w: observed results are not verified and in coordinator custody", ErrSupervisionPrerequisite)
			}
			return GateReadyForReview, nil
		case GateEventProducerNotSucceeded:
			// A failed, cancelled or skipped producer cannot be approved
			// through a gate. The gate stays closed and the protected task is
			// unreachable by ordinary DAG rules.
			return GatePendingEvidence, nil
		case GateEventAccept, GateEventReject:
			return "", fmt.Errorf("%w: the gate has no evidence to review", ErrSupervisionPrerequisite)
		}
	case GateReadyForReview:
		switch in.Event {
		case GateEventAccept, GateEventReject:
			if err := in.Actor.Validate(); err != nil {
				return "", err
			}
			if in.ExpectedGraphRevision != in.Gate.GraphRevision {
				return "", fmt.Errorf("%w: decision names graph revision %d, the gate is at %d",
					ErrSupervisionStaleRevision, in.ExpectedGraphRevision, in.Gate.GraphRevision)
			}
			if !in.EvidenceMatches {
				return "", fmt.Errorf("%w: the decision's evidence snapshot no longer matches", ErrSupervisionStaleRevision)
			}
			if in.Event == GateEventAccept {
				return GateAccepted, nil
			}
			return GateHeld, nil
		case GateEventReviewDeadlineExpired, GateEventActivationBudgetExhausted:
			return GateEscalated, nil
		case GateEventProducerReplaced:
			return GatePendingEvidence, nil
		}
	case GateAccepted:
		switch in.Event {
		case GateEventProducerReplaced:
			if in.SuccessorOfferedOrStarted {
				return "", fmt.Errorf("%w: a protected task is already offered or started; use explicit recovery or a new run",
					ErrSupervisionPrerequisite)
			}
			return GatePendingEvidence, nil
		case GateEventAmendmentTouchesScope:
			// Conservative invalidation is the default: a false invalidation
			// costs one review, a false carry-forward is an unreviewed
			// dispatch.
			return GatePendingEvidence, nil
		case GateEventAmendmentOutsideScope:
			if strings.TrimSpace(in.RevalidationReceiptID) == "" {
				return "", fmt.Errorf("%w: carrying an acceptance forward needs an atomic revalidation receipt",
					ErrSupervisionPrerequisite)
			}
			return GateAccepted, nil
		}
	case GateHeld:
		switch in.Event {
		case GateEventReconsider:
			if in.Actor.Kind != ActorOperator {
				return "", fmt.Errorf("%w: reconsideration of a held gate is operator-authorized", ErrSupervisionUnauthorizedActor)
			}
			if !in.FreshSnapshot {
				return "", fmt.Errorf("%w: reconsideration needs a fresh evidence snapshot", ErrSupervisionPrerequisite)
			}
			return GateReadyForReview, nil
		case GateEventNewProducerEvidence:
			if !in.ProducersVerified {
				return "", fmt.Errorf("%w: new producer evidence is not verified", ErrSupervisionPrerequisite)
			}
			return GatePendingEvidence, nil
		case GateEventAccept, GateEventReject:
			// No self-wake loop: an overseer cannot re-decide the same
			// rejected evidence.
			return "", fmt.Errorf("%w: a held gate needs fresh evidence or operator-authorized reconsideration",
				ErrSupervisionPrerequisite)
		}
	case GateEscalated:
		switch in.Event {
		case GateEventReconsider:
			if in.Actor.Kind != ActorOperator {
				return "", fmt.Errorf("%w: reconsideration of an escalated gate is operator-authorized", ErrSupervisionUnauthorizedActor)
			}
			if !in.FreshSnapshot {
				return "", fmt.Errorf("%w: reconsideration needs a fresh evidence snapshot", ErrSupervisionPrerequisite)
			}
			return GateReadyForReview, nil
		case GateEventAccept:
			// Acceptance is not reachable from escalated. The run settles
			// through incident resolution instead.
			return "", fmt.Errorf("%w: an escalated gate settles through incident resolution, not acceptance",
				ErrSupervisionPrerequisite)
		}
	}
	return "", fmt.Errorf("%w: gate %s cannot handle %s", ErrSupervisionIllegalTransition, state, in.Event)
}

// HoldEvent names one row of the hold transition table.
type HoldEvent string

const (
	HoldEventPlace                    HoldEvent = "place"
	HoldEventDuplicateSamePayload     HoldEvent = "duplicate-same-payload"
	HoldEventDuplicateChangedPayload  HoldEvent = "duplicate-changed-payload"
	HoldEventRelease                  HoldEvent = "release"
	HoldEventAmendmentAddsDescendants HoldEvent = "amendment-adds-descendants"
	HoldEventClosureMemberClaimed     HoldEvent = "closure-member-claimed"
	HoldEventRunSettled               HoldEvent = "run-settled"
)

// HoldTransitionInput carries the hold, the event and its transaction-time
// guards. Placing a hold passes a Hold whose State is empty.
type HoldTransitionInput struct {
	Hold  Hold
	Event HoldEvent
	Actor Actor
	// ActorScopeCoversRun reports that the acting capability is scoped to this
	// run.
	ActorScopeCoversRun bool
	// BranchRootExists reports that the branch root task exists at the
	// recorded graph revision.
	BranchRootExists bool
	// ClosureRecomputed reports that the downstream closure was recomputed in
	// the same transaction.
	ClosureRecomputed bool
}

// HoldTransition is the pure hold state machine of the seam review, section
// 3.2.
func HoldTransition(in HoldTransitionInput) (HoldState, error) {
	switch in.Hold.State {
	case "":
		if in.Event != HoldEventPlace {
			return "", fmt.Errorf("%w: no hold exists to receive %s", ErrSupervisionIllegalTransition, in.Event)
		}
		if err := in.Actor.Validate(); err != nil {
			return "", err
		}
		if !in.ActorScopeCoversRun {
			return "", fmt.Errorf("%w: the actor's scope does not cover this run", ErrSupervisionUnauthorizedActor)
		}
		switch in.Hold.Scope.Kind {
		case HoldScopeRun:
		case HoldScopeBranch:
			if strings.TrimSpace(in.Hold.Scope.BranchRootTaskID) == "" {
				return "", fmt.Errorf("%w: a branch hold needs a root task", ErrSupervisionPrerequisite)
			}
			if !in.BranchRootExists {
				return "", fmt.Errorf("%w: the branch root does not exist at graph revision %d",
					ErrSupervisionPrerequisite, in.Hold.GraphRevision)
			}
			if !in.ClosureRecomputed {
				return "", fmt.Errorf("%w: the downstream closure was not computed in this transaction", ErrSupervisionPrerequisite)
			}
		default:
			return "", fmt.Errorf("%w: unknown hold scope %q", ErrSupervisionPrerequisite, in.Hold.Scope.Kind)
		}
		return HoldActive, nil
	case HoldActive:
		switch in.Event {
		case HoldEventDuplicateSamePayload, HoldEventClosureMemberClaimed:
			// A claimed attempt inside the closure is past the start boundary.
			// It is reported, not interrupted and not rolled back.
			return HoldActive, nil
		case HoldEventDuplicateChangedPayload:
			return "", fmt.Errorf("%w: the request key was already used with a different payload", ErrSupervisionStaleRevision)
		case HoldEventRelease:
			if err := in.Actor.Validate(); err != nil {
				return "", err
			}
			if !in.Actor.SameIdentity(in.Hold.Owner) && in.Actor.Kind != ActorOperator {
				return "", fmt.Errorf("%w: only the owner or an operator may release this hold", ErrSupervisionUnauthorizedActor)
			}
			if in.Actor.Kind == ActorOverseer && in.Hold.Owner.Kind == ActorOperator {
				return "", fmt.Errorf("%w: an overseer cannot clear an operator hold", ErrSupervisionUnauthorizedActor)
			}
			return HoldReleased, nil
		case HoldEventAmendmentAddsDescendants:
			if !in.ClosureRecomputed {
				return "", fmt.Errorf("%w: the amendment did not recompute the held closure atomically", ErrSupervisionPrerequisite)
			}
			// New descendants cannot escape an existing branch hold.
			return HoldActive, nil
		case HoldEventRunSettled:
			return HoldReleased, nil
		}
	case HoldReleased:
		return "", fmt.Errorf("%w: the hold is released", ErrSupervisionTerminal)
	}
	return "", fmt.Errorf("%w: hold %s cannot handle %s", ErrSupervisionIllegalTransition, in.Hold.State, in.Event)
}

// ActivationEvent names one row of the activation transition table.
type ActivationEvent string

const (
	ActivationEventTriggerFired               ActivationEvent = "trigger-fired"
	ActivationEventDispatchConfirmed          ActivationEvent = "dispatch-confirmed"
	ActivationEventDispatchUndelivered        ActivationEvent = "dispatch-undelivered"
	ActivationEventDispatchAmbiguous          ActivationEvent = "dispatch-ambiguous"
	ActivationEventDecisionRecorded           ActivationEvent = "decision-recorded"
	ActivationEventLimitReached               ActivationEvent = "limit-reached"
	ActivationEventLeaseExpired               ActivationEvent = "lease-expired"
	ActivationEventOperatorTakeover           ActivationEvent = "operator-takeover"
	ActivationEventThreadLost                 ActivationEvent = "thread-lost"
	ActivationEventReconciliationAcknowledged ActivationEvent = "reconciliation-acknowledged"
	ActivationEventReconciliationAmbiguous    ActivationEvent = "reconciliation-ambiguous"
	ActivationEventEventsArrived              ActivationEvent = "events-arrived"
	ActivationEventRunSettled                 ActivationEvent = "run-settled"
	ActivationEventOperatorContinuation       ActivationEvent = "operator-continuation"
)

// ActivationTransitionInput carries the activation, the event and its
// transaction-time guards.
type ActivationTransitionInput struct {
	Activation Activation
	Event      ActivationEvent
	Actor      Actor
	// Supervised reports that the run has a supervision record at all.
	Supervised bool
	// InboxNonEmpty reports pending events waiting to be consumed.
	InboxNonEmpty bool
	// ActivationBudgetRemaining and TurnsRemaining are the two budgets.
	ActivationBudgetRemaining bool
	TurnsRemaining            bool
	// OtherValidActivation reports another valid active activation for this
	// run. At most one may exist.
	OtherValidActivation bool
	// LeaseValid reports that the coordinator-issued lease is still live.
	LeaseValid bool
	// ExpectedEpoch is the epoch a decision names.
	ExpectedEpoch int64
	// RevisionMatches reports that the expected record revision a decision
	// names is current.
	RevisionMatches bool
	// ExecutionObserved reports that the dispatched activation was ever
	// observed executing. An activation that started and then vanished counts
	// toward the budget; a provably undelivered one does not.
	ExecutionObserved bool
	// RuntimeProvenStopped reports that the old runtime was reconciled and
	// proven stopped. An ambiguous live runtime never authorizes a
	// replacement.
	RuntimeProvenStopped bool
	// RecoveryComplete reports that effect-safe recovery finished, which is
	// the only point at which reservations are released.
	RecoveryComplete bool
	// OperatorAuthorized reports an explicit operator authorization for a
	// continuation.
	OperatorAuthorized bool
}

// ActivationTransitionResult is the next activation state plus the two facts a
// caller cannot derive from the state alone: which epoch the activation is now
// at, and whether this transition spent budget.
type ActivationTransitionResult struct {
	State ActivationState
	Epoch int64
	// CountsTowardBudget reports that this transition consumed one activation
	// of the run budget.
	CountsTowardBudget bool
	// FreshBudget reports an operator-authorized continuation, which writes a
	// new budget and a new receipt rather than silently resetting counters.
	FreshBudget bool
}

// ActivationTransition is the pure activation state machine of the seam review,
// section 3.3.
func ActivationTransition(in ActivationTransitionInput) (ActivationTransitionResult, error) {
	state := in.Activation.State
	if state == "" {
		state = ActivationIdle
	}
	epoch := in.Activation.Epoch
	stay := func(next ActivationState) (ActivationTransitionResult, error) {
		return ActivationTransitionResult{State: next, Epoch: epoch}, nil
	}
	if state == ActivationClosed {
		return ActivationTransitionResult{}, fmt.Errorf("%w: the activation is closed", ErrSupervisionTerminal)
	}
	switch in.Event {
	case ActivationEventRunSettled:
		return stay(ActivationClosed)
	case ActivationEventOperatorContinuation:
		if err := in.Actor.Validate(); err != nil {
			return ActivationTransitionResult{}, err
		}
		if in.Actor.Kind != ActorOperator || !in.OperatorAuthorized {
			return ActivationTransitionResult{}, fmt.Errorf("%w: continuation needs explicit operator authorization",
				ErrSupervisionUnauthorizedActor)
		}
		return ActivationTransitionResult{State: ActivationIdle, Epoch: epoch + 1, FreshBudget: true}, nil
	}
	switch state {
	case ActivationIdle:
		if in.Event != ActivationEventTriggerFired {
			break
		}
		if !in.Supervised {
			return ActivationTransitionResult{}, fmt.Errorf("%w: the run has no supervision record", ErrSupervisionPrerequisite)
		}
		if !in.InboxNonEmpty {
			return ActivationTransitionResult{}, fmt.Errorf("%w: no pending supervision events to consume", ErrSupervisionPrerequisite)
		}
		if in.OtherValidActivation {
			return ActivationTransitionResult{}, fmt.Errorf("%w: another activation is already valid for this run",
				ErrSupervisionPrerequisite)
		}
		if !in.ActivationBudgetRemaining {
			// One escalation, then wait for an operator. This is a state, not
			// an error: the run keeps a record of why nothing woke.
			return stay(ActivationEscalated)
		}
		return stay(ActivationPendingDispatch)
	case ActivationPendingDispatch:
		switch in.Event {
		case ActivationEventDispatchConfirmed:
			if !in.LeaseValid {
				return ActivationTransitionResult{}, fmt.Errorf("%w: dispatch was confirmed without a live lease",
					ErrSupervisionPrerequisite)
			}
			return ActivationTransitionResult{State: ActivationActive, Epoch: epoch, CountsTowardBudget: true}, nil
		case ActivationEventDispatchUndelivered:
			if in.ExecutionObserved {
				return ActivationTransitionResult{}, fmt.Errorf("%w: execution was observed, so the dispatch is not undelivered",
					ErrSupervisionPrerequisite)
			}
			// Retried with the original dispatch identity, and it spends no
			// budget.
			return stay(ActivationPendingDispatch)
		case ActivationEventDispatchAmbiguous:
			return stay(ActivationRecoveryRequired)
		}
	case ActivationActive:
		switch in.Event {
		case ActivationEventDecisionRecorded:
			if !in.LeaseValid {
				return ActivationTransitionResult{}, fmt.Errorf("%w: the activation lease is not live", ErrSupervisionPrerequisite)
			}
			if in.ExpectedEpoch != epoch {
				return ActivationTransitionResult{}, fmt.Errorf("%w: decision names epoch %d, the activation is at %d",
					ErrSupervisionStaleRevision, in.ExpectedEpoch, epoch)
			}
			if !in.RevisionMatches {
				return ActivationTransitionResult{}, fmt.Errorf("%w: the decision's expected revision is not current",
					ErrSupervisionStaleRevision)
			}
			if !in.TurnsRemaining {
				return ActivationTransitionResult{}, fmt.Errorf("%w: no turns remain in this activation",
					ErrSupervisionBudgetExhausted)
			}
			return stay(ActivationActive)
		case ActivationEventLimitReached:
			return stay(ActivationSpent)
		case ActivationEventLeaseExpired:
			return stay(ActivationRevoked)
		case ActivationEventOperatorTakeover:
			if err := in.Actor.Validate(); err != nil {
				return ActivationTransitionResult{}, err
			}
			if in.Actor.Kind != ActorOperator {
				return ActivationTransitionResult{}, fmt.Errorf("%w: only an operator may take over supervision",
					ErrSupervisionUnauthorizedActor)
			}
			// Takeover raises the epoch as well as revoking. Revocation alone
			// leaves the record at the epoch the replaced overseer still
			// names, and every authority check that compares a decision's
			// epoch against the record's would keep agreeing with it: a
			// decision already formed before the takeover would land after
			// it and reverse the operator. Raising the epoch is what "late
			// decisions from this epoch are fenced out" means once the fence
			// is a number rather than an intention.
			return ActivationTransitionResult{State: ActivationRevoked, Epoch: epoch + 1}, nil
		case ActivationEventThreadLost:
			if !in.RuntimeProvenStopped {
				return stay(ActivationRecoveryRequired)
			}
			return ActivationTransitionResult{
				State: ActivationIdle, Epoch: epoch + 1, CountsTowardBudget: in.ExecutionObserved,
			}, nil
		}
	case ActivationRevoked:
		switch in.Event {
		case ActivationEventReconciliationAcknowledged:
			if !in.RecoveryComplete {
				return ActivationTransitionResult{}, fmt.Errorf("%w: effect-safe recovery has not completed",
					ErrSupervisionPrerequisite)
			}
			// Idle at epoch+1, for the reason spelled out on the spent case
			// below: the activation this one replaces already exists at the
			// current epoch, and every identity the replacement would derive is
			// derived from (run, epoch). A lease that expired revokes without
			// raising the epoch, so without this the epoch would be raised only
			// by a takeover and a recovered run would keep reusing the identity
			// of the activation whose authority it just revoked.
			return ActivationTransitionResult{State: ActivationIdle, Epoch: epoch + 1}, nil
		case ActivationEventReconciliationAmbiguous:
			return stay(ActivationRecoveryRequired)
		}
	case ActivationSpent:
		if in.Event != ActivationEventEventsArrived {
			break
		}
		if !in.ActivationBudgetRemaining {
			return stay(ActivationEscalated)
		}
		// A spent activation returns to idle at epoch+1, so the next wake is a
		// new activation rather than the same one again.
		//
		// Everything an activation is made of is derived from (run, epoch): its
		// own ID, its dispatch identity, the attempt that carries it, that
		// attempt's assignment and the T3 thread the assignment names. Returning
		// to idle at the same epoch would therefore recompute the identities the
		// spent activation already used, and the second review round would
		// collide with the first instead of being a second turn: the assignment
		// commit would find a row that already exists, and the record's epoch
		// fence could not tell a late decision of the first round from a
		// decision of the second. Raising the epoch here is what lets a run that
		// needs two review rounds get a second overseer without an operator
		// takeover, which is the only other transition that raises it.
		return ActivationTransitionResult{State: ActivationIdle, Epoch: epoch + 1}, nil
	case ActivationRecoveryRequired, ActivationEscalated:
		// Both wait for an operator. Only run settlement and an authorized
		// continuation, handled above, move them.
	}
	return ActivationTransitionResult{}, fmt.Errorf("%w: activation %s cannot handle %s",
		ErrSupervisionIllegalTransition, state, in.Event)
}

// IncidentEvent names one row of the review incident transition table.
type IncidentEvent string

const (
	IncidentEventRaise           IncidentEvent = "raise"
	IncidentEventGateAccepted    IncidentEvent = "gate-accepted"
	IncidentEventEscalate        IncidentEvent = "escalate"
	IncidentEventConcludeFailure IncidentEvent = "conclude-failure"
	IncidentEventDeadlineExpired IncidentEvent = "deadline-expired"
	IncidentEventBulkClose       IncidentEvent = "bulk-close"
	IncidentEventOperatorResolve IncidentEvent = "operator-resolve"
	IncidentEventOperatorCancel  IncidentEvent = "operator-cancel"
	IncidentEventRunCancelled    IncidentEvent = "run-cancelled"
)

// IncidentTransitionInput carries the incident, the event and its
// transaction-time guards. Raising an incident passes a ReviewIncident whose
// State is empty.
type IncidentTransitionInput struct {
	Incident ReviewIncident
	Event    IncidentEvent
	Actor    Actor
	// ReasonChangedOrThresholdCrossed reports that the normalized block reason
	// differs from the last recorded one, or that a threshold was crossed.
	// Ordinary brief quota or resource waiting never satisfies it.
	ReasonChangedOrThresholdCrossed bool
	// ActorScopeCoversRun reports that the acting capability is scoped to this
	// run.
	ActorScopeCoversRun bool
	// MatchingGateIncident reports that an acceptance is closing its own
	// incident rather than someone else's.
	MatchingGateIncident bool
	// TaskTerminallyFailed reports that the source task is terminally failed,
	// which is the only condition under which conclude-failure is permitted.
	TaskTerminallyFailed bool
	// ExpectedRevision is the incident revision the decision names.
	ExpectedRevision int64
}

// IncidentTransition is the pure review incident state machine of the seam
// review, section 3.4.
//
// v1 ships single-incident resolve only, so a bulk close is refused outright
// and the exact-set rule is vacuously satisfied.
func IncidentTransition(in IncidentTransitionInput) (IncidentState, error) {
	state := in.Incident.State
	if state == IncidentResolved {
		return "", fmt.Errorf("%w: the incident is resolved", ErrSupervisionTerminal)
	}
	if in.Event == IncidentEventRunCancelled && state != "" {
		return IncidentResolved, nil
	}
	switch state {
	case "":
		if in.Event != IncidentEventRaise {
			break
		}
		if !in.ReasonChangedOrThresholdCrossed {
			return "", fmt.Errorf("%w: the normalized block reason did not change and no threshold was crossed",
				ErrSupervisionPrerequisite)
		}
		return IncidentOpen, nil
	case IncidentOpen:
		switch in.Event {
		case IncidentEventGateAccepted:
			if !in.MatchingGateIncident {
				return "", fmt.Errorf("%w: a gate acceptance closes only its own incident", ErrSupervisionPrerequisite)
			}
			return IncidentResolved, nil
		case IncidentEventEscalate:
			if err := in.Actor.Validate(); err != nil {
				return "", err
			}
			if !in.ActorScopeCoversRun {
				return "", fmt.Errorf("%w: the actor's scope does not cover this run", ErrSupervisionUnauthorizedActor)
			}
			return IncidentEscalated, nil
		case IncidentEventConcludeFailure:
			if err := in.Actor.Validate(); err != nil {
				return "", err
			}
			if !in.ActorScopeCoversRun {
				return "", fmt.Errorf("%w: the actor's scope does not cover this run", ErrSupervisionUnauthorizedActor)
			}
			if !in.TaskTerminallyFailed {
				return "", fmt.Errorf("%w: conclude-failure needs an observed terminal task failure",
					ErrSupervisionPrerequisite)
			}
			if in.ExpectedRevision != in.Incident.Revision {
				return "", fmt.Errorf("%w: resolution names incident revision %d, the incident is at %d",
					ErrSupervisionStaleRevision, in.ExpectedRevision, in.Incident.Revision)
			}
			return IncidentResolved, nil
		case IncidentEventDeadlineExpired:
			return IncidentEscalated, nil
		case IncidentEventBulkClose:
			return "", fmt.Errorf("%w: v1 resolves one named incident at a time", ErrSupervisionPrerequisite)
		}
	case IncidentEscalated:
		switch in.Event {
		case IncidentEventOperatorResolve, IncidentEventOperatorCancel:
			if err := in.Actor.Validate(); err != nil {
				return "", err
			}
			if in.Actor.Kind != ActorOperator {
				return "", fmt.Errorf("%w: an escalation stays unresolved until operator action",
					ErrSupervisionUnauthorizedActor)
			}
			if in.Event == IncidentEventOperatorResolve && in.ExpectedRevision != in.Incident.Revision {
				return "", fmt.Errorf("%w: resolution names incident revision %d, the incident is at %d",
					ErrSupervisionStaleRevision, in.ExpectedRevision, in.Incident.Revision)
			}
			return IncidentResolved, nil
		case IncidentEventConcludeFailure, IncidentEventEscalate:
			if in.Actor.Kind == ActorOverseer {
				return "", fmt.Errorf("%w: an escalation stays unresolved until operator action",
					ErrSupervisionUnauthorizedActor)
			}
		}
	}
	return "", fmt.Errorf("%w: incident %s cannot handle %s", ErrSupervisionIllegalTransition, state, in.Event)
}
