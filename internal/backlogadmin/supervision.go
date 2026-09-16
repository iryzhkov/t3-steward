package backlogadmin

// Campaign supervision on the coordinator admin transport.
//
// This file is the authority half of supervision: it decides who may act, on
// which run and under which activation epoch, and it routes a structured
// decision into the pure state machines published by internal/domain. It
// defines no state machine of its own and it never widens one.
//
// Two operation words carry the family. "supervision-show" is read-only.
// "supervision-decision" carries decide, hold, release, escalate and resolve,
// and is mutating, so the remote carrier gives it durable replay protection.
// Two words rather than one so that an authorized_keys line can pin a key to
// reading supervision without granting it the authority to decide.
//
// Nothing here parses prose. A reason is recorded on every decision and grants
// nothing: the outcome, the gate, the evidence snapshot, the revisions and the
// epoch are separate structured fields, and a request that omits them is
// refused however persuasive its reason is.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// SupervisionVersion versions the supervision request and response documents.
// It is separate from the transport version because the two change for
// different reasons: a new carrier does not change what a decision is.
const SupervisionVersion = "backlog.admin.supervision/v1"

// The two action names supervision is authorized under. They are deliberately
// not query kinds: every declared query kind is granted to the remote-admin
// role automatically, and the authority to decide a gate is not a read.
const (
	SupervisionShowKind     QueryKind = "supervision-show"
	SupervisionDecisionKind QueryKind = "supervision-decision"
)

// ErrSupervisionUnavailable reports that the coordinator cannot answer a
// supervision request right now: no store is bound, or the record could not be
// read. It is the fourth error class the plan requires, beside the three the
// domain guards already provide, and it is the only one a caller should retry.
var ErrSupervisionUnavailable = errors.New("supervision is temporarily unavailable")

// ErrSupervisionRequestConflict reports the same request key carrying a
// different payload. Replaying a key is how a lost answer is recovered;
// changing the payload under a key that was already answered is a different
// decision wearing the first one's identity, so it is refused rather than
// answered from the receipt.
var ErrSupervisionRequestConflict = errors.New("supervision request key was already used with a different payload")

// SupervisionOperation is one word of the supervision family.
type SupervisionOperation string

const (
	SupervisionShow     SupervisionOperation = "show"
	SupervisionDecide   SupervisionOperation = "decide"
	SupervisionHold     SupervisionOperation = "hold"
	SupervisionRelease  SupervisionOperation = "release"
	SupervisionEscalate SupervisionOperation = "escalate"
	SupervisionResolve  SupervisionOperation = "resolve"
)

// SupervisionOperations is every declared supervision operation, in the order
// an operator meets them.
func SupervisionOperations() []SupervisionOperation {
	return []SupervisionOperation{
		SupervisionShow, SupervisionDecide, SupervisionHold,
		SupervisionRelease, SupervisionEscalate, SupervisionResolve,
	}
}

// Mutating reports whether the operation changes durable state. Only show does
// not.
func (o SupervisionOperation) Mutating() bool { return o != SupervisionShow }

// ActionKind is the name the operation is authorized under.
func (o SupervisionOperation) ActionKind() QueryKind {
	if o.Mutating() {
		return SupervisionDecisionKind
	}
	return SupervisionShowKind
}

func validSupervisionOperation(operation SupervisionOperation) bool {
	for _, candidate := range SupervisionOperations() {
		if candidate == operation {
			return true
		}
	}
	return false
}

// SupervisionGateDecision is a gate accept or reject. Every field is a
// separate structured value the coordinator checks; none of them is inferred
// from text.
type SupervisionGateDecision struct {
	GateID string `json:"gateId"`
	// Outcome is accept or reject. There is no third value and no default: a
	// request that names neither is malformed rather than a rejection.
	Outcome domain.GateDecisionOutcome `json:"outcome"`
	// EvidenceSnapshotID is the snapshot identity the decision was made
	// against. It is compared with the gate's current snapshot, so a decision
	// about evidence that has since been replaced loses the race by name.
	EvidenceSnapshotID string `json:"evidenceSnapshotId"`
	// ExpectedGraphRevision is the graph revision the decision names.
	ExpectedGraphRevision int64 `json:"expectedGraphRevision"`
	// IncidentID is the review incident this acceptance closes, when it closes
	// one. An acceptance closes only its own incident.
	IncidentID string `json:"incidentId,omitempty"`
}

// SupervisionHoldRequest places one dispatch hold.
type SupervisionHoldRequest struct {
	Scope domain.HoldScope `json:"scope"`
}

// SupervisionReleaseRequest releases one named hold. Releasing one hold never
// releases another, so exactly one ID is named.
type SupervisionReleaseRequest struct {
	HoldID string `json:"holdId"`
}

// SupervisionIncidentRequest escalates or resolves one review incident.
//
// IncidentIDs exists only so that a bulk close has a shape to arrive in and a
// refusal to be answered with. Version 1 resolves one named incident at a
// time; the domain refuses the bulk event outright.
type SupervisionIncidentRequest struct {
	IncidentID  string                 `json:"incidentId,omitempty"`
	IncidentIDs []string               `json:"incidentIds,omitempty"`
	Outcome     domain.IncidentOutcome `json:"outcome,omitempty"`
}

// SupervisionRequest is one structured supervision operation.
//
// ExpectedRevision fences the mutation against the record it targets: the
// gate's revision for decide, the supervision record's revision for hold and
// release, the incident's revision for escalate and resolve.
type SupervisionRequest struct {
	Version   string               `json:"version"`
	Operation SupervisionOperation `json:"operation"`
	RunID     string               `json:"runId"`
	// ActivationEpoch is the epoch the caller acts under. A supervisor must
	// name the epoch the coordinator currently considers valid; an operator
	// may leave it zero, because operator authority is not bound to an
	// activation.
	ActivationEpoch int64 `json:"activationEpoch,omitempty"`
	// RequestKey is the idempotency key of a mutation.
	RequestKey       string `json:"requestKey,omitempty"`
	ExpectedRevision int64  `json:"expectedRevision,omitempty"`
	// Reason is recorded on the decision. It is evidence for a later reader
	// and never a grant of authority.
	Reason   string                      `json:"reason,omitempty"`
	Gate     *SupervisionGateDecision    `json:"gate,omitempty"`
	Hold     *SupervisionHoldRequest     `json:"hold,omitempty"`
	Release  *SupervisionReleaseRequest  `json:"release,omitempty"`
	Incident *SupervisionIncidentRequest `json:"incident,omitempty"`
}

// SupervisionGateView is one gate plus the transaction-time facts the pure
// gate state machine needs and a caller cannot derive from the gate alone.
type SupervisionGateView struct {
	Gate domain.Gate `json:"gate"`
	// ProducersVerified reports that every observed task succeeded with its
	// outputs verified and in coordinator custody.
	ProducersVerified bool `json:"producersVerified"`
	// SuccessorOfferedOrStarted reports that a protected task has already been
	// offered or started.
	SuccessorOfferedOrStarted bool `json:"successorOfferedOrStarted"`
	// Evidence is the snapshot the gate currently holds, when one exists.
	Evidence *domain.EvidenceSnapshot `json:"evidence,omitempty"`
	// LastDecision is the most recent decision recorded against this gate. It
	// names the actor, so show answers who decided a gate rather than only what
	// the gate became: an overseer acting as itself and an operator acting while
	// that overseer was live leave the same accepted gate behind.
	LastDecision *domain.GateDecision `json:"lastDecision,omitempty"`
}

// SupervisionIncidentView is one incident plus the one fact conclude-failure
// turns on.
type SupervisionIncidentView struct {
	Incident domain.ReviewIncident `json:"incident"`
	// SourceTaskTerminallyFailed reports that the incident's source task is
	// terminally failed, which is the only condition under which
	// conclude-failure is permitted.
	SourceTaskTerminallyFailed bool `json:"sourceTaskTerminallyFailed"`
}

// SupervisionState is the whole read-only picture of one run's supervision:
// the overview, the inbox and the evidence the plan asks show to expose.
type SupervisionState struct {
	Record     domain.SupervisionRecord  `json:"record"`
	Activation domain.Activation         `json:"activation"`
	Gates      []SupervisionGateView     `json:"gates,omitempty"`
	Holds      []domain.Hold             `json:"holds,omitempty"`
	Incidents  []SupervisionIncidentView `json:"incidents,omitempty"`
	// SinkSettled reports terminal settlement, after which every mutating
	// supervision capability is revoked.
	SinkSettled bool `json:"sinkSettled"`
	// RouteAvailable reports whether an overseer could be dispatched to decide
	// this run's gates right now, and RouteBlockReason says why not when it
	// could not.
	//
	// They are here because show is the command an operator runs when a gate is
	// not moving, and the most common reason it is not moving is not visible in
	// any gate, hold or incident: no worker hosts the overseer route, or this
	// coordinator has no supervisor admin client at all.
	RouteAvailable   bool   `json:"routeAvailable"`
	RouteBlockReason string `json:"routeBlockReason,omitempty"`
}

// SupervisionBranch is the dependency closure of one branch root, resolved by
// the store in the same call that will commit the hold.
type SupervisionBranch struct {
	Exists          bool     `json:"exists"`
	GraphRevision   int64    `json:"graphRevision"`
	ResolvedTaskIDs []string `json:"resolvedTaskIds,omitempty"`
}

// SupervisionCommit is the decided outcome handed to the store. The service
// has already run the pure transition, so the store writes a computed next
// state rather than deciding one: the state machine lives in exactly one
// place.
type SupervisionCommit struct {
	RunID     string               `json:"runId"`
	Operation SupervisionOperation `json:"operation"`
	Actor     domain.Actor         `json:"actor"`
	// RequestKey and PayloadDigest are the idempotency receipt the store
	// records with the effect, so that a replay is answered from the same
	// transaction that produced the answer.
	RequestKey       string `json:"requestKey"`
	PayloadDigest    string `json:"payloadDigest"`
	Reason           string `json:"reason"`
	ExpectedRevision int64  `json:"expectedRevision"`
	// Gate, Hold and Incident carry the computed next state of whichever
	// record the operation targets.
	GateID        string                    `json:"gateId,omitempty"`
	GateState     domain.GateState          `json:"gateState,omitempty"`
	Decision      *domain.GateDecision      `json:"decision,omitempty"`
	Hold          *domain.Hold              `json:"hold,omitempty"`
	HoldID        string                    `json:"holdId,omitempty"`
	HoldState     domain.HoldState          `json:"holdState,omitempty"`
	IncidentID    string                    `json:"incidentId,omitempty"`
	IncidentState domain.IncidentState      `json:"incidentState,omitempty"`
	Resolution    *domain.ResolutionReceipt `json:"resolution,omitempty"`
	Now           time.Time                 `json:"now"`
	// Request is the structured request exactly as it arrived, carried so that
	// a store binding can build its own per-operation request from one place
	// instead of re-deriving fields from the computed state above.
	Request SupervisionRequest `json:"request"`
}

// SupervisionReceipt is the durable answer of one committed request key.
type SupervisionReceipt struct {
	PayloadDigest string              `json:"payloadDigest"`
	Response      SupervisionResponse `json:"response"`
}

// SupervisionStore is the durable half of the supervision family. It is
// declared here, where it is used, rather than imported from the store
// package: this package owns authorization and routing and must not depend on
// a schema. Lane B7 binds it to the coordinator store.
//
// It is reached through an optional interface on the service's reader, the
// same way graph amendment and native waits are, so a store that predates
// supervision leaves the operation answering "unavailable" instead of failing
// to compile.
//
// CommitSupervision is one method rather than five because authorization and
// routing need one seam, not one per verb. A binding switches on
// SupervisionCommit.Operation and calls the store transaction that verb owns:
// decide onto the gate decision, hold onto placing a hold, release onto
// releasing one, escalate and resolve onto the incident resolution. The whole
// structured request travels in SupervisionCommit.Request so the binding has
// every field those transactions ask for.
//
// The store, not this package, is the authority on the outcome. The pure
// transitions run here as well, before the commit, so that a refusal is
// answered without opening a transaction and so that the refusal carries the
// same guard error either way; the store runs them again inside its
// transaction, against rows it read there, and its answer wins. Running a pure
// function twice costs nothing and removes the temptation to decide outside
// the transaction that commits.
type SupervisionStore interface {
	// LoadSupervision reads one run's whole supervision state. A run without a
	// supervision record is reported by an error wrapping ErrNotFound, because
	// absence of the record is the unsupervised case and not an empty one.
	LoadSupervision(ctx context.Context, runID string) (SupervisionState, error)
	// ResolveSupervisionBranch computes a branch root's downstream closure at
	// the current graph revision.
	ResolveSupervisionBranch(ctx context.Context, runID, rootTaskID string) (SupervisionBranch, error)
	// SupervisionReplay returns the receipt an earlier request key produced.
	SupervisionReplay(ctx context.Context, runID, requestKey string) (SupervisionReceipt, bool, error)
	// CommitSupervision applies one decided outcome and its receipt in one
	// transaction, refusing a stale expected revision.
	CommitSupervision(ctx context.Context, commit SupervisionCommit) (SupervisionReceipt, error)
}

// SupervisionTransport is a coordinator admin transport that also carries
// supervision. It is declared beside the operation rather than folded into
// CoordinatorAdminTransport so that a client which predates supervision still
// satisfies that interface; a caller asserts for this one and reports an
// unavailable operation when the assertion fails. Both carriers satisfy it.
type SupervisionTransport interface {
	CoordinatorAdminTransport
	Supervise(ctx context.Context, request SupervisionRequest) (SupervisionResponse, error)
}

// SupervisionResponse is the versioned answer to one supervision operation.
type SupervisionResponse struct {
	Version     string               `json:"version"`
	Operation   SupervisionOperation `json:"operation"`
	RunID       string               `json:"runId"`
	GeneratedAt time.Time            `json:"generatedAt"`
	// Actor is the authenticated identity the decision was recorded under. It
	// is the coordinator's own conclusion from the verified principal, never
	// the actor the request claimed.
	Actor domain.Actor `json:"actor"`
	// Replay reports an answer recorded by an earlier request carrying this
	// key and this payload.
	Replay bool `json:"replay,omitempty"`
	// State is the whole read-only picture, present on show.
	State *SupervisionState `json:"state,omitempty"`
	// The remaining fields are the resulting state of whichever record the
	// operation changed.
	GateID        string                    `json:"gateId,omitempty"`
	GateState     domain.GateState          `json:"gateState,omitempty"`
	Decision      *domain.GateDecision      `json:"decision,omitempty"`
	Hold          *domain.Hold              `json:"hold,omitempty"`
	IncidentID    string                    `json:"incidentId,omitempty"`
	IncidentState domain.IncidentState      `json:"incidentState,omitempty"`
	Resolution    *domain.ResolutionReceipt `json:"resolution,omitempty"`
}

// SupervisionErrorClass is the stable class a client branches on. The plan
// requires exactly these distinctions; the prose of a refusal is for a human.
type SupervisionErrorClass string

const (
	SupervisionErrorStaleEvidence     SupervisionErrorClass = "stale-evidence"
	SupervisionErrorUnauthorizedScope SupervisionErrorClass = "unauthorized-scope"
	SupervisionErrorPrerequisite      SupervisionErrorClass = "unmet-prerequisite"
	SupervisionErrorUnavailable       SupervisionErrorClass = "temporarily-unavailable"
	SupervisionErrorMalformed         SupervisionErrorClass = "malformed-request"
)

// ClassifySupervisionError maps a refusal onto its class. The mapping is the
// whole point of the guard sentinels: a race must never read as a permission
// problem, and a permission problem must never read as something to retry.
func ClassifySupervisionError(err error) SupervisionErrorClass {
	// A refusal that crossed a carrier has lost its sentinels, and carries the
	// class the coordinator decided on the transport error instead.
	var transportErr *TransportError
	if err != nil && errors.As(err, &transportErr) && transportErr.SupervisionClass != "" {
		return transportErr.SupervisionClass
	}
	switch {
	case err == nil:
		return ""
	case errors.Is(err, domain.ErrSupervisionStaleRevision), errors.Is(err, ErrSupervisionRequestConflict):
		return SupervisionErrorStaleEvidence
	case errors.Is(err, domain.ErrSupervisionUnauthorizedActor):
		return SupervisionErrorUnauthorizedScope
	case errors.Is(err, domain.ErrSupervisionPrerequisite),
		errors.Is(err, domain.ErrSupervisionBudgetExhausted),
		errors.Is(err, domain.ErrSupervisionTerminal),
		errors.Is(err, ErrNotFound):
		return SupervisionErrorPrerequisite
	case errors.Is(err, ErrSupervisionUnavailable):
		return SupervisionErrorUnavailable
	default:
		// An illegal transition, an unsupported version and an invalid query are
		// all the same thing to a client: the request itself was wrong, and
		// repeating it unchanged will fail again.
		return SupervisionErrorMalformed
	}
}

// SupervisionPayloadDigest is the identity of a request's payload under its
// key. The key, the version and the reason are excluded: a retry is allowed to
// re-send the same decision, and the reason is recorded rather than acted on,
// so neither should turn a replay into a conflict.
func SupervisionPayloadDigest(request SupervisionRequest) (string, error) {
	payload := request
	payload.Version = ""
	payload.RequestKey = ""
	payload.Reason = ""
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("%w: supervision payload cannot be digested", ErrInvalidQuery)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// Validate checks the shape of a supervision request before any authority is
// consulted. A malformed request is refused the same way for every principal,
// so the refusal says nothing about who is configured here.
func (r SupervisionRequest) Validate() error {
	if r.Version != SupervisionVersion {
		return fmt.Errorf("%w: supervision version %q", ErrUnsupportedVersion, r.Version)
	}
	if !validSupervisionOperation(r.Operation) {
		return fmt.Errorf("%w: unknown supervision operation %q", ErrInvalidQuery, r.Operation)
	}
	if strings.TrimSpace(r.RunID) != r.RunID || r.RunID == "" {
		return fmt.Errorf("%w: a supervision request names one run", ErrInvalidQuery)
	}
	if r.ActivationEpoch < 0 {
		return fmt.Errorf("%w: an activation epoch is not negative", ErrInvalidQuery)
	}
	if !r.Operation.Mutating() {
		if r.Gate != nil || r.Hold != nil || r.Release != nil || r.Incident != nil ||
			r.RequestKey != "" || r.ExpectedRevision != 0 {
			return fmt.Errorf("%w: a supervision read carries no decision", ErrInvalidQuery)
		}
		return nil
	}
	if strings.TrimSpace(r.RequestKey) != r.RequestKey || r.RequestKey == "" {
		return fmt.Errorf("%w: a supervision decision needs a request key", ErrInvalidQuery)
	}
	if strings.TrimSpace(r.Reason) == "" {
		return fmt.Errorf("%w: a supervision decision needs a reason", ErrInvalidQuery)
	}
	if r.ExpectedRevision <= 0 {
		return fmt.Errorf("%w: a supervision decision names the revision it expects", ErrInvalidQuery)
	}
	carried := 0
	for _, present := range []bool{r.Gate != nil, r.Hold != nil, r.Release != nil, r.Incident != nil} {
		if present {
			carried++
		}
	}
	if carried != 1 {
		return fmt.Errorf("%w: a supervision decision carries exactly one payload", ErrInvalidQuery)
	}
	switch r.Operation {
	case SupervisionDecide:
		return r.validateGate()
	case SupervisionHold:
		return r.validateHold()
	case SupervisionRelease:
		if r.Release == nil || strings.TrimSpace(r.Release.HoldID) == "" {
			return fmt.Errorf("%w: a release names one hold", ErrInvalidQuery)
		}
	case SupervisionEscalate, SupervisionResolve:
		return r.validateIncident()
	}
	return nil
}

func (r SupervisionRequest) validateGate() error {
	if r.Gate == nil || strings.TrimSpace(r.Gate.GateID) == "" {
		return fmt.Errorf("%w: a decision names one gate", ErrInvalidQuery)
	}
	switch r.Gate.Outcome {
	case domain.GateDecisionAccept, domain.GateDecisionReject:
	default:
		return fmt.Errorf("%w: a decision is accept or reject, not %q", ErrInvalidQuery, r.Gate.Outcome)
	}
	if strings.TrimSpace(r.Gate.EvidenceSnapshotID) == "" {
		return fmt.Errorf("%w: a decision names the evidence snapshot it reviewed", ErrInvalidQuery)
	}
	if r.Gate.ExpectedGraphRevision <= 0 {
		return fmt.Errorf("%w: a decision names the graph revision it expects", ErrInvalidQuery)
	}
	return nil
}

func (r SupervisionRequest) validateHold() error {
	if r.Hold == nil {
		return fmt.Errorf("%w: a hold names its scope", ErrInvalidQuery)
	}
	switch r.Hold.Scope.Kind {
	case domain.HoldScopeRun:
		if r.Hold.Scope.BranchRootTaskID != "" {
			return fmt.Errorf("%w: a run hold names no branch root", ErrInvalidQuery)
		}
	case domain.HoldScopeBranch:
		if strings.TrimSpace(r.Hold.Scope.BranchRootTaskID) == "" {
			return fmt.Errorf("%w: a branch hold names its root task", ErrInvalidQuery)
		}
	default:
		return fmt.Errorf("%w: unknown hold scope %q", ErrInvalidQuery, r.Hold.Scope.Kind)
	}
	return nil
}

func (r SupervisionRequest) validateIncident() error {
	if r.Incident == nil {
		return fmt.Errorf("%w: an incident operation names one incident", ErrInvalidQuery)
	}
	if len(r.Incident.IncidentIDs) > 0 {
		// A bulk close is shaped, accepted by the decoder and then refused by
		// the domain, so the refusal is the documented one rather than a
		// decoder error about an unknown field.
		return nil
	}
	if strings.TrimSpace(r.Incident.IncidentID) == "" {
		return fmt.Errorf("%w: an incident operation names one incident", ErrInvalidQuery)
	}
	if r.Operation == SupervisionResolve {
		switch r.Incident.Outcome {
		case domain.IncidentOutcomeConcludeFailure, domain.IncidentOutcomeRemediated, domain.IncidentOutcomeCancelled:
		case domain.IncidentOutcomeGateAccepted:
			return fmt.Errorf("%w: a gate acceptance closes its incident through decide, not resolve", ErrInvalidQuery)
		default:
			return fmt.Errorf("%w: unknown resolution outcome %q", ErrInvalidQuery, r.Incident.Outcome)
		}
	}
	return nil
}

// supervisionActor decides the actor kind from the authenticated roles alone.
// The request never states it: an actor kind taken from the request would be
// an overseer's claim to be an operator.
func supervisionActor(principal Principal, request SupervisionRequest) (domain.Actor, error) {
	actor := domain.Actor{Principal: principal.ID}
	for _, role := range principal.Roles {
		switch role {
		case SupervisorRole:
			actor.Kind = domain.ActorOverseer
			actor.ActivationEpoch = request.ActivationEpoch
			return actor, actor.Validate()
		case LocalAdminRole, RemoteAdminRole:
			actor.Kind = domain.ActorOperator
			return actor, actor.Validate()
		}
	}
	return domain.Actor{}, fmt.Errorf("%w: %q holds no supervision role",
		domain.ErrSupervisionUnauthorizedActor, principal.ID)
}

// fenceSupervisionEpoch refuses a decision made outside the activation the
// coordinator currently considers valid.
//
// This is the fence that makes an operator takeover final: the takeover
// revokes the activation and raises the epoch, so a decision the old
// supervisor had already formed arrives naming an epoch that is no longer
// current and is refused as stale rather than applied late.
func fenceSupervisionEpoch(actor domain.Actor, state SupervisionState, request SupervisionRequest) error {
	if actor.Kind == domain.ActorOperator {
		if request.ActivationEpoch != 0 && request.ActivationEpoch != state.Record.ActivationEpoch {
			return fmt.Errorf("%w: request names epoch %d, the run is at %d",
				domain.ErrSupervisionStaleRevision, request.ActivationEpoch, state.Record.ActivationEpoch)
		}
		return nil
	}
	if request.ActivationEpoch <= 0 {
		return fmt.Errorf("%w: a supervisor names the activation epoch it acts under",
			domain.ErrSupervisionUnauthorizedActor)
	}
	if request.ActivationEpoch != state.Record.ActivationEpoch {
		return fmt.Errorf("%w: request names epoch %d, the run is at %d",
			domain.ErrSupervisionStaleRevision, request.ActivationEpoch, state.Record.ActivationEpoch)
	}
	if state.Activation.Epoch != request.ActivationEpoch {
		return fmt.Errorf("%w: request names epoch %d, the activation is at %d",
			domain.ErrSupervisionStaleRevision, request.ActivationEpoch, state.Activation.Epoch)
	}
	if state.Activation.State != domain.ActivationActive {
		return fmt.Errorf("%w: the activation is %s and holds no decision authority",
			domain.ErrSupervisionStaleRevision, state.Activation.State)
	}
	return nil
}

// Supervise answers one supervision operation.
//
// The order is deliberate: shape, then authority, then state, then
// idempotency, then the pure transition, then the commit. Authority is decided
// before any run state is read, so a principal outside its scope learns
// nothing about the run it asked about.
func (s *Service) Supervise(ctx context.Context, principal Principal, request SupervisionRequest) (SupervisionResponse, error) {
	var empty SupervisionResponse
	if err := request.Validate(); err != nil {
		return empty, err
	}
	action := Action{
		Kind:            request.Operation.ActionKind(),
		WorkflowRunID:   request.RunID,
		ActivationEpoch: request.ActivationEpoch,
	}
	if request.Gate != nil {
		action.GateID, action.IncidentID = request.Gate.GateID, request.Gate.IncidentID
	}
	if request.Release != nil {
		action.HoldID = request.Release.HoldID
	}
	if request.Incident != nil {
		action.IncidentID = request.Incident.IncidentID
	}
	if err := s.authorizer.Authorize(ctx, principal, action); err != nil {
		return empty, fmt.Errorf("authorize %s: %w", action.Kind, err)
	}
	actor, err := supervisionActor(principal, request)
	if err != nil {
		return empty, err
	}
	store, ok := s.supervisionStore()
	if !ok {
		return empty, fmt.Errorf("%w: this coordinator store records no supervision", ErrSupervisionUnavailable)
	}
	state, err := store.LoadSupervision(ctx, request.RunID)
	if err != nil {
		return empty, err
	}
	response := SupervisionResponse{
		Version:     SupervisionVersion,
		Operation:   request.Operation,
		RunID:       request.RunID,
		GeneratedAt: s.now().UTC(),
		Actor:       actor,
	}
	if !request.Operation.Mutating() {
		s.describeSupervisionRoute(ctx, &state)
		response.State = &state
		return response, nil
	}
	if state.SinkSettled {
		return empty, fmt.Errorf("%w: the run settled; supervision capabilities are revoked",
			domain.ErrSupervisionTerminal)
	}
	if err := fenceSupervisionEpoch(actor, state, request); err != nil {
		return empty, err
	}
	digest, err := SupervisionPayloadDigest(request)
	if err != nil {
		return empty, err
	}
	if receipt, found, err := store.SupervisionReplay(ctx, request.RunID, request.RequestKey); err != nil {
		return empty, err
	} else if found {
		if receipt.PayloadDigest != digest {
			return empty, fmt.Errorf("%w: key %q", ErrSupervisionRequestConflict, request.RequestKey)
		}
		answer := receipt.Response
		answer.Replay = true
		return answer, nil
	}
	commit := SupervisionCommit{
		RunID:            request.RunID,
		Operation:        request.Operation,
		Actor:            actor,
		RequestKey:       request.RequestKey,
		PayloadDigest:    digest,
		Reason:           request.Reason,
		ExpectedRevision: request.ExpectedRevision,
		Now:              response.GeneratedAt,
		Request:          request,
	}
	switch request.Operation {
	case SupervisionDecide:
		err = s.decideGate(ctx, actor, state, request, &commit)
	case SupervisionHold:
		err = s.placeHold(ctx, actor, state, request, &commit)
	case SupervisionRelease:
		err = s.releaseHold(actor, state, request, &commit)
	case SupervisionEscalate, SupervisionResolve:
		err = s.decideIncident(actor, state, request, &commit)
	}
	if err != nil {
		return empty, err
	}
	receipt, err := store.CommitSupervision(ctx, commit)
	if err != nil {
		return empty, err
	}
	if receipt.Response.Version == "" {
		// A store that records the effect without composing an answer still owes
		// the caller one, and the computed commit is exactly that answer.
		response.GateID, response.GateState = commit.GateID, commit.GateState
		response.Decision, response.Hold = commit.Decision, commit.Hold
		response.IncidentID, response.IncidentState = commit.IncidentID, commit.IncidentState
		response.Resolution = commit.Resolution
		return response, nil
	}
	return receipt.Response, nil
}

// describeSupervisionRoute answers, on a show, whether an overseer could be
// dispatched for this run right now.
//
// A failure to read the fleet is reported as an unanswered question rather than
// as an available route, for the same reason the readiness matrix reports an
// unobserved repository as unobserved: a check nobody performed must never read
// as a check that passed.
func (s *Service) describeSupervisionRoute(ctx context.Context, state *SupervisionState) {
	workers, err := s.reader.LoadWorkerSnapshots(ctx)
	if err != nil {
		state.RouteBlockReason = "the worker inventory could not be read, so supervisor route availability is unknown"
		return
	}
	state.RouteAvailable = backlog.SupervisionRouteAvailable(backlog.SupervisionRouteRequest{
		Route: state.Record.Config.Route, Workers: workers,
		SupervisorClientConfigured: s.supervisorClientConfigured,
	})
	if !state.RouteAvailable {
		state.RouteBlockReason = backlog.SupervisionRouteBlockCause(s.supervisorClientConfigured)
	}
}

func (s *Service) decideGate(
	ctx context.Context,
	actor domain.Actor,
	state SupervisionState,
	request SupervisionRequest,
	commit *SupervisionCommit,
) error {
	_ = ctx
	view, found := findGate(state, request.Gate.GateID)
	if !found {
		return fmt.Errorf("%w: run %s has no gate %s", ErrNotFound, request.RunID, request.Gate.GateID)
	}
	if view.Gate.Revision != request.ExpectedRevision {
		return fmt.Errorf("%w: decision names gate revision %d, the gate is at %d",
			domain.ErrSupervisionStaleRevision, request.ExpectedRevision, view.Gate.Revision)
	}
	event := domain.GateEventAccept
	if request.Gate.Outcome == domain.GateDecisionReject {
		event = domain.GateEventReject
	}
	next, err := domain.GateTransition(domain.GateTransitionInput{
		Gate:                      view.Gate,
		Event:                     event,
		Actor:                     actor,
		ExpectedGraphRevision:     request.Gate.ExpectedGraphRevision,
		EvidenceMatches:           view.Gate.EvidenceSnapshotID == request.Gate.EvidenceSnapshotID,
		ProducersVerified:         view.ProducersVerified,
		SuccessorOfferedOrStarted: view.SuccessorOfferedOrStarted,
		SinkSettled:               state.SinkSettled,
	})
	if err != nil {
		return err
	}
	decision := domain.GateDecision{
		RunID:           request.RunID,
		GateID:          view.Gate.Definition.ID,
		Actor:           actor,
		ActivationEpoch: request.ActivationEpoch,
		GraphRevision:   request.Gate.ExpectedGraphRevision,
		Outcome:         request.Gate.Outcome,
		RequestID:       request.RequestKey,
		Reason:          request.Reason,
		IncidentID:      request.Gate.IncidentID,
		DecidedAt:       commit.Now,
	}
	if view.Evidence != nil {
		decision.Evidence = *view.Evidence
	}
	commit.GateID, commit.GateState, commit.Decision = view.Gate.Definition.ID, next, &decision
	if request.Gate.IncidentID == "" {
		return nil
	}
	incident, found := findIncident(state, request.Gate.IncidentID)
	if !found {
		return fmt.Errorf("%w: run %s has no incident %s", ErrNotFound, request.RunID, request.Gate.IncidentID)
	}
	if request.Gate.Outcome != domain.GateDecisionAccept {
		return fmt.Errorf("%w: only an acceptance closes a review incident", domain.ErrSupervisionPrerequisite)
	}
	incidentState, err := domain.IncidentTransition(domain.IncidentTransitionInput{
		Incident:             incident.Incident,
		Event:                domain.IncidentEventGateAccepted,
		Actor:                actor,
		ActorScopeCoversRun:  true,
		MatchingGateIncident: incident.Incident.GateID == view.Gate.Definition.ID,
		ExpectedRevision:     incident.Incident.Revision,
	})
	if err != nil {
		return err
	}
	commit.IncidentID, commit.IncidentState = incident.Incident.ID, incidentState
	return nil
}

func (s *Service) placeHold(
	ctx context.Context,
	actor domain.Actor,
	state SupervisionState,
	request SupervisionRequest,
	commit *SupervisionCommit,
) error {
	if state.Record.Revision != request.ExpectedRevision {
		return fmt.Errorf("%w: hold names record revision %d, the record is at %d",
			domain.ErrSupervisionStaleRevision, request.ExpectedRevision, state.Record.Revision)
	}
	// The hold takes its identity from the request key. One key is one hold,
	// so a retried placement is the same hold rather than a second one, and a
	// store that refuses a duplicate hold id refuses exactly what the replay
	// receipt would have replayed.
	hold := domain.Hold{
		ID:        request.RequestKey,
		RunID:     request.RunID,
		Scope:     request.Hold.Scope,
		Owner:     actor,
		Reason:    request.Reason,
		CreatedAt: commit.Now,
	}
	branchExists, closureRecomputed := false, false
	if request.Hold.Scope.Kind == domain.HoldScopeBranch {
		store, ok := s.supervisionStore()
		if !ok {
			return fmt.Errorf("%w: this coordinator store resolves no branch", ErrSupervisionUnavailable)
		}
		branch, err := store.ResolveSupervisionBranch(ctx, request.RunID, request.Hold.Scope.BranchRootTaskID)
		if err != nil {
			return err
		}
		branchExists, closureRecomputed = branch.Exists, true
		hold.GraphRevision, hold.ResolvedTaskIDs = branch.GraphRevision, branch.ResolvedTaskIDs
	}
	next, err := domain.HoldTransition(domain.HoldTransitionInput{
		Hold:  hold,
		Event: domain.HoldEventPlace,
		Actor: actor,
		// Authorization has already established that this actor's capability
		// covers this run; the domain guard is told what the authorizer
		// decided rather than re-deciding it.
		ActorScopeCoversRun: true,
		BranchRootExists:    branchExists,
		ClosureRecomputed:   closureRecomputed,
	})
	if err != nil {
		return err
	}
	hold.State = next
	commit.Hold, commit.HoldState = &hold, next
	return nil
}

func (s *Service) releaseHold(
	actor domain.Actor,
	state SupervisionState,
	request SupervisionRequest,
	commit *SupervisionCommit,
) error {
	if state.Record.Revision != request.ExpectedRevision {
		return fmt.Errorf("%w: release names record revision %d, the record is at %d",
			domain.ErrSupervisionStaleRevision, request.ExpectedRevision, state.Record.Revision)
	}
	var hold domain.Hold
	found := false
	for _, candidate := range state.Holds {
		if candidate.ID == request.Release.HoldID {
			hold, found = candidate, true
			break
		}
	}
	if !found {
		return fmt.Errorf("%w: run %s has no hold %s", ErrNotFound, request.RunID, request.Release.HoldID)
	}
	next, err := domain.HoldTransition(domain.HoldTransitionInput{
		Hold:                hold,
		Event:               domain.HoldEventRelease,
		Actor:               actor,
		ActorScopeCoversRun: true,
	})
	if err != nil {
		return err
	}
	hold.State, hold.ReleasedAt = next, &commit.Now
	commit.Hold, commit.HoldID, commit.HoldState = &hold, hold.ID, next
	return nil
}

func (s *Service) decideIncident(
	actor domain.Actor,
	state SupervisionState,
	request SupervisionRequest,
	commit *SupervisionCommit,
) error {
	if len(request.Incident.IncidentIDs) > 0 {
		_, err := domain.IncidentTransition(domain.IncidentTransitionInput{
			Incident: domain.ReviewIncident{State: domain.IncidentOpen},
			Event:    domain.IncidentEventBulkClose,
			Actor:    actor,
		})
		if err == nil {
			err = fmt.Errorf("%w: v1 resolves one named incident at a time", domain.ErrSupervisionPrerequisite)
		}
		return err
	}
	view, found := findIncident(state, request.Incident.IncidentID)
	if !found {
		return fmt.Errorf("%w: run %s has no incident %s", ErrNotFound, request.RunID, request.Incident.IncidentID)
	}
	if view.Incident.Revision != request.ExpectedRevision {
		return fmt.Errorf("%w: request names incident revision %d, the incident is at %d",
			domain.ErrSupervisionStaleRevision, request.ExpectedRevision, view.Incident.Revision)
	}
	event := domain.IncidentEventEscalate
	if request.Operation == SupervisionResolve {
		switch request.Incident.Outcome {
		case domain.IncidentOutcomeConcludeFailure:
			event = domain.IncidentEventConcludeFailure
		case domain.IncidentOutcomeRemediated:
			event = domain.IncidentEventOperatorResolve
		case domain.IncidentOutcomeCancelled:
			event = domain.IncidentEventOperatorCancel
		}
	}
	next, err := domain.IncidentTransition(domain.IncidentTransitionInput{
		Incident:             view.Incident,
		Event:                event,
		Actor:                actor,
		ActorScopeCoversRun:  true,
		TaskTerminallyFailed: view.SourceTaskTerminallyFailed,
		ExpectedRevision:     request.ExpectedRevision,
	})
	if err != nil {
		return err
	}
	commit.IncidentID, commit.IncidentState = view.Incident.ID, next
	if request.Operation != SupervisionResolve {
		return nil
	}
	commit.Resolution = &domain.ResolutionReceipt{
		Actor:            actor,
		Outcome:          request.Incident.Outcome,
		ExpectedRevision: request.ExpectedRevision,
		RequestID:        request.RequestKey,
		Reason:           request.Reason,
		ResolvedAt:       commit.Now,
	}
	return nil
}

// supervise answers one supervision operation on either carrier. It reaches
// the service through an optional interface, the same way graph amendment and
// native waits do, so a service that offers no supervision answers that it
// does not rather than failing to compile.
//
// The operation word and the request's own operation must agree. Otherwise a
// key pinned to supervision-show could carry a decide request and the pin
// would mean nothing.
func (d adminDispatch) supervise(ctx context.Context, principal Principal, request localRequest, response *localResponse) {
	handler, ok := d.service.(interface {
		Supervise(context.Context, Principal, SupervisionRequest) (SupervisionResponse, error)
	})
	if !ok {
		// A service that carries no supervision is a deployment fact, not a
		// malformed request, and reporting it as one sent an operator looking at
		// a request that was correct. This branch was the whole of defect 3: the
		// coordinator's local service simply did not forward Supervise.
		response.Error = "this coordinator does not carry supervision"
		response.SupervisionClass = SupervisionErrorUnavailable
		return
	}
	if request.Supervision == nil || request.GraphAmendment != nil || request.NodeWait != nil ||
		request.WorkerEnrollment != nil || request.Query != nil || request.Mutation != nil ||
		request.ArtifactID != "" || request.Submission != nil || request.SubmissionSize != 0 ||
		request.ScheduleDefinition != nil || request.UnknownRecovery != nil ||
		request.QuarantineRelease != nil {
		response.Error = "malformed supervision request"
		response.SupervisionClass = SupervisionErrorMalformed
		return
	}
	decision := request.Supervision.Operation.Mutating()
	if decision != (request.Operation == localOperationSupervisionDecision) {
		response.Error = "supervision operation word does not match the request"
		response.SupervisionClass = SupervisionErrorMalformed
		return
	}
	value, err := handler.Supervise(ctx, principal, *request.Supervision)
	if err != nil {
		response.Error = err.Error()
		response.SupervisionClass = ClassifySupervisionError(err)
		return
	}
	response.SupervisionResponse = &value
}

// SupervisorScope is the run and activation epoch the coordinator currently
// considers valid for one supervisor principal, plus the actions that
// principal may perform.
//
// It is read from the coordinator's own supervision record, never from the
// request and never from the credential. That is the whole of the run-and-epoch
// binding: the credential proves which client is calling, and the coordinator
// decides what that client may touch right now.
type SupervisorScope struct {
	RunID           string      `json:"runId"`
	ActivationEpoch int64       `json:"activationEpoch"`
	Actions         []QueryKind `json:"actions,omitempty"`
}

// SupervisorActions is the allowlist a supervisor capability gets when its
// scope declares none.
//
// Every entry is either a supervision operation or a read of the run the
// capability is bound to. The absences are the point, and each is deliberate:
// no command kind at all, so nothing can pause, retry, skip, cancel or start a
// task, and in particular nothing can route to backlog start, which bypasses
// quota admission; no QueryWorkflows, QuerySchedules, QueryWorkers, QueryQuota
// or QueryCommands, because those are fleet-wide views that would leak other
// runs; no worker enrollment, no graph amendment, no submission, no schedule
// definition and no quarantine release.
func SupervisorActions() []QueryKind {
	return []QueryKind{
		SupervisionShowKind, SupervisionDecisionKind,
		QueryWorkflow, QueryGraph, QueryDiagnose, QueryTask, QueryExplanation,
		QueryEvents, QueryArtifacts, QueryArtifact,
	}
}

// SupervisorScopeSource answers what a supervisor principal may act on right
// now, and which run an artifact belongs to.
//
// The artifact question exists because an artifact read is authorized by
// artifact ID alone. Without resolving the artifact to its run, a capability
// bound to one run could read another run's outputs through an admin API it
// was never meant to reach.
type SupervisorScopeSource interface {
	SupervisorScope(ctx context.Context, principal string) (SupervisorScope, error)
	ArtifactRun(ctx context.Context, artifactID string) (string, error)
}

// SupervisorAuthorizer enforces the run-and-epoch scope of a supervisor
// capability server-side, and delegates every other principal unchanged.
//
// Server-side is the only place the scope can be enforced honestly. A
// credential reference maps to a client name and a fixed role; it has no run,
// no epoch and no expiry. The coordinator, on the other hand, already knows
// which activation is valid for which run, so a capability that can act only
// on the run it was woken for is a property of this authorizer rather than of
// the secret. See docs/backlog-v2-operations.md for what that does and does
// not contain.
// relayedPrincipalPrefix marks a principal the coordinator accepted through a
// relay rather than from its own socket peer.
const relayedPrincipalPrefix = "remote:"

// RelayedPrincipalID is the identity a relayed admin client is known by inside
// the coordinator.
//
// Both carriers reach the coordinator through the same local server, which
// rewrites the claimed principal to this form, and the supervisor authorizer
// resolves scope under it. Anything that has to match that identity from the
// outside -- above all the principal recorded on an activation, which is what
// binds a supervisor credential to one run and one epoch -- must derive it here
// rather than spell the prefix again. The two spellings drifting apart is not a
// compile error: it is a supervisor that is silently refused every operation.
func RelayedPrincipalID(client string) string {
	if client == "" {
		return ""
	}
	if strings.HasPrefix(client, relayedPrincipalPrefix) {
		return client
	}
	return relayedPrincipalPrefix + client
}

type SupervisorAuthorizer struct {
	Scope    SupervisorScopeSource
	Delegate Authorizer
}

func (a SupervisorAuthorizer) Authorize(ctx context.Context, principal Principal, action Action) error {
	supervisor := false
	for _, role := range principal.Roles {
		if role == SupervisorRole {
			supervisor = true
			break
		}
	}
	if !supervisor {
		if a.Delegate == nil {
			return fmt.Errorf("%w: no authorizer is configured for %q",
				domain.ErrSupervisionUnauthorizedActor, principal.ID)
		}
		return a.Delegate.Authorize(ctx, principal, action)
	}
	// A second role would widen the capability simply by being carried beside
	// it, which is the one way a narrow role stops being narrow.
	if len(principal.Roles) != 1 {
		return fmt.Errorf("%w: a supervisor capability carries no second role",
			domain.ErrSupervisionUnauthorizedActor)
	}
	if a.Scope == nil {
		return fmt.Errorf("%w: no supervision scope source is configured", ErrSupervisionUnavailable)
	}
	scope, err := a.Scope.SupervisorScope(ctx, principal.ID)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrSupervisionUnavailable, err)
	}
	if strings.TrimSpace(scope.RunID) == "" {
		return fmt.Errorf("%w: %q holds no live supervision capability",
			domain.ErrSupervisionUnauthorizedActor, principal.ID)
	}
	// A command kind is a coordinator control: pause, resume, retry, skip,
	// cancel, start. A supervisor has none of them, so the refusal is by
	// category rather than by allowlist entry.
	if action.CommandKind != "" {
		return fmt.Errorf("%w: a supervisor may not issue the %q command",
			domain.ErrSupervisionUnauthorizedActor, action.CommandKind)
	}
	allowed := scope.Actions
	if len(allowed) == 0 {
		allowed = SupervisorActions()
	}
	permitted := false
	for _, candidate := range allowed {
		if candidate == action.Kind {
			permitted = true
			break
		}
	}
	if !permitted {
		return fmt.Errorf("%w: a supervisor may not perform %q",
			domain.ErrSupervisionUnauthorizedActor, action.Kind)
	}
	if err := a.authorizeRun(ctx, scope, action); err != nil {
		return err
	}
	return authorizeSupervisorEpoch(scope, action)
}

// authorizeRun refuses anything outside the one run the capability is bound
// to, including a request that names no run at all: a run-less read of a
// fleet-wide view is exactly the cross-run access this capability must not
// inherit.
func (a SupervisorAuthorizer) authorizeRun(ctx context.Context, scope SupervisorScope, action Action) error {
	runID := action.WorkflowRunID
	// An artifact carries its own run, and that run is the one that decides
	// access. Resolving it only when the request named no run would let a
	// supervisor name its own run and any other run's artifact in the same
	// request, because the handler reads the artifact by ID and ignores the run
	// entirely. So the artifact's owning run is checked against the scope
	// whenever an artifact is named, in addition to the named run.
	if action.ArtifactID != "" {
		resolved, err := a.Scope.ArtifactRun(ctx, action.ArtifactID)
		if err != nil {
			return fmt.Errorf("%w: %s", ErrSupervisionUnavailable, err)
		}
		if resolved == "" {
			return fmt.Errorf("%w: artifact %s belongs to no run this capability can name",
				domain.ErrSupervisionUnauthorizedActor, action.ArtifactID)
		}
		if resolved != scope.RunID {
			return fmt.Errorf("%w: this capability is bound to run %s and artifact %s belongs to %s",
				domain.ErrSupervisionUnauthorizedActor, scope.RunID, action.ArtifactID, resolved)
		}
		if runID == "" {
			runID = resolved
		}
	}
	if runID == "" {
		return fmt.Errorf("%w: a supervisor acts on one named run, and %q names none",
			domain.ErrSupervisionUnauthorizedActor, action.Kind)
	}
	if runID != scope.RunID {
		return fmt.Errorf("%w: this capability is bound to run %s and not to %s",
			domain.ErrSupervisionUnauthorizedActor, scope.RunID, runID)
	}
	return nil
}

// authorizeSupervisorEpoch refuses a decision named under an epoch that is no
// longer the valid one. A read may name none; a decision must name the current
// one, which is what makes a stolen credential useless between activations and
// an operator takeover final.
func authorizeSupervisorEpoch(scope SupervisorScope, action Action) error {
	if action.Kind == SupervisionDecisionKind && action.ActivationEpoch == 0 {
		return fmt.Errorf("%w: a supervision decision names the activation epoch it acts under",
			domain.ErrSupervisionUnauthorizedActor)
	}
	if action.ActivationEpoch == 0 {
		return nil
	}
	if action.ActivationEpoch != scope.ActivationEpoch {
		return fmt.Errorf("%w: this capability is valid at epoch %d, the request names %d",
			domain.ErrSupervisionStaleRevision, scope.ActivationEpoch, action.ActivationEpoch)
	}
	return nil
}

func findGate(state SupervisionState, gateID string) (SupervisionGateView, bool) {
	for _, candidate := range state.Gates {
		if candidate.Gate.Definition.ID == gateID {
			return candidate, true
		}
	}
	return SupervisionGateView{}, false
}

func findIncident(state SupervisionState, incidentID string) (SupervisionIncidentView, bool) {
	for _, candidate := range state.Incidents {
		if candidate.Incident.ID == incidentID {
			return candidate, true
		}
	}
	return SupervisionIncidentView{}, false
}
