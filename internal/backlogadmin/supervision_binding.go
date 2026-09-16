package backlogadmin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// CoordinatorSupervisionStore binds this package's one-seam supervision store
// to the coordinator store's five decision transactions.
//
// The seam is one method because authorization and routing need one place to
// stand, not five. The store is five transactions because each verb fences a
// different record and releases a different set of narrowed offers. This type is
// the only place those two shapes meet: it switches on the operation and hands
// the whole structured request to the transaction that verb owns.
//
// It decides nothing. Every guard runs twice by design, once in the service
// before a transaction is opened and once in the store against rows it read
// there, and the store's answer wins.
type CoordinatorSupervisionStore struct {
	Store *sqlite.Store
}

// LoadSupervision reads one run's whole supervision state.
func (c CoordinatorSupervisionStore) LoadSupervision(ctx context.Context, runID string) (SupervisionState, error) {
	facts, err := c.Store.LoadSupervisionAdminState(ctx, runID)
	if err != nil {
		return SupervisionState{}, supervisionNotFound(err, runID)
	}
	state := SupervisionState{
		Record:      facts.Record,
		Activation:  facts.Activation,
		Holds:       facts.Holds,
		SinkSettled: facts.SinkSettled,
	}
	for _, gate := range facts.Gates {
		state.Gates = append(state.Gates, SupervisionGateView{
			Gate:                      gate.Gate,
			ProducersVerified:         gate.ProducersVerified,
			SuccessorOfferedOrStarted: gate.SuccessorOfferedOrStarted,
			Evidence:                  gate.Evidence,
		})
	}
	for _, incident := range facts.Incidents {
		state.Incidents = append(state.Incidents, SupervisionIncidentView{
			Incident:                   incident.Incident,
			SourceTaskTerminallyFailed: incident.SourceTaskTerminallyFailed,
		})
	}
	return state, nil
}

// ResolveSupervisionBranch computes a branch root's downstream closure.
func (c CoordinatorSupervisionStore) ResolveSupervisionBranch(ctx context.Context, runID, rootTaskID string) (SupervisionBranch, error) {
	exists, revision, resolved, err := c.Store.SupervisionBranchClosure(ctx, runID, rootTaskID)
	if err != nil {
		return SupervisionBranch{}, supervisionNotFound(err, runID)
	}
	return SupervisionBranch{Exists: exists, GraphRevision: revision, ResolvedTaskIDs: resolved}, nil
}

// SupervisionReplay returns the receipt an earlier request key produced.
func (c CoordinatorSupervisionStore) SupervisionReplay(ctx context.Context, runID, requestKey string) (SupervisionReceipt, bool, error) {
	if requestKey == "" {
		return SupervisionReceipt{}, false, nil
	}
	answer, digest, found, err := c.Store.LookupSupervisionReceipt(ctx, runID, requestKey)
	if err != nil || !found {
		return SupervisionReceipt{}, false, err
	}
	receipt := SupervisionReceipt{PayloadDigest: digest}
	if err := json.Unmarshal(answer, &receipt.Response); err != nil {
		return SupervisionReceipt{}, false, fmt.Errorf("decode supervision receipt %q: %w", requestKey, err)
	}
	return receipt, true, nil
}

// CommitSupervision applies one decided outcome and records its receipt.
func (c CoordinatorSupervisionStore) CommitSupervision(ctx context.Context, commit SupervisionCommit) (SupervisionReceipt, error) {
	request := commit.Request
	response := SupervisionResponse{
		Version:     SupervisionVersion,
		Operation:   commit.Operation,
		RunID:       commit.RunID,
		GeneratedAt: commit.Now,
		Actor:       commit.Actor,
	}
	var decision sqlite.SupervisionDecision
	var err error
	switch commit.Operation {
	case SupervisionDecide:
		decision, err = c.decide(ctx, commit, request)
	case SupervisionHold:
		// A branch hold's closure was resolved against one graph revision in an
		// earlier transaction. Naming it here is what makes the claim in the
		// comment on SupervisionBranch true: an amendment that landed in between
		// refuses the placement instead of committing a closure computed against
		// a graph that has moved. A run-wide hold resolves no closure and carries
		// revision zero, which does not fence.
		expectedGraphRevision := int64(0)
		if commit.Hold != nil {
			expectedGraphRevision = commit.Hold.GraphRevision
		}
		decision, err = c.Store.PlaceHold(ctx, sqlite.HoldRequest{
			RunID: commit.RunID, HoldID: commit.RequestKey, RequestID: commit.RequestKey,
			Actor: commit.Actor, Scope: request.Hold.Scope, Reason: commit.Reason, PlacedAt: commit.Now,
			ExpectedGraphRevision: expectedGraphRevision,
		})
	case SupervisionRelease:
		decision, err = c.Store.ReleaseHold(ctx, sqlite.HoldReleaseRequest{
			RunID: commit.RunID, HoldID: request.Release.HoldID, RequestID: commit.RequestKey,
			Actor: commit.Actor, Reason: commit.Reason, ReleasedAt: commit.Now,
		})
	case SupervisionEscalate, SupervisionResolve:
		decision, err = c.resolveIncident(ctx, commit, request)
	default:
		return SupervisionReceipt{}, fmt.Errorf("%w: supervision operation %q commits nothing", ErrInvalidQuery, commit.Operation)
	}
	if err != nil {
		return SupervisionReceipt{}, supervisionNotFound(err, commit.RunID)
	}
	response.Replay = decision.Replay
	if decision.Gate != nil {
		response.GateID, response.GateState = decision.Gate.Definition.ID, decision.Gate.State
	}
	response.Decision = decision.GateDecision
	response.Hold = decision.Hold
	if decision.Incident != nil {
		response.IncidentID, response.IncidentState = decision.Incident.ID, decision.Incident.State
		response.Resolution = decision.Incident.Resolution
	}
	receipt := SupervisionReceipt{PayloadDigest: commit.PayloadDigest, Response: response}
	if err := c.Store.RecordSupervisionReceipt(ctx, commit.RunID, commit.RequestKey, commit.PayloadDigest, response); err != nil {
		return SupervisionReceipt{}, err
	}
	return receipt, nil
}

// decide binds the evidence a gate decision names. The request carries the
// snapshot's identity rather than its contents, so the binding reads the
// contents the coordinator currently holds and refuses when the identity no
// longer matches: that refusal is the retried-producer case, by name.
func (c CoordinatorSupervisionStore) decide(
	ctx context.Context,
	commit SupervisionCommit,
	request SupervisionRequest,
) (sqlite.SupervisionDecision, error) {
	state, err := c.LoadSupervision(ctx, commit.RunID)
	if err != nil {
		return sqlite.SupervisionDecision{}, err
	}
	view, found := findGate(state, request.Gate.GateID)
	if !found {
		return sqlite.SupervisionDecision{}, fmt.Errorf("%w: run %s has no gate %s", ErrNotFound, commit.RunID, request.Gate.GateID)
	}
	if view.Evidence == nil || view.Evidence.ID != request.Gate.EvidenceSnapshotID {
		return sqlite.SupervisionDecision{}, fmt.Errorf(
			"%w: the decision's evidence snapshot is not the one the gate now holds",
			domain.ErrSupervisionStaleRevision)
	}
	return c.Store.DecideGate(ctx, sqlite.GateDecisionRequest{
		RunID:                 commit.RunID,
		GateID:                request.Gate.GateID,
		RequestID:             commit.RequestKey,
		Actor:                 commit.Actor,
		ExpectedGraphRevision: request.Gate.ExpectedGraphRevision,
		ExpectedGateRevision:  commit.ExpectedRevision,
		Evidence:              *view.Evidence,
		Outcome:               request.Gate.Outcome,
		Reason:                commit.Reason,
		IncidentID:            request.Gate.IncidentID,
		DecidedAt:             commit.Now,
	})
}

func (c CoordinatorSupervisionStore) resolveIncident(
	ctx context.Context,
	commit SupervisionCommit,
	request SupervisionRequest,
) (sqlite.SupervisionDecision, error) {
	state, err := c.LoadSupervision(ctx, commit.RunID)
	if err != nil {
		return sqlite.SupervisionDecision{}, err
	}
	view, found := findIncident(state, request.Incident.IncidentID)
	if !found {
		return sqlite.SupervisionDecision{}, fmt.Errorf("%w: run %s has no incident %s",
			ErrNotFound, commit.RunID, request.Incident.IncidentID)
	}
	event := domain.IncidentEventEscalate
	if commit.Operation == SupervisionResolve {
		switch request.Incident.Outcome {
		case domain.IncidentOutcomeConcludeFailure:
			event = domain.IncidentEventConcludeFailure
		case domain.IncidentOutcomeCancelled:
			event = domain.IncidentEventOperatorCancel
		default:
			event = domain.IncidentEventOperatorResolve
		}
	}
	return c.Store.ResolveReviewIncident(ctx, sqlite.IncidentResolutionRequest{
		RunID:      commit.RunID,
		IncidentID: request.Incident.IncidentID,
		RequestID:  commit.RequestKey,
		Actor:      commit.Actor,
		Event:      event,
		// The gate a resolution matches by is the incident's own gate. Naming it
		// here rather than testing that the incident has any gate at all is what
		// stops one gate's acceptance closing another gate's review.
		GateID:           view.Incident.GateID,
		ExpectedRevision: commit.ExpectedRevision,
		Outcome:          request.Incident.Outcome,
		Reason:           commit.Reason,
		ResolvedAt:       commit.Now,
	})
}

// SupervisorScope answers what one supervisor principal may act on right now.
// It is read from the coordinator's own activation records: the credential
// proves which client is calling, and the coordinator decides what that client
// may touch.
func (c CoordinatorSupervisionStore) SupervisorScope(ctx context.Context, principal string) (SupervisorScope, error) {
	runID, epoch, err := c.Store.SupervisorScopeForPrincipal(ctx, principal)
	if err != nil {
		return SupervisorScope{}, err
	}
	return SupervisorScope{RunID: runID, ActivationEpoch: epoch}, nil
}

// ArtifactRun resolves an artifact to the run it belongs to, so a capability
// bound to one run cannot read another run's outputs by artifact ID.
func (c CoordinatorSupervisionStore) ArtifactRun(ctx context.Context, artifactID string) (string, error) {
	return c.Store.ArtifactRun(ctx, artifactID)
}

// supervisionNotFound maps the store's absent-record sentinel onto this
// package's, so the service and the CLI classify it as an unmet prerequisite
// rather than as something to retry.
func supervisionNotFound(err error, runID string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sqlite.ErrSupervisionRecordNotFound) {
		return fmt.Errorf("%w: run %s records no supervision", ErrNotFound, runID)
	}
	return err
}

var (
	_ SupervisionStore      = CoordinatorSupervisionStore{}
	_ SupervisorScopeSource = CoordinatorSupervisionStore{}
)
