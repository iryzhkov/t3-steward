package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The read side the admin supervision operation needs: one run's whole
// supervision picture, plus the per-gate and per-incident facts the pure state
// machines cannot derive from a record alone.
//
// The types are declared in internal/backlogadmin, which owns authorization and
// must not depend on a schema, so this file answers in plain values and that
// package's binding assembles its own view. That keeps the dependency pointing
// one way and still gives the service exactly what it asks for.

// SupervisionGateFacts is one gate plus the transaction-time facts a decision
// turns on.
type SupervisionGateFacts struct {
	Gate                      domain.Gate
	ProducersVerified         bool
	SuccessorOfferedOrStarted bool
	Evidence                  *domain.EvidenceSnapshot
	// LastDecision is the most recent decision recorded against this gate, or
	// nil when none was. It is here because the gate itself records only what it
	// became, and the question an operator asks about a decided gate is who
	// decided it: an overseer acting as itself and an operator acting for it
	// leave the same accepted gate behind.
	LastDecision *domain.GateDecision
}

// SupervisionIncidentFacts is one incident plus the one fact conclude-failure
// turns on.
type SupervisionIncidentFacts struct {
	Incident                   domain.ReviewIncident
	SourceTaskTerminallyFailed bool
}

// SupervisionAdminState is one run's whole supervision state for the admin
// operation.
type SupervisionAdminState struct {
	Record      domain.SupervisionRecord
	Activation  domain.Activation
	Gates       []SupervisionGateFacts
	Holds       []domain.Hold
	Incidents   []SupervisionIncidentFacts
	SinkSettled bool
}

// LoadSupervisionAdminState reads one run's supervision state. A run with no
// supervision record is reported with ErrSupervisionRecordNotFound, because
// absence of the record is the unsupervised case and not an empty one.
func (s *Store) LoadSupervisionAdminState(ctx context.Context, runID string) (SupervisionAdminState, error) {
	var state SupervisionAdminState
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return state, err
	}
	defer tx.Rollback()
	contextState, supervised, err := supervisionContextTx(ctx, tx, runID, false)
	if err != nil {
		return state, err
	}
	if !supervised {
		return state, fmt.Errorf("%w: run %q is not supervised", ErrSupervisionRecordNotFound, runID)
	}
	state.Record = contextState.Record
	state.SinkSettled = contextState.Snapshot.RunTerminal
	activations, err := loadSupervisionActivationsTx(ctx, tx, runID)
	if err != nil {
		return SupervisionAdminState{}, err
	}
	for _, candidate := range activations {
		if candidate.Epoch == state.Record.ActivationEpoch {
			state.Activation = candidate
		}
	}
	decisions, err := loadSupervisionDecisionsTx(ctx, tx, runID)
	if err != nil {
		return SupervisionAdminState{}, err
	}
	lastDecision := make(map[string]domain.GateDecision, len(decisions))
	for _, decision := range decisions {
		current, seen := lastDecision[decision.GateID]
		if !seen || !decision.DecidedAt.Before(current.DecidedAt) {
			lastDecision[decision.GateID] = decision
		}
	}
	for _, gate := range contextState.Snapshot.Gates {
		evidence, verified, err := supervisionGateEvidenceTx(ctx, tx, runID, gate)
		if err != nil {
			return SupervisionAdminState{}, err
		}
		live, err := supervisionSuccessorLiveTx(ctx, tx, runID, gate)
		if err != nil {
			return SupervisionAdminState{}, err
		}
		facts := SupervisionGateFacts{Gate: gate, ProducersVerified: verified, SuccessorOfferedOrStarted: live}
		if verified {
			bound := evidence
			facts.Evidence = &bound
		}
		if decision, decided := lastDecision[gate.Definition.ID]; decided {
			bound := decision
			facts.LastDecision = &bound
		}
		state.Gates = append(state.Gates, facts)
	}
	if state.Holds, err = loadSupervisionHoldsTx(ctx, tx, runID, ""); err != nil {
		return SupervisionAdminState{}, err
	}
	incidents, err := loadSupervisionIncidentsTx(ctx, tx, runID)
	if err != nil {
		return SupervisionAdminState{}, err
	}
	for _, incident := range incidents {
		failed, err := supervisionSourceTerminallyFailedTx(ctx, tx, incident)
		if err != nil {
			return SupervisionAdminState{}, err
		}
		state.Incidents = append(state.Incidents, SupervisionIncidentFacts{
			Incident: incident, SourceTaskTerminallyFailed: failed,
		})
	}
	return state, tx.Commit()
}

// SupervisionBranchClosure resolves a branch root, named by task ID or by the
// task name the manifest declared, to its downstream dependency closure at the
// current graph revision.
func (s *Store) SupervisionBranchClosure(ctx context.Context, runID, rootTaskID string) (bool, int64, []string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, 0, nil, err
	}
	defer tx.Rollback()
	state, supervised, err := supervisionContextTx(ctx, tx, runID, true)
	if err != nil {
		return false, 0, nil, err
	}
	if !supervised {
		return false, 0, nil, fmt.Errorf("%w: run %q is not supervised", ErrSupervisionRecordNotFound, runID)
	}
	resolved, exists := supervisionBranchClosure(state.Tasks, rootTaskID)
	return exists, state.Run.GraphRevision, resolved, tx.Commit()
}

// SupervisorScopeForPrincipal answers which run and activation epoch one
// supervisor principal may act on right now.
//
// The answer comes from the activation the coordinator itself dispatched to
// that principal, and only while that activation is live. Between activations a
// stolen credential resolves to no run at all, and an operator takeover raises
// the epoch, so a decision the replaced supervisor had already formed arrives
// naming an epoch that is no longer valid.
func (s *Store) SupervisorScopeForPrincipal(ctx context.Context, principal string) (string, int64, error) {
	if principal == "" {
		return "", 0, nil
	}
	// Pending-dispatch is included so that a worker which has been handed the
	// activation can read the run while its first turn starts. Both states still
	// require a live lease: a lease that expired revokes decision authority at
	// the moment it expired, and scope that outlived it would let the read half
	// of the capability survive the write half.
	rows, err := s.db.QueryContext(ctx, `
		SELECT run_id, epoch, record FROM coordinator_supervision_activations
		WHERE state IN (?, ?) ORDER BY epoch DESC`,
		string(domain.ActivationActive), string(domain.ActivationPendingDispatch))
	if err != nil {
		return "", 0, fmt.Errorf("load live supervision activations: %w", err)
	}
	defer rows.Close()
	now := s.now().UTC()
	var scopedRun string
	var scopedEpoch int64
	for rows.Next() {
		var runID string
		var epoch int64
		var raw []byte
		if err := rows.Scan(&runID, &epoch, &raw); err != nil {
			return "", 0, err
		}
		var activation domain.Activation
		if err := json.Unmarshal(raw, &activation); err != nil {
			return "", 0, err
		}
		if activation.Principal != principal ||
			!domain.ActivationLeaseLive(activation, now) ||
			domain.ActivationPastDeadline(activation, now) {
			continue
		}
		if scopedRun != "" && scopedRun != runID {
			// One principal live on two runs is a configuration this coordinator
			// cannot resolve: picking the higher epoch would silently decide
			// which run a credential may act on. Refusing both is the answer
			// that cannot be wrong.
			return "", 0, fmt.Errorf(
				"supervisor %q holds live activations on runs %q and %q; a supervisor capability is bound to one run",
				principal, scopedRun, runID)
		}
		if epoch > scopedEpoch {
			scopedRun, scopedEpoch = runID, epoch
		}
	}
	if err := rows.Err(); err != nil {
		return "", 0, err
	}
	return scopedRun, scopedEpoch, nil
}

// ArtifactRun resolves an artifact to the run that owns it.
func (s *Store) ArtifactRun(ctx context.Context, artifactID string) (string, error) {
	var runID string
	err := s.db.QueryRowContext(ctx,
		"SELECT workflow_run_id FROM coordinator_artifacts WHERE id = ?", artifactID).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("%w: artifact %q", ErrSupervisionRecordNotFound, artifactID)
	}
	if err != nil {
		return "", fmt.Errorf("resolve artifact %q to its run: %w", artifactID, err)
	}
	return runID, nil
}

// LookupSupervisionReceipt returns the answer an earlier request key produced on
// the admin operation, so a replay is answered from the transaction that
// produced it rather than recomputed.
func (s *Store) LookupSupervisionReceipt(ctx context.Context, runID, requestKey string) ([]byte, string, bool, error) {
	var digest string
	var answer []byte
	err := s.db.QueryRowContext(ctx,
		"SELECT payload_sha256, answer FROM coordinator_supervision_receipts WHERE request_id = ? AND run_id = ?",
		"admin:"+requestKey, runID).Scan(&digest, &answer)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, "", false, nil
	}
	if err != nil {
		return nil, "", false, fmt.Errorf("load supervision receipt %q: %w", requestKey, err)
	}
	return answer, digest, true, nil
}

// RecordSupervisionReceipt stores the answer of one admin request key beside the
// effect it produced.
func (s *Store) RecordSupervisionReceipt(ctx context.Context, runID, requestKey, digest string, answer any) error {
	raw, err := json.Marshal(answer)
	if err != nil {
		return fmt.Errorf("encode supervision receipt %q: %w", requestKey, err)
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO coordinator_supervision_receipts(request_id, operation, run_id, payload_sha256, answer, recorded_at)
		VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(request_id) DO NOTHING`,
		"admin:"+requestKey, "supervision-admin", runID, digest, raw, s.now().UTC().Format("2006-01-02T15:04:05.999999999Z07:00"))
	if err != nil {
		return fmt.Errorf("record supervision receipt %q: %w", requestKey, err)
	}
	return nil
}
