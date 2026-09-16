package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The supervision decision transactions. Each one is revision-fenced against
// the record it changes, idempotent by request key, records its actor, and, in
// the same transaction, releases the outstanding offers whose readiness it
// narrowed. The guards are evaluated here, from rows read inside the
// transaction, and handed to the pure state machine in internal/domain.

// SupervisionMaterialization is the declared supervision of one run as the
// manifest and the amendment path produce it: the record plus the gate set of
// the effective graph. It creates no state a decision owns, so re-applying it
// never clears a gate, a hold or an incident.
type SupervisionMaterialization struct {
	Record domain.SupervisionRecord
	Gates  []domain.Gate
}

// PutSupervision creates or updates a run's supervision record and gate set.
// Gates that already exist keep their state, evidence and revision: only their
// definition, which belongs to the graph, is refreshed.
func (s *Store) PutSupervision(ctx context.Context, materialization SupervisionMaterialization) (domain.SupervisionRecord, error) {
	record := materialization.Record
	if strings.TrimSpace(record.RunID) == "" {
		return domain.SupervisionRecord{}, errors.New("a supervision record needs a run")
	}
	if err := record.Config.Validate(); err != nil {
		return domain.SupervisionRecord{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.SupervisionRecord{}, fmt.Errorf("begin supervision materialization: %w", err)
	}
	defer tx.Rollback()
	now := s.now().UTC()
	current, found, err := loadSupervisionRecordTx(ctx, tx, record.RunID)
	if err != nil {
		return domain.SupervisionRecord{}, err
	}
	if found {
		// Counters, cursor and epoch are decision-owned state; materialization
		// refreshes the declared configuration only.
		next := current
		next.Config = record.Config
		next.Revision = current.Revision + 1
		next.UpdatedAt = now
		record = next
	} else {
		if record.ActivationEpoch < 1 {
			record.ActivationEpoch = 1
		}
		record.Revision = 1
		if record.CreatedAt.IsZero() {
			record.CreatedAt = now
		}
		record.UpdatedAt = now
	}
	if err := saveSupervisionRecordTx(ctx, tx, record); err != nil {
		return domain.SupervisionRecord{}, err
	}
	for _, gate := range materialization.Gates {
		if err := gate.Definition.Validate(); err != nil {
			return domain.SupervisionRecord{}, err
		}
		gate.RunID = record.RunID
		existing, err := loadSupervisionGateTx(ctx, tx, record.RunID, gate.Definition.ID)
		switch {
		case err == nil:
			state := existing
			state.Definition = gate.Definition
			state.Revision = existing.Revision + 1
			state.UpdatedAt = now
			gate = state
		case errors.Is(err, ErrSupervisionRecordNotFound):
			if gate.State == "" {
				gate.State = domain.GatePendingEvidence
			}
			// The graph revision is the caller's to state: a gate is part of the
			// effective graph, so it is at the graph's revision and not at the
			// supervision record's. A new run is at revision zero, which is a real
			// revision rather than a missing one.
			gate.Revision = 1
			gate.UpdatedAt = now
		default:
			return domain.SupervisionRecord{}, err
		}
		if err := saveSupervisionGateTx(ctx, tx, gate); err != nil {
			return domain.SupervisionRecord{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return domain.SupervisionRecord{}, fmt.Errorf("commit supervision materialization: %w", err)
	}
	return record, nil
}

// LoadSupervisionSnapshot returns the compact supervision state the readiness
// predicate consumes. The planner takes it into its plan input; the store
// rebuilds it inside the transaction it is about to commit.
func (s *Store) LoadSupervisionSnapshot(ctx context.Context, runID string) (domain.SupervisionSnapshot, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.SupervisionSnapshot{}, err
	}
	defer tx.Rollback()
	snapshot, err := supervisionStateTx(ctx, tx, runID)
	if err != nil {
		return domain.SupervisionSnapshot{}, err
	}
	return snapshot, tx.Commit()
}

// SupervisionReadSet returns the run-local supervision state the workflow
// projection fences on, or nil for an unsupervised run.
//
// The projection's read set includes supervision so that a decision landing
// mid-pass invalidates the settlement it would have changed. That only works if
// the caller reads the same set it will be fenced against, which is what this
// exists for.
func (s *Store) SupervisionReadSet(ctx context.Context, runID string) (*SupervisionReadSet, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	read, err := supervisionReadSetTx(ctx, tx, runID)
	if err != nil {
		return nil, err
	}
	return read, tx.Commit()
}

// LoadSupervisionProjection returns the full supervision read set plus its
// append-only decision history, for explain and status.
func (s *Store) LoadSupervisionProjection(ctx context.Context, runID string) (SupervisionProjection, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SupervisionProjection{}, err
	}
	defer tx.Rollback()
	var projection SupervisionProjection
	read, err := supervisionReadSetTx(ctx, tx, runID)
	if err != nil {
		return SupervisionProjection{}, err
	}
	if read != nil {
		projection.SupervisionReadSet = *read
	}
	if projection.Decisions, err = loadSupervisionDecisionsTx(ctx, tx, runID); err != nil {
		return SupervisionProjection{}, err
	}
	return projection, tx.Commit()
}

// ---------------------------------------------------------------------------
// Gate decisions
// ---------------------------------------------------------------------------

// GateDecisionRequest is one structured gate decision. Prose is never a
// decision: the accept or reject outcome, the evidence it binds, the graph and
// gate revisions it names and the request key are all explicit.
type GateDecisionRequest struct {
	RunID     string
	GateID    string
	RequestID string
	Actor     domain.Actor
	// ExpectedGraphRevision and ExpectedGateRevision are the two fences. A
	// decision that names either staler than the record is refused with
	// domain.ErrSupervisionStaleRevision.
	ExpectedGraphRevision int64
	ExpectedGateRevision  int64
	Evidence              domain.EvidenceSnapshot
	Outcome               domain.GateDecisionOutcome
	Reason                string
	// IncidentID is the review incident this decision closes. An acceptance
	// closes only its own matching incident.
	IncidentID string
	DecidedAt  time.Time
}

// DecideGate accepts or rejects a gate. An acceptance widens readiness; a
// rejection moves the gate to held, which narrows it, and the same transaction
// releases every offer the narrowed readiness no longer admits.
func (s *Store) DecideGate(ctx context.Context, request GateDecisionRequest) (SupervisionDecision, error) {
	switch request.Outcome {
	case domain.GateDecisionAccept, domain.GateDecisionReject:
	default:
		return SupervisionDecision{}, fmt.Errorf("unknown gate decision outcome %q", request.Outcome)
	}
	if strings.TrimSpace(request.Reason) == "" {
		return SupervisionDecision{}, errors.New("a gate decision needs a reason")
	}
	return s.supervisionDecisionTx(ctx, "gate-decision", request.RunID, request.RequestID, request,
		func(ctx context.Context, tx *sql.Tx, state supervisionContext) (SupervisionDecision, error) {
			if err := request.Actor.Validate(); err != nil {
				return SupervisionDecision{}, err
			}
			if !supervisionActorCoversRun(state.Record, request.Actor) {
				return SupervisionDecision{}, fmt.Errorf("%w: actor is not scoped to run %q at epoch %d",
					domain.ErrSupervisionUnauthorizedActor, request.RunID, state.Record.ActivationEpoch)
			}
			gate, err := loadSupervisionGateTx(ctx, tx, request.RunID, request.GateID)
			if err != nil {
				return SupervisionDecision{}, err
			}
			if request.ExpectedGateRevision != gate.Revision {
				return SupervisionDecision{}, fmt.Errorf("%w: decision names gate revision %d, the gate is at %d",
					domain.ErrSupervisionStaleRevision, request.ExpectedGateRevision, gate.Revision)
			}
			matches, err := supervisionEvidenceMatchesTx(ctx, tx, request.RunID, request.Evidence, gate)
			if err != nil {
				return SupervisionDecision{}, err
			}
			event := domain.GateEventAccept
			if request.Outcome == domain.GateDecisionReject {
				event = domain.GateEventReject
			}
			next, err := domain.GateTransition(domain.GateTransitionInput{
				Gate:                  gate,
				Event:                 event,
				Actor:                 request.Actor,
				ExpectedGraphRevision: request.ExpectedGraphRevision,
				EvidenceMatches:       matches,
				SinkSettled:           state.Snapshot.RunTerminal,
			})
			if err != nil {
				return SupervisionDecision{}, err
			}
			decidedAt := request.DecidedAt.UTC()
			if decidedAt.IsZero() {
				decidedAt = s.now().UTC()
			}
			gate.State = next
			gate.EvidenceSnapshotID = request.Evidence.ID
			gate.Revision++
			gate.UpdatedAt = decidedAt
			if err := saveSupervisionGateTx(ctx, tx, gate); err != nil {
				return SupervisionDecision{}, err
			}
			recorded := domain.GateDecision{
				ID:              "gate-decision:" + request.GateID + ":" + request.RequestID,
				RunID:           request.RunID,
				GateID:          request.GateID,
				Actor:           request.Actor,
				ActivationEpoch: state.Record.ActivationEpoch,
				GraphRevision:   request.ExpectedGraphRevision,
				Evidence:        request.Evidence,
				Outcome:         request.Outcome,
				RequestID:       request.RequestID,
				Reason:          request.Reason,
				IncidentID:      request.IncidentID,
				DecidedAt:       decidedAt,
			}
			if err := appendGateDecisionTx(ctx, tx, recorded); err != nil {
				return SupervisionDecision{}, err
			}
			decision := SupervisionDecision{Gate: &gate, GateDecision: &recorded}
			if next == domain.GateAccepted && strings.TrimSpace(request.IncidentID) != "" {
				incident, err := loadSupervisionIncidentTx(ctx, tx, request.RunID, request.IncidentID)
				if err != nil {
					return SupervisionDecision{}, err
				}
				resolvedState, err := domain.IncidentTransition(domain.IncidentTransitionInput{
					Incident:             incident,
					Event:                domain.IncidentEventGateAccepted,
					Actor:                request.Actor,
					MatchingGateIncident: incident.GateID == request.GateID,
					ExpectedRevision:     incident.Revision,
				})
				if err != nil {
					return SupervisionDecision{}, err
				}
				incident.State = resolvedState
				incident.Revision++
				incident.Resolution = &domain.ResolutionReceipt{
					Actor: request.Actor, Outcome: domain.IncidentOutcomeGateAccepted,
					EvidenceSnapshotID: request.Evidence.ID, ExpectedRevision: incident.Revision - 1,
					RequestID: request.RequestID, Reason: request.Reason, ResolvedAt: decidedAt,
				}
				if err := saveSupervisionIncidentTx(ctx, tx, incident); err != nil {
					return SupervisionDecision{}, err
				}
				decision.Incident = &incident
			}
			return decision, nil
		})
}

func appendGateDecisionTx(ctx context.Context, tx *sql.Tx, decision domain.GateDecision) error {
	raw, err := json.Marshal(decision)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO coordinator_supervision_decisions(id, run_id, gate_id, request_id, activation_epoch, record)
		VALUES (?, ?, ?, ?, ?, ?)`,
		decision.ID, decision.RunID, decision.GateID, decision.RequestID, decision.ActivationEpoch, raw)
	if err != nil {
		return fmt.Errorf("append gate decision %q: %w", decision.ID, err)
	}
	return nil
}

// supervisionEvidenceMatchesTx checks the binding a decision names against the
// rows this transaction reads: the graph revision, and per observed task the
// exact attempt, its terminal success, its result revision and its artifact
// digests. A retried or replaced producer therefore fails the match instead of
// silently updating an acceptance.
//
// Commit identities are recorded with the decision but not verified here: the
// coordinator holds no repository, so a commit is evidence the worker reported
// rather than a fact the store can recompute.
func supervisionEvidenceMatchesTx(ctx context.Context, tx *sql.Tx, runID string, evidence domain.EvidenceSnapshot, gate domain.Gate) (bool, error) {
	if strings.TrimSpace(evidence.ID) == "" || evidence.GraphRevision != gate.GraphRevision {
		return false, nil
	}
	observed := make(map[string]bool, len(gate.Definition.ObservedTaskIDs))
	for _, taskID := range gate.Definition.ObservedTaskIDs {
		observed[taskID] = false
	}
	for _, producer := range evidence.Producers {
		if _, ok := observed[producer.TaskID]; !ok {
			return false, nil
		}
		attempt, err := loadAttemptTx(ctx, tx, producer.AttemptID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return false, nil
			}
			return false, err
		}
		if attempt.WorkflowRunID != runID || attempt.TaskID != producer.TaskID ||
			attempt.Progress != domain.ProgressSucceeded || attempt.Revision != producer.ResultRevision {
			return false, nil
		}
		for _, digest := range producer.ArtifactDigests {
			var stored string
			err := tx.QueryRowContext(ctx,
				"SELECT sha256 FROM coordinator_artifacts WHERE id = ? AND workflow_run_id = ?",
				digest.ArtifactID, runID).Scan(&stored)
			if errors.Is(err, sql.ErrNoRows) {
				return false, nil
			}
			if err != nil {
				return false, fmt.Errorf("load evidence artifact %q: %w", digest.ArtifactID, err)
			}
			if stored != digest.Digest {
				return false, nil
			}
		}
		observed[producer.TaskID] = true
	}
	for _, present := range observed {
		if !present {
			return false, nil
		}
	}
	return true, nil
}

// ---------------------------------------------------------------------------
// Holds
// ---------------------------------------------------------------------------

// HoldRequest places one dispatch hold. A branch hold names an explicit root
// task; its downstream closure is recomputed in the placing transaction and
// recorded with the graph revision it was computed against.
type HoldRequest struct {
	RunID     string
	HoldID    string
	RequestID string
	Actor     domain.Actor
	Scope     domain.HoldScope
	// ExpectedGraphRevision fences the placement against an amendment that
	// landed after the caller read the graph. Zero does not fence.
	ExpectedGraphRevision int64
	Reason                string
	PlacedAt              time.Time
}

// PlaceHold places a run-wide or branch hold and, in the same transaction,
// releases every outstanding offer the hold now covers. An assignment already
// claimed is past the start boundary: it is reported in ClaimedTaskIDs and is
// never interrupted or rolled back.
func (s *Store) PlaceHold(ctx context.Context, request HoldRequest) (SupervisionDecision, error) {
	if strings.TrimSpace(request.HoldID) == "" || strings.TrimSpace(request.Reason) == "" {
		return SupervisionDecision{}, errors.New("a hold needs an id and a reason")
	}
	return s.supervisionDecisionTx(ctx, "hold-place", request.RunID, request.RequestID, request,
		func(ctx context.Context, tx *sql.Tx, state supervisionContext) (SupervisionDecision, error) {
			if request.ExpectedGraphRevision != 0 && request.ExpectedGraphRevision != state.Snapshot.GraphRevision {
				return SupervisionDecision{}, fmt.Errorf("%w: hold names graph revision %d, the run is at %d",
					domain.ErrSupervisionStaleRevision, request.ExpectedGraphRevision, state.Snapshot.GraphRevision)
			}
			if existing, found, err := loadSupervisionHoldTx(ctx, tx, request.RunID, request.HoldID); err != nil {
				return SupervisionDecision{}, err
			} else if found {
				return SupervisionDecision{}, fmt.Errorf("%w: hold %q already exists in state %q",
					ErrSupervisionRequestConflict, existing.ID, existing.State)
			}
			placedAt := request.PlacedAt.UTC()
			if placedAt.IsZero() {
				placedAt = s.now().UTC()
			}
			hold := domain.Hold{
				ID: request.HoldID, RunID: request.RunID, Scope: request.Scope, Owner: request.Actor,
				GraphRevision: state.Snapshot.GraphRevision, Reason: request.Reason, CreatedAt: placedAt,
			}
			rootExists := true
			if request.Scope.Kind == domain.HoldScopeBranch {
				hold.ResolvedTaskIDs, rootExists = supervisionBranchClosure(state.Tasks, request.Scope.BranchRootTaskID)
			}
			next, err := domain.HoldTransition(domain.HoldTransitionInput{
				Hold:                hold,
				Event:               domain.HoldEventPlace,
				Actor:               request.Actor,
				ActorScopeCoversRun: supervisionActorCoversRun(state.Record, request.Actor),
				BranchRootExists:    rootExists,
				ClosureRecomputed:   true,
			})
			if err != nil {
				return SupervisionDecision{}, err
			}
			hold.State = next
			if err := saveSupervisionHoldTx(ctx, tx, hold); err != nil {
				return SupervisionDecision{}, err
			}
			return SupervisionDecision{Hold: &hold}, nil
		})
}

// HoldReleaseRequest releases one hold by identity. Releasing one hold never
// releases another, never grants a gate and never bypasses quota: the work
// returns to ordinary admission.
type HoldReleaseRequest struct {
	RunID      string
	HoldID     string
	RequestID  string
	Actor      domain.Actor
	Reason     string
	ReleasedAt time.Time
}

// ReleaseHold releases a hold owned by the acting actor, or any hold when the
// actor is an operator. An overseer cannot clear an operator hold.
func (s *Store) ReleaseHold(ctx context.Context, request HoldReleaseRequest) (SupervisionDecision, error) {
	return s.supervisionDecisionTx(ctx, "hold-release", request.RunID, request.RequestID, request,
		func(ctx context.Context, tx *sql.Tx, state supervisionContext) (SupervisionDecision, error) {
			hold, found, err := loadSupervisionHoldTx(ctx, tx, request.RunID, request.HoldID)
			if err != nil {
				return SupervisionDecision{}, err
			}
			if !found {
				return SupervisionDecision{}, fmt.Errorf("%w: hold %q of run %q",
					ErrSupervisionRecordNotFound, request.HoldID, request.RunID)
			}
			if !supervisionActorCoversRun(state.Record, request.Actor) {
				return SupervisionDecision{}, fmt.Errorf("%w: actor is not scoped to run %q at epoch %d",
					domain.ErrSupervisionUnauthorizedActor, request.RunID, state.Record.ActivationEpoch)
			}
			next, err := domain.HoldTransition(domain.HoldTransitionInput{
				Hold: hold, Event: domain.HoldEventRelease, Actor: request.Actor,
				ActorScopeCoversRun: true,
			})
			if err != nil {
				return SupervisionDecision{}, err
			}
			releasedAt := request.ReleasedAt.UTC()
			if releasedAt.IsZero() {
				releasedAt = s.now().UTC()
			}
			hold.State = next
			hold.ReleasedAt = &releasedAt
			if err := saveSupervisionHoldTx(ctx, tx, hold); err != nil {
				return SupervisionDecision{}, err
			}
			return SupervisionDecision{Hold: &hold}, nil
		})
}

// supervisionBranchClosure resolves a branch root to the root plus every task
// that transitively depends on it, at the task set the caller read in this
// transaction. It also reports whether the root exists at all.
//
// The root may be named by task ID or by the task name the manifest declared,
// because those are the two identities a person and a machine respectively have
// for the same task. The resolved set is always task IDs, which is what
// Hold.Covers is asked about.
//
// A dependency edge names the upstream task by name, not by ID, so the walk
// translates through the name index rather than comparing a need to an ID. That
// translation is the whole reason this is not a three-line loop: without it a
// branch hold would resolve to its root alone and cover no descendant at all.
func supervisionBranchClosure(tasks []domain.Task, root string) ([]string, bool) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, false
	}
	idByName := make(map[string]string, len(tasks))
	for _, task := range tasks {
		idByName[task.Name] = task.ID
	}
	exists := false
	for _, task := range tasks {
		if task.ID == root {
			exists = true
			break
		}
	}
	if !exists {
		if id, named := idByName[root]; named {
			root, exists = id, true
		}
	}
	if !exists {
		return nil, false
	}
	closure := map[string]bool{root: true}
	for changed := true; changed; {
		changed = false
		for _, task := range tasks {
			if closure[task.ID] {
				continue
			}
			for _, need := range task.Needs {
				// A dependency edge names its upstream task by the name the
				// manifest declared. An imported or synthetic run may name it by
				// ID instead, so both are accepted and neither is guessed at.
				if closure[idByName[need]] || closure[need] {
					closure[task.ID] = true
					changed = true
					break
				}
			}
		}
	}
	resolved := make([]string, 0, len(closure))
	for id := range closure {
		resolved = append(resolved, id)
	}
	sort.Strings(resolved)
	return resolved, true
}

// ---------------------------------------------------------------------------
// Activations
// ---------------------------------------------------------------------------

// ActivationObservations are the guards a caller observed outside the store: a
// live lease, an observed execution, a reconciled runtime, a completed
// recovery, an explicit operator authorization. Everything the store can read
// for itself — the budget, the inbox, the other activations, the record
// revision — is read here rather than accepted from the caller.
type ActivationObservations struct {
	LeaseValid           bool
	ExecutionObserved    bool
	RuntimeProvenStopped bool
	RecoveryComplete     bool
	OperatorAuthorized   bool
}

// ActivationRequest records one activation state transition.
type ActivationRequest struct {
	RunID        string
	ActivationID string
	RequestID    string
	Actor        domain.Actor
	Event        domain.ActivationEvent
	// ExpectedEpoch and ExpectedSupervisionRevision are the fences a decision
	// recorded by an activation must name.
	ExpectedEpoch               int64
	ExpectedSupervisionRevision int64
	Observations                ActivationObservations
	// DispatchIdentity is deterministic and set once: an undelivered dispatch
	// is retried with the same identity rather than a new one.
	DispatchIdentity    string
	LeaseToken          string
	LeaseExpiresAt      *time.Time
	Deadline            *time.Time
	ConsumedEventCursor int64
	Outcome             domain.ActivationOutcome
	TransitionedAt      time.Time
}

// RecordActivationTransition applies one activation transition and its budget
// accounting in one fenced transaction. Counters are never silently reset: an
// operator-authorized continuation writes a fresh budget and a new receipt.
func (s *Store) RecordActivationTransition(ctx context.Context, request ActivationRequest) (SupervisionDecision, error) {
	if strings.TrimSpace(request.ActivationID) == "" {
		return SupervisionDecision{}, errors.New("an activation transition needs an activation id")
	}
	return s.supervisionDecisionTx(ctx, "activation-transition", request.RunID, request.RequestID, request,
		func(ctx context.Context, tx *sql.Tx, state supervisionContext) (SupervisionDecision, error) {
			activations, err := loadSupervisionActivationsTx(ctx, tx, request.RunID)
			if err != nil {
				return SupervisionDecision{}, err
			}
			activation := domain.Activation{
				ID: request.ActivationID, RunID: request.RunID,
				Epoch: state.Record.ActivationEpoch, State: domain.ActivationIdle,
				DispatchIdentity: request.DispatchIdentity,
			}
			other := false
			for _, candidate := range activations {
				if candidate.ID == request.ActivationID {
					activation = candidate
					continue
				}
				if candidate.State == domain.ActivationPendingDispatch || candidate.State == domain.ActivationActive {
					other = true
				}
			}
			if request.ExpectedEpoch != 0 && request.ExpectedEpoch != activation.Epoch {
				return SupervisionDecision{}, fmt.Errorf("%w: transition names epoch %d, the activation is at %d",
					domain.ErrSupervisionStaleRevision, request.ExpectedEpoch, activation.Epoch)
			}
			pending, err := supervisionInboxPendingTx(ctx, tx, request.RunID)
			if err != nil {
				return SupervisionDecision{}, err
			}
			result, err := domain.ActivationTransition(domain.ActivationTransitionInput{
				Activation:                activation,
				Event:                     request.Event,
				Actor:                     request.Actor,
				Supervised:                true,
				InboxNonEmpty:             pending > 0,
				ActivationBudgetRemaining: state.Record.ActivationBudgetRemaining(),
				TurnsRemaining:            activation.TurnsUsed < state.Record.Config.MaxTurnsPerActivation,
				OtherValidActivation:      other,
				LeaseValid:                request.Observations.LeaseValid,
				ExpectedEpoch:             request.ExpectedEpoch,
				RevisionMatches:           request.ExpectedSupervisionRevision == state.Record.Revision,
				ExecutionObserved:         request.Observations.ExecutionObserved,
				RuntimeProvenStopped:      request.Observations.RuntimeProvenStopped,
				RecoveryComplete:          request.Observations.RecoveryComplete,
				OperatorAuthorized:        request.Observations.OperatorAuthorized,
			})
			if err != nil {
				return SupervisionDecision{}, err
			}
			at := request.TransitionedAt.UTC()
			if at.IsZero() {
				at = s.now().UTC()
			}
			record := state.Record
			activation.State, activation.Epoch = result.State, result.Epoch
			if request.DispatchIdentity != "" {
				activation.DispatchIdentity = request.DispatchIdentity
			}
			if request.LeaseToken != "" {
				activation.LeaseToken = request.LeaseToken
			}
			if request.LeaseExpiresAt != nil {
				expiry := request.LeaseExpiresAt.UTC()
				activation.LeaseExpiresAt = &expiry
			}
			if request.Deadline != nil {
				deadline := request.Deadline.UTC()
				activation.Deadline = &deadline
			}
			if request.ConsumedEventCursor > activation.ConsumedEventCursor {
				activation.ConsumedEventCursor = request.ConsumedEventCursor
			}
			if request.Outcome != domain.ActivationOutcomeNone {
				activation.Outcome = request.Outcome
			}
			switch result.State {
			case domain.ActivationActive:
				if activation.StartedAt == nil {
					started := at
					activation.StartedAt = &started
				}
				if request.Event == domain.ActivationEventDecisionRecorded {
					activation.TurnsUsed++
				}
			case domain.ActivationRevoked:
				activation.LeaseToken, activation.LeaseExpiresAt = "", nil
			case domain.ActivationClosed:
				closed := at
				activation.ClosedAt = &closed
			}
			if result.CountsTowardBudget {
				record.ActivationsUsed++
			}
			if result.FreshBudget {
				record.BudgetGrantedActivations = record.ActivationsUsed + record.Config.MaxActivations
			}
			if result.Epoch > record.ActivationEpoch {
				record.ActivationEpoch = result.Epoch
			}
			if activation.ConsumedEventCursor > record.EventCursor {
				record.EventCursor = activation.ConsumedEventCursor
				if _, err := tx.ExecContext(ctx,
					"UPDATE coordinator_supervision_inbox SET consumed = 1 WHERE run_id = ? AND sequence <= ?",
					request.RunID, record.EventCursor); err != nil {
					return SupervisionDecision{}, fmt.Errorf("consume supervision inbox of run %q: %w", request.RunID, err)
				}
			}
			if err := saveSupervisionActivationTx(ctx, tx, activation); err != nil {
				return SupervisionDecision{}, err
			}
			return SupervisionDecision{Activation: &activation, Record: &record}, nil
		})
}

func supervisionInboxPendingTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var pending int
	err := tx.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM coordinator_supervision_inbox WHERE run_id = ? AND consumed = 0", runID).Scan(&pending)
	if err != nil {
		return 0, fmt.Errorf("count pending supervision events of run %q: %w", runID, err)
	}
	return pending, nil
}

// ---------------------------------------------------------------------------
// Review incidents
// ---------------------------------------------------------------------------

// IncidentRequest raises one review incident. Only a reason transition or a
// threshold crossing raises one, which is read here from the run's open
// incidents rather than asserted by the caller, so ordinary brief quota waiting
// never raises a second incident for the same reason.
type IncidentRequest struct {
	RunID               string
	IncidentID          string
	RequestID           string
	Actor               domain.Actor
	SourceEventID       string
	SourceTaskID        string
	SourceAttemptID     string
	GateID              string
	RequiredDisposition domain.IncidentDisposition
	Reason              string
	OpenedAt            time.Time
}

// OpenReviewIncident raises a decision-requiring condition with a stable
// identity. It narrows nothing by itself, so it releases no offer.
func (s *Store) OpenReviewIncident(ctx context.Context, request IncidentRequest) (SupervisionDecision, error) {
	if strings.TrimSpace(request.IncidentID) == "" || strings.TrimSpace(request.Reason) == "" ||
		strings.TrimSpace(request.SourceEventID) == "" {
		return SupervisionDecision{}, errors.New("a review incident needs an id, a source event and a normalized reason")
	}
	return s.supervisionDecisionTx(ctx, "incident-open", request.RunID, request.RequestID, request,
		func(ctx context.Context, tx *sql.Tx, state supervisionContext) (SupervisionDecision, error) {
			existing, err := loadSupervisionIncidentsTx(ctx, tx, request.RunID)
			if err != nil {
				return SupervisionDecision{}, err
			}
			fresh := true
			for _, candidate := range existing {
				if candidate.ID == request.IncidentID {
					return SupervisionDecision{}, fmt.Errorf("%w: incident %q already exists",
						ErrSupervisionRequestConflict, candidate.ID)
				}
				if candidate.State != domain.IncidentResolved &&
					candidate.Reason == request.Reason && candidate.SourceTaskID == request.SourceTaskID {
					fresh = false
				}
			}
			openedAt := request.OpenedAt.UTC()
			if openedAt.IsZero() {
				openedAt = s.now().UTC()
			}
			incident := domain.ReviewIncident{
				ID: request.IncidentID, RunID: request.RunID,
				SourceEventID: request.SourceEventID, SourceTaskID: request.SourceTaskID,
				SourceAttemptID: request.SourceAttemptID, GateID: request.GateID,
				Revision: 1, RequiredDisposition: request.RequiredDisposition,
				Reason: request.Reason, OpenedAt: openedAt,
			}
			next, err := domain.IncidentTransition(domain.IncidentTransitionInput{
				Incident: domain.ReviewIncident{}, Event: domain.IncidentEventRaise, Actor: request.Actor,
				ReasonChangedOrThresholdCrossed: fresh,
				ActorScopeCoversRun:             supervisionActorCoversRun(state.Record, request.Actor),
			})
			if err != nil {
				return SupervisionDecision{}, err
			}
			incident.State = next
			if err := saveSupervisionIncidentTx(ctx, tx, incident); err != nil {
				return SupervisionDecision{}, err
			}
			return SupervisionDecision{Incident: &incident}, nil
		})
}

// IncidentResolutionRequest escalates or resolves one named incident. v1
// resolves one incident at a time, so the exact-set rule of a bulk close is
// vacuously satisfied.
type IncidentResolutionRequest struct {
	RunID      string
	IncidentID string
	RequestID  string
	Actor      domain.Actor
	Event      domain.IncidentEvent
	// GateID names the gate a gate-acceptance resolution closes the incident
	// for. A resolution matches by gate rather than by the mere presence of a
	// gate on the incident, so accepting one gate cannot close another gate's
	// review incident.
	GateID string
	// ExpectedRevision fences the resolution against a later observation on the
	// same incident.
	ExpectedRevision   int64
	Outcome            domain.IncidentOutcome
	EvidenceSnapshotID string
	Reason             string
	ResolvedAt         time.Time
}

// ResolveReviewIncident applies one incident transition and binds its
// resolution receipt. Closing one incident never dismisses a newer one and
// never clears a gate or a separately owned hold.
func (s *Store) ResolveReviewIncident(ctx context.Context, request IncidentResolutionRequest) (SupervisionDecision, error) {
	if strings.TrimSpace(request.Reason) == "" {
		return SupervisionDecision{}, errors.New("an incident resolution needs a reason")
	}
	return s.supervisionDecisionTx(ctx, "incident-resolve", request.RunID, request.RequestID, request,
		func(ctx context.Context, tx *sql.Tx, state supervisionContext) (SupervisionDecision, error) {
			incident, err := loadSupervisionIncidentTx(ctx, tx, request.RunID, request.IncidentID)
			if err != nil {
				return SupervisionDecision{}, err
			}
			terminallyFailed, err := supervisionSourceTerminallyFailedTx(ctx, tx, incident)
			if err != nil {
				return SupervisionDecision{}, err
			}
			next, err := domain.IncidentTransition(domain.IncidentTransitionInput{
				Incident:                        incident,
				Event:                           request.Event,
				Actor:                           request.Actor,
				ReasonChangedOrThresholdCrossed: true,
				ActorScopeCoversRun:             supervisionActorCoversRun(state.Record, request.Actor),
				MatchingGateIncident:            request.GateID != "" && incident.GateID == request.GateID,
				TaskTerminallyFailed:            terminallyFailed,
				ExpectedRevision:                request.ExpectedRevision,
			})
			if err != nil {
				return SupervisionDecision{}, err
			}
			resolvedAt := request.ResolvedAt.UTC()
			if resolvedAt.IsZero() {
				resolvedAt = s.now().UTC()
			}
			receipt := domain.ResolutionReceipt{
				Actor: request.Actor, Outcome: request.Outcome,
				EvidenceSnapshotID: request.EvidenceSnapshotID, ExpectedRevision: incident.Revision,
				RequestID: request.RequestID, Reason: request.Reason, ResolvedAt: resolvedAt,
			}
			incident.State = next
			incident.Revision++
			if next == domain.IncidentResolved {
				incident.Resolution = &receipt
			}
			if err := saveSupervisionIncidentTx(ctx, tx, incident); err != nil {
				return SupervisionDecision{}, err
			}
			return SupervisionDecision{Incident: &incident}, nil
		})
}

func loadSupervisionIncidentTx(ctx context.Context, tx *sql.Tx, runID, incidentID string) (domain.ReviewIncident, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx,
		"SELECT record FROM coordinator_supervision_incidents WHERE id = ? AND run_id = ?", incidentID, runID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ReviewIncident{}, fmt.Errorf("%w: incident %q of run %q", ErrSupervisionRecordNotFound, incidentID, runID)
	}
	if err != nil {
		return domain.ReviewIncident{}, fmt.Errorf("load incident %q: %w", incidentID, err)
	}
	var incident domain.ReviewIncident
	if err := json.Unmarshal(raw, &incident); err != nil {
		return domain.ReviewIncident{}, fmt.Errorf("decode incident %q: %w", incidentID, err)
	}
	return incident, nil
}

// supervisionSourceTerminallyFailedTx reads the observed terminal failure that
// conclude-failure needs. It is the only condition under which an overseer may
// acknowledge a failure for settlement, so it is read rather than asserted.
func supervisionSourceTerminallyFailedTx(ctx context.Context, tx *sql.Tx, incident domain.ReviewIncident) (bool, error) {
	if strings.TrimSpace(incident.SourceAttemptID) == "" {
		return false, nil
	}
	attempt, err := loadAttemptTx(ctx, tx, incident.SourceAttemptID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return attempt.Progress == domain.ProgressFailed || attempt.Progress == domain.ProgressCancelled, nil
}
