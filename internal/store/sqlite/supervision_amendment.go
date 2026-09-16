package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Supervision under a graph change: what an amendment must recompute before it
// commits, and what a clone or a rerun inherits.
//
// The plan states the amendment rule directly: "While a run has supervision,
// every operator graph amendment must atomically recompute affected branch-hold
// closures and gate protection, invalidate affected evidence, and increment
// graph/review revisions before dispatch resumes. If the amendment cannot
// preserve these invariants it is refused." Every step below therefore runs
// inside the amendment's own transaction, so a refusal rolls the whole
// amendment back and a success is one serialized event.
//
// Nothing here decides anything by itself. The hold and gate state machines and
// the acceptance predicate are the frozen domain interface; this file reads the
// rows their guards need, recomputes the two closures the graph change can move,
// and writes what the pure functions returned.
//
// An unsupervised run reaches none of this: supervisionContextTx reports it is
// not supervised and the amendment behaves exactly as it did before supervision
// existed.

// ErrSupervisionAmendmentRefused reports a graph change that supervision cannot
// absorb without either losing a gate's task or invalidating an acceptance whose
// protected work has already been offered or started. v1 refuses rather than
// pretending a started effect can be undone; the operator's recourse is explicit
// recovery or a new run.
var ErrSupervisionAmendmentRefused = errors.New("graph amendment cannot preserve supervision invariants")

// supervisionAmendment is everything the supervision half of an amendment needs
// from the amendment transaction that is already in flight: the run as the
// amendment has just rewritten it, the amended task set, the tasks whose
// definitions changed, and the request identity the receipts are derived from.
type supervisionAmendment struct {
	Run            domain.WorkflowRun
	Tasks          []domain.Task
	ChangedTaskIDs []string
	RequestID      string
	Actor          string
	Reason         string
	Now            time.Time
}

// applySupervisionAmendmentTx recomputes a supervised run's holds and gates
// against the amended task set, invalidates or carries forward the acceptances
// the change touched, releases the offers the new state no longer admits and
// bumps the supervision record's revision, all in the caller's transaction.
//
// The record revision bump is the fence: a decision formed against the
// pre-amendment state names a revision this transaction has already moved past.
func applySupervisionAmendmentTx(ctx context.Context, tx *sql.Tx, amendment supervisionAmendment) error {
	state, supervised, err := supervisionContextTx(ctx, tx, amendment.Run.ID, false)
	if err != nil || !supervised {
		return err
	}
	changed := append([]string(nil), amendment.ChangedTaskIDs...)
	sort.Strings(changed)
	amendment.ChangedTaskIDs = changed
	if err := widenSupervisionHoldsTx(ctx, tx, amendment); err != nil {
		return err
	}
	if err := refreshSupervisionGatesTx(ctx, tx, amendment); err != nil {
		return err
	}
	// Re-read the state this transaction just wrote, exactly as a decision
	// transaction does, so the offers released are the ones the recomputed holds
	// and gates no longer admit.
	after, err := supervisionStateTx(ctx, tx, amendment.Run.ID)
	if err != nil {
		return err
	}
	if _, _, _, err := releaseNarrowedOffersTx(ctx, tx, after, amendment.Now); err != nil {
		return err
	}
	record := state.Record
	record.RunID = amendment.Run.ID
	record.Revision++
	if record.CreatedAt.IsZero() {
		record.CreatedAt = amendment.Now
	}
	record.UpdatedAt = amendment.Now
	return saveSupervisionRecordTx(ctx, tx, record)
}

// widenSupervisionHoldsTx recomputes every active branch hold's closure against
// the amended task set. A task the amendment added below a held root is a
// descendant of that root, so it joins the hold: new descendants cannot escape
// an existing branch hold.
func widenSupervisionHoldsTx(ctx context.Context, tx *sql.Tx, amendment supervisionAmendment) error {
	holds, err := loadSupervisionHoldsTx(ctx, tx, amendment.Run.ID, domain.HoldActive)
	if err != nil {
		return err
	}
	for _, hold := range holds {
		if hold.Scope.Kind != domain.HoldScopeBranch {
			// A run hold covers every worker task of the run by definition, so an
			// amendment cannot widen it. Its graph revision still moves, so the
			// row says which graph the hold is now recorded against.
			hold.GraphRevision = amendment.Run.GraphRevision
			if err := saveSupervisionHoldTx(ctx, tx, hold); err != nil {
				return err
			}
			continue
		}
		resolved, exists := supervisionBranchClosure(amendment.Tasks, hold.Scope.BranchRootTaskID)
		if !exists {
			return fmt.Errorf("%w: hold %q holds branch root %q, which the amended task set does not contain",
				ErrSupervisionAmendmentRefused, hold.ID, hold.Scope.BranchRootTaskID)
		}
		// ClosureRecomputed is true because it was recomputed here, in this
		// transaction. The state machine checks the invariant rather than being
		// told to assume it.
		next, err := domain.HoldTransition(domain.HoldTransitionInput{
			Hold:                hold,
			Event:               domain.HoldEventAmendmentAddsDescendants,
			Actor:               hold.Owner,
			ActorScopeCoversRun: true,
			BranchRootExists:    true,
			ClosureRecomputed:   true,
		})
		if err != nil {
			return fmt.Errorf("%w: hold %q: %w", ErrSupervisionAmendmentRefused, hold.ID, err)
		}
		hold.State = next
		hold.ResolvedTaskIDs = resolved
		hold.GraphRevision = amendment.Run.GraphRevision
		if err := saveSupervisionHoldTx(ctx, tx, hold); err != nil {
			return err
		}
	}
	return nil
}

// refreshSupervisionGatesTx recomputes every gate's protected closure, moves the
// gate onto the amendment's graph revision and settles what happens to an
// acceptance the change touched.
func refreshSupervisionGatesTx(ctx context.Context, tx *sql.Tx, amendment supervisionAmendment) error {
	gates, err := loadSupervisionGatesTx(ctx, tx, amendment.Run.ID)
	if err != nil || len(gates) == 0 {
		return err
	}
	present := make(map[string]bool, len(amendment.Tasks))
	for _, task := range amendment.Tasks {
		present[task.ID] = true
	}
	live, err := supervisionLiveTaskStatesTx(ctx, tx, amendment.Run.ID)
	if err != nil {
		return err
	}
	acceptances, err := latestGateAcceptancesTx(ctx, tx, amendment.Run.ID)
	if err != nil {
		return err
	}
	for _, gate := range gates {
		definition := gate.Definition
		for _, observed := range definition.ObservedTaskIDs {
			if !present[observed] {
				return fmt.Errorf("%w: gate %q observes task %q, which the amended task set does not contain",
					ErrSupervisionAmendmentRefused, definition.ID, observed)
			}
		}
		protected, err := supervisionProtectedClosure(amendment.Tasks, definition)
		if err != nil {
			return err
		}
		definition.ProtectedTaskIDs = protected
		if err := definition.Validate(); err != nil {
			return fmt.Errorf("%w: gate %q: %w", ErrSupervisionAmendmentRefused, definition.ID, err)
		}
		gate.Definition = definition
		gate.GraphRevision = amendment.Run.GraphRevision
		gate.Revision++
		gate.UpdatedAt = amendment.Now
		if gate.State == domain.GateAccepted {
			if err := settleAmendedAcceptanceTx(ctx, tx, amendment, &gate, acceptances[definition.ID], live); err != nil {
				return err
			}
		}
		if err := saveSupervisionGateTx(ctx, tx, gate); err != nil {
			return err
		}
	}
	return nil
}

// settleAmendedAcceptanceTx decides what an amendment does to one accepted gate.
//
// Conservative invalidation is the default: an amendment touching the gate's
// observed or protected set invalidates the acceptance unconditionally, and an
// amendment touching neither carries it forward only against an atomic
// revalidation receipt, which is written as a native audit event in this same
// transaction so the carry-forward is auditable rather than merely asserted.
func settleAmendedAcceptanceTx(
	ctx context.Context,
	tx *sql.Tx,
	amendment supervisionAmendment,
	gate *domain.Gate,
	acceptance domain.GateDecision,
	live map[string]domain.AssignmentState,
) error {
	receipt := supervisionRevalidationReceiptID(amendment.RequestID, gate.Definition.ID)
	if acceptance.Outcome == "" {
		// The gate is accepted but no decision row says so, which is what a
		// materialized database looks like. The predicate reads only the
		// outcome, so the gate's own state is the acceptance being tested.
		acceptance = domain.GateDecision{
			RunID: gate.RunID, GateID: gate.Definition.ID, Outcome: domain.GateDecisionAccept,
			GraphRevision: gate.GraphRevision,
		}
	}
	verdict := domain.GateAcceptanceStands(gate.Definition, acceptance, domain.GateChange{
		Kind:                  domain.GateChangeAmendment,
		AmendedTaskIDs:        amendment.ChangedTaskIDs,
		RevalidationReceiptID: receipt,
	})
	event := domain.GateEventAmendmentOutsideScope
	if !verdict.Stands {
		event = domain.GateEventAmendmentTouchesScope
		// Reopening reviewed work whose dependent successor has already been
		// offered or started is refused in v1: the offer is the start boundary,
		// and a boundary already crossed is reported rather than rewound.
		for _, taskID := range gate.Definition.ProtectedTaskIDs {
			switch live[taskID] {
			case domain.AssignmentOffered, domain.AssignmentClaimed:
				return fmt.Errorf(
					"%w: gate %q must be reviewed again, but the task %q it protects is already %s; use explicit recovery or a new run",
					ErrSupervisionAmendmentRefused, gate.Definition.ID, taskID, live[taskID])
			}
		}
	}
	next, err := domain.GateTransition(domain.GateTransitionInput{
		Gate:                  *gate,
		Event:                 event,
		ExpectedGraphRevision: gate.GraphRevision,
		RevalidationReceiptID: receipt,
	})
	if err != nil {
		return fmt.Errorf("%w: gate %q: %w", ErrSupervisionAmendmentRefused, gate.Definition.ID, err)
	}
	gate.State = next
	if next != domain.GateAccepted {
		// The gate has to be reviewed again, so it refers to no snapshot until
		// fresh producer evidence arrives.
		gate.EvidenceSnapshotID = ""
		return nil
	}
	return recordSupervisionRevalidationTx(ctx, tx, amendment, gate.Definition.ID, receipt)
}

// supervisionRevalidationReceiptID is deterministic in the amendment request and
// the gate, so a replayed amendment names the same receipt.
func supervisionRevalidationReceiptID(requestID, gateID string) string {
	return "revalidation:" + requestID + ":" + gateID
}

func recordSupervisionRevalidationTx(ctx context.Context, tx *sql.Tx, amendment supervisionAmendment, gateID, receipt string) error {
	reason := strings.TrimSpace(amendment.Reason)
	if reason == "" {
		reason = "amendment outside the gate's observed and protected sets"
	}
	_, err := insertNativeAuditEventTx(ctx, tx, nativeAuditInput{
		ID:            "supervision-revalidated:" + amendment.RequestID + ":" + gateID,
		Kind:          "supervision-revalidated",
		WorkflowRunID: amendment.Run.ID,
		TargetType:    domain.AdminTargetWorkflowRun,
		TargetID:      amendment.Run.ID,
		Actor:         amendment.Actor,
		Reason:        reason,
		CreatedAt:     amendment.Now,
		Detail: nativeAuditDetail{
			ExpectedRevision:    amendment.Run.GraphRevision - 1,
			Revision:            amendment.Run.GraphRevision,
			IdempotencyIdentity: receipt,
			Outcome:             "acceptance-carried-forward",
		},
	})
	return err
}

// supervisionProtectedClosure resolves a gate's protected set: the tasks it
// declares plus their dependency descendants, at the amended task set. A gate
// protects a task in order to protect what depends on it, so a descendant the
// amendment added is protected too.
//
// A declared protected task that is no longer in the task set is a refusal: a
// gate silently losing one of its tasks is exactly the unreviewed dispatch this
// machinery exists to prevent.
func supervisionProtectedClosure(tasks []domain.Task, definition domain.GateDefinition) ([]string, error) {
	if len(definition.ProtectedTaskIDs) == 0 {
		// A final-settlement gate protects no downstream task.
		return nil, nil
	}
	observed := make(map[string]bool, len(definition.ObservedTaskIDs))
	for _, id := range definition.ObservedTaskIDs {
		observed[id] = true
	}
	union := map[string]bool{}
	for _, id := range definition.ProtectedTaskIDs {
		resolved, exists := supervisionBranchClosure(tasks, id)
		if !exists {
			return nil, fmt.Errorf("%w: gate %q protects task %q, which the amended task set does not contain",
				ErrSupervisionAmendmentRefused, definition.ID, id)
		}
		for _, member := range resolved {
			// An observed producer never becomes protected: a gate that observed
			// and protected the same task would hold its own evidence.
			if !observed[member] {
				union[member] = true
			}
		}
	}
	protected := make([]string, 0, len(union))
	for id := range union {
		protected = append(protected, id)
	}
	sort.Strings(protected)
	return protected, nil
}

// supervisionLiveTaskStatesTx reports, per task of the run, whether an
// assignment for it is currently offered or claimed. Those are the two states
// that make an invalidation a refusal rather than a transition.
func supervisionLiveTaskStatesTx(ctx context.Context, tx *sql.Tx, runID string) (map[string]domain.AssignmentState, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT attempt.task_id, assignment.assignment_state
		FROM coordinator_assignments AS assignment
		JOIN coordinator_attempts AS attempt ON attempt.id = assignment.attempt_id
		WHERE attempt.workflow_run_id = ? AND assignment.assignment_state IN (?, ?)`,
		runID, string(domain.AssignmentOffered), string(domain.AssignmentClaimed))
	if err != nil {
		return nil, fmt.Errorf("load live assignments of run %q: %w", runID, err)
	}
	defer rows.Close()
	states := map[string]domain.AssignmentState{}
	for rows.Next() {
		var taskID, state string
		if err := rows.Scan(&taskID, &state); err != nil {
			return nil, err
		}
		// A claimed assignment is past the start boundary, so it outranks an
		// offer on the same task when both exist.
		if states[taskID] != domain.AssignmentClaimed {
			states[taskID] = domain.AssignmentState(state)
		}
	}
	return states, rows.Err()
}

// latestGateAcceptancesTx returns the most recent acceptance per gate.
//
// A gate can be in state accepted without a decision row in a database
// materialized rather than decided; the acceptance predicate reads only the
// outcome, so a synthetic acceptance answers the same question the recorded one
// would.
func latestGateAcceptancesTx(ctx context.Context, tx *sql.Tx, runID string) (map[string]domain.GateDecision, error) {
	decisions, err := loadSupervisionDecisionsTx(ctx, tx, runID)
	if err != nil {
		return nil, err
	}
	acceptances := map[string]domain.GateDecision{}
	for _, decision := range decisions {
		if decision.Outcome == domain.GateDecisionAccept {
			acceptances[decision.GateID] = decision
		}
	}
	return acceptances, nil
}

// ---------------------------------------------------------------------------
// Clone and rerun inheritance
// ---------------------------------------------------------------------------

// inheritedSupervision is the supervision a clone or a rerun starts with: the
// configuration and the gate definitions, and nothing a decision produced.
type inheritedSupervision struct {
	Record domain.SupervisionRecord
	Gates  []domain.Gate
}

// inheritSupervisionTx builds the supervision a clone or rerun inherits from its
// source run.
//
// A clone or rerun inherits the supervision configuration, a fresh activation
// epoch and the gate definitions remapped onto the new run's tasks. It inherits
// no acceptances, no holds and no incidents: an acceptance is a statement about
// evidence that the new run does not have, so every inherited gate starts at
// pending evidence with no evidence snapshot.
//
// It returns nil for an unsupervised source, which leaves the new run byte for
// byte what it was before supervision existed.
func inheritSupervisionTx(
	ctx context.Context,
	tx *sql.Tx,
	source domain.WorkflowRun,
	run domain.WorkflowRun,
	remap map[string]string,
	tasks []domain.Task,
	now time.Time,
) (*inheritedSupervision, error) {
	record, found, err := loadSupervisionRecordTx(ctx, tx, source.ID)
	if err != nil {
		return nil, err
	}
	if !found {
		if source.Supervision == nil {
			return nil, nil
		}
		record = *source.Supervision
	}
	inherited := &inheritedSupervision{Record: domain.SupervisionRecord{
		RunID:           run.ID,
		Config:          record.Config,
		EventCursor:     0,
		ActivationEpoch: 1,
		Revision:        1,
		CreatedAt:       now,
		UpdatedAt:       now,
	}}
	gates, err := loadSupervisionGatesTx(ctx, tx, source.ID)
	if err != nil {
		return nil, err
	}
	present := make(map[string]bool, len(tasks))
	for _, task := range tasks {
		present[task.ID] = true
	}
	for _, gate := range gates {
		definition, carried, err := remapGateDefinition(gate.Definition, run.ID, remap, present)
		if err != nil {
			return nil, err
		}
		if !carried {
			// None of this gate's tasks exists in the new run, which is the
			// ordinary case for a rerun of a subtree that the gate is not part
			// of. There is nothing for it to observe or to hold.
			continue
		}
		inherited.Gates = append(inherited.Gates, domain.Gate{
			Definition:    definition,
			RunID:         run.ID,
			State:         domain.GatePendingEvidence,
			GraphRevision: run.GraphRevision,
			Revision:      1,
			UpdatedAt:     now,
		})
	}
	return inherited, nil
}

// remapGateDefinition moves one gate definition onto the new run's task IDs.
//
// A task the clone or rerun rebuilt is named by the remap; a task it reused
// keeps its own ID and is recognised by being present in the new task set.
//
// A gate is inherited whole or not at all. A rerun of a subtree that excludes a
// gate's producers cannot inherit that gate: a gate whose observed task is
// missing could never become review-ready, so protecting a task with it would
// be a permanent hold, and a gate whose protected task is missing protects
// nothing. Dropping one of a gate's tasks while keeping the gate is the one
// outcome this must never produce, and an amendment that would do it to a live
// run is refused outright by refreshSupervisionGatesTx.
func remapGateDefinition(
	definition domain.GateDefinition,
	runID string,
	remap map[string]string,
	present map[string]bool,
) (domain.GateDefinition, bool, error) {
	mapped := func(id string) (string, bool) {
		if next, ok := remap[id]; ok && present[next] {
			return next, true
		}
		if present[id] {
			return id, true
		}
		return "", false
	}
	var observed, protected, missing []string
	for _, id := range definition.ObservedTaskIDs {
		if next, ok := mapped(id); ok {
			observed = append(observed, next)
		} else {
			missing = append(missing, id)
		}
	}
	for _, id := range definition.ProtectedTaskIDs {
		if next, ok := mapped(id); ok {
			protected = append(protected, next)
		} else {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		return domain.GateDefinition{}, false, nil
	}
	definition.ID = runID + ":" + definition.ID
	definition.ObservedTaskIDs = observed
	definition.ProtectedTaskIDs = protected
	if err := definition.Validate(); err != nil {
		return domain.GateDefinition{}, false, fmt.Errorf("%w: gate %q: %w",
			ErrSupervisionAmendmentRefused, definition.ID, err)
	}
	return definition, true, nil
}

// saveInheritedSupervisionTx writes the inherited record and gates in the clone
// or rerun transaction, so the new run is supervised from the moment it exists.
func saveInheritedSupervisionTx(ctx context.Context, tx *sql.Tx, inherited *inheritedSupervision) error {
	if inherited == nil {
		return nil
	}
	if err := saveSupervisionRecordTx(ctx, tx, inherited.Record); err != nil {
		return err
	}
	for _, gate := range inherited.Gates {
		if err := saveSupervisionGateTx(ctx, tx, gate); err != nil {
			return err
		}
	}
	return nil
}
