package sqlite

// Committing one overseer activation as offered work.
//
// This is deliberately not a second dispatch engine. It writes the same two
// rows CommitAssignmentPlan writes, into the same two tables, so that every
// later step -- offer delivery, claim, lease renewal, expiry, worker state
// reconciliation, unknown-assignment recovery -- is the machinery a task
// already uses and needs no activation-aware branch.
//
// Three of the per-item fences a task assignment passes are deliberately absent,
// and each absence is the point rather than an omission:
//
//   - The run's supervision predicate is not applied. An activation exists to
//     decide the gates and holds that predicate enforces; subjecting it to them
//     would let an overseer be withheld by its own gate, which is a deadlock.
//   - The graph is not bound. An activation has no declared task, so there is no
//     task revision or task digest to bind, and binding one would put the
//     activation into the run's graph.
//   - External dependency success is not required. An activation reviews the
//     evidence a run has, including the evidence of something that failed.
//
// Everything else is enforced, and one thing is enforced here that the task path
// does not need: the worker's durable snapshot must advertise the campaign
// supervision capability. Placement already excluded a worker that does not, so
// this is the transactional backstop for a fleet that changed underneath the
// plan.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// ErrActivationDispatch reports a refused activation dispatch commit.
var ErrActivationDispatch = errors.New("supervision activation dispatch")

// ActivationAssignmentCommit is one activation's attempt and offered
// assignment, written together or not at all.
type ActivationAssignmentCommit struct {
	CoordinatorEpoch       int64
	Attempt                domain.Attempt
	Assignment             domain.Assignment
	WorkerEpoch            string
	WorkerSnapshotSequence int64
	CommittedAt            time.Time
	QuotaMaxConcurrent     int
}

// FailedActivationOfferSupersession identifies the one failed offer an explicit
// operator reassessment may release before replacement placement is attempted.
type FailedActivationOfferSupersession struct {
	CoordinatorEpoch       int64
	RunID                  string
	ActivationID           string
	ActivationEpoch        int64
	ExpectedRecordRevision int64
	AssignmentID           string
	AssignmentEpoch        int64
	ReassessmentEventID    string
	SupersededAt           time.Time
}

// SupersedeFailedActivationOffer releases one provably never-delivered
// activation offer. Every qualification and the offered-to-released transition
// share the claim transaction boundary, so either a worker claim wins or this
// release wins; neither side can overwrite the other.
func (s *Store) SupersedeFailedActivationOffer(ctx context.Context, request FailedActivationOfferSupersession) error {
	if request.CoordinatorEpoch < 1 || request.RunID == "" || request.ActivationID == "" ||
		request.ActivationEpoch < 1 || request.ExpectedRecordRevision < 1 || request.AssignmentID == "" ||
		request.AssignmentEpoch < 1 || request.ReassessmentEventID == "" || request.SupersededAt.IsZero() {
		return fmt.Errorf("%w: incomplete failed-offer supersession fence", ErrActivationDispatch)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin activation offer supersession: %w", err)
	}
	defer tx.Rollback()
	if err := requireCoordinatorEpoch(ctx, tx, request.CoordinatorEpoch); err != nil {
		return err
	}
	context_, supervised, err := supervisionContextTx(ctx, tx, request.RunID, false)
	if err != nil {
		return err
	}
	if !supervised || context_.Record.Revision != request.ExpectedRecordRevision ||
		context_.Record.ActivationEpoch != request.ActivationEpoch {
		return fmt.Errorf("%w: supervision record moved before offer supersession", ErrActivationDispatch)
	}
	activations, err := loadSupervisionActivationsTx(ctx, tx, request.RunID)
	if err != nil {
		return err
	}
	current := false
	for _, activation := range activations {
		if activation.ID == request.ActivationID && activation.Epoch == request.ActivationEpoch &&
			activation.State == domain.ActivationPendingDispatch {
			current = true
			break
		}
	}
	if !current {
		return fmt.Errorf("%w: activation is not the current pending dispatch", ErrActivationDispatch)
	}
	var eventRaw []byte
	var eventSequence int64
	var eventConsumed int
	if err := tx.QueryRowContext(ctx,
		"SELECT sequence, consumed, record FROM coordinator_supervision_inbox WHERE id = ? AND run_id = ?",
		request.ReassessmentEventID, request.RunID).Scan(&eventSequence, &eventConsumed, &eventRaw); err != nil {
		return fmt.Errorf("%w: supported operator reassessment event is absent", ErrActivationDispatch)
	}
	var event struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(eventRaw, &event); err != nil {
		return fmt.Errorf("decode reassessment event %q: %w", request.ReassessmentEventID, err)
	}
	if event.Kind != "operator-reassessment" || eventConsumed != 0 || eventSequence <= context_.Record.EventCursor {
		return fmt.Errorf("%w: event %q is not a pending operator reassessment", ErrActivationDispatch, request.ReassessmentEventID)
	}
	assignment, err := loadAssignmentTx(ctx, tx, request.AssignmentID)
	if err != nil {
		return err
	}
	attempt, err := loadAttemptTx(ctx, tx, assignment.AttemptID)
	if err != nil {
		return err
	}
	if assignment.Epoch != request.AssignmentEpoch || attempt.WorkflowRunID != request.RunID ||
		attempt.SupervisionActivationID != request.ActivationID ||
		attempt.SupervisionActivationEpoch != request.ActivationEpoch || assignment.DispatchState != "" {
		return fmt.Errorf("%w: assignment does not match the failed pending activation", ErrActivationDispatch)
	}
	var failureRaw []byte
	if err := tx.QueryRowContext(ctx, `SELECT record FROM coordinator_activation_dispatch_failures
		WHERE assignment_id = ? AND assignment_epoch = ? AND run_id = ? AND activation_id = ?`,
		request.AssignmentID, request.AssignmentEpoch, request.RunID, request.ActivationID).Scan(&failureRaw); err != nil {
		return fmt.Errorf("%w: immutable dispatch-failure evidence is absent", ErrActivationDispatch)
	}
	var failure domain.ActivationDispatchFailure
	if err := json.Unmarshal(failureRaw, &failure); err != nil {
		return fmt.Errorf("decode activation dispatch failure: %w", err)
	}
	if failure.AssignmentID != request.AssignmentID || failure.AssignmentEpoch != request.AssignmentEpoch ||
		failure.AttemptID != attempt.ID || failure.ActivationID != request.ActivationID ||
		failure.ActivationEpoch != request.ActivationEpoch || failure.RunID != request.RunID {
		return fmt.Errorf("%w: dispatch-failure identity changed", ErrActivationDispatch)
	}
	auditID := "activation-offer-superseded:" + assignment.ID
	if assignment.State == domain.AssignmentReleased {
		var present int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM coordinator_audit_events WHERE id = ?", auditID).Scan(&present); err != nil {
			return err
		}
		if present != 1 {
			return fmt.Errorf("%w: activation offer was released by another authority", ErrActivationDispatch)
		}
		return tx.Commit()
	}
	if assignment.State != domain.AssignmentOffered {
		return fmt.Errorf("%w: activation assignment %q is %s, not offered", ErrActivationDispatch, assignment.ID, assignment.State)
	}
	next := assignment
	next.State = domain.AssignmentReleased
	next.LeaseExpiresAt = time.Time{}
	next.UpdatedAt = request.SupersededAt.UTC()
	raw, err := json.Marshal(next)
	if err != nil {
		return fmt.Errorf("encode superseded activation assignment %q: %w", next.ID, err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE coordinator_assignments
		SET assignment_state = ?, lease_expires_at = '', record = ?
		WHERE id = ? AND assignment_epoch = ? AND assignment_state = ?`,
		next.State, raw, assignment.ID, request.AssignmentEpoch, domain.AssignmentOffered)
	if err != nil {
		return fmt.Errorf("release failed activation offer %q: %w", assignment.ID, err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return fmt.Errorf("%w: activation offer %q crossed the claim boundary", ErrActivationDispatch, assignment.ID)
	}
	if _, err := insertNativeAuditEventTx(ctx, tx, nativeAuditInput{
		ID: auditID, Kind: "assignment-released", WorkflowRunID: request.RunID,
		AttemptID: attempt.ID, TargetType: domain.AdminTargetAssignment, TargetID: assignment.ID,
		Actor: "coordinator", Reason: "operator reassessment superseded a failed undelivered activation offer",
		CreatedAt: request.SupersededAt.UTC(),
		Detail: nativeAuditDetail{CoordinatorEpoch: request.CoordinatorEpoch, AssignmentEpoch: assignment.Epoch,
			ExpectedRevision: request.ActivationEpoch, Revision: request.ExpectedRecordRevision,
			IdempotencyIdentity: auditID, Outcome: string(domain.AssignmentReleased)},
	}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit activation offer supersession %q: %w", assignment.ID, err)
	}
	return nil
}

// CommitActivationAssignment offers one activation to its placed worker.
//
// It is idempotent on the activation's deterministic identity: a retry of a
// provably undelivered dispatch recomputes the same attempt and the same
// assignment, finds them already offered, and returns the existing records
// rather than creating a second overseer.
func (s *Store) CommitActivationAssignment(ctx context.Context, commit ActivationAssignmentCommit) (domain.Assignment, error) {
	attempt, assignment := commit.Attempt, commit.Assignment
	assignment.WorkerEpoch = commit.WorkerEpoch
	switch {
	case commit.CoordinatorEpoch < 1 || commit.CommittedAt.IsZero():
		return domain.Assignment{}, fmt.Errorf("%w: coordinator epoch and commit time are required", ErrActivationDispatch)
	case !attempt.IsSupervisionActivation():
		return domain.Assignment{}, fmt.Errorf("%w: the attempt carries no activation", ErrActivationDispatch)
	case assignment.AttemptID != attempt.ID || attempt.WorkflowRunID == "":
		return domain.Assignment{}, fmt.Errorf("%w: the assignment is not attached to the activation attempt", ErrActivationDispatch)
	case commit.WorkerEpoch == "" || commit.WorkerSnapshotSequence < 1:
		return domain.Assignment{}, fmt.Errorf("%w: the placement named no worker snapshot", ErrActivationDispatch)
	case assignment.State != domain.AssignmentOffered || assignment.Epoch < 1 ||
		assignment.LeaseToken == "" || assignment.DispatchToken == "" || assignment.ThreadID == "":
		return domain.Assignment{}, fmt.Errorf("%w: the offered assignment is incomplete", ErrActivationDispatch)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Assignment{}, fmt.Errorf("begin activation dispatch: %w", err)
	}
	defer tx.Rollback()
	if err := requireCoordinatorEpoch(ctx, tx, commit.CoordinatorEpoch); err != nil {
		return domain.Assignment{}, err
	}
	existing, found, err := loadOptionalAssignmentTx(ctx, tx, assignment.ID)
	if err != nil {
		return domain.Assignment{}, err
	}
	if found {
		// The same activation epoch already has its assignment. Returning it is
		// what makes an at-least-once wake delivery safe.
		if existing.AttemptID != assignment.AttemptID || existing.ThreadID != assignment.ThreadID ||
			existing.DispatchToken != assignment.DispatchToken {
			return domain.Assignment{}, fmt.Errorf(
				"%w: assignment %q already exists with another identity", ErrActivationDispatch, assignment.ID)
		}
		return existing, nil
	}
	run, err := loadActivationRunTx(ctx, tx, attempt.WorkflowRunID)
	if err != nil {
		return domain.Assignment{}, err
	}
	if run.Supervision == nil {
		return domain.Assignment{}, fmt.Errorf("%w: run %q is not supervised", ErrActivationDispatch, run.ID)
	}
	if runSettled(run) {
		// After terminal settlement every mutating supervision capability is
		// revoked, so dispatching a review into a settled run would produce an
		// overseer with nothing it is allowed to do.
		return domain.Assignment{}, fmt.Errorf("%w: run %q has settled", ErrActivationDispatch, run.ID)
	}
	snapshot, exists, err := loadWorkerSnapshotTx(ctx, tx, assignment.WorkerID)
	if err != nil {
		return domain.Assignment{}, err
	}
	if !exists || snapshot.WorkerEpoch != commit.WorkerEpoch ||
		snapshot.Sequence != commit.WorkerSnapshotSequence ||
		snapshot.CoordinatorEpoch != commit.CoordinatorEpoch {
		return domain.Assignment{}, fmt.Errorf("%w: worker %q planning snapshot changed", ErrStaleWorkerSnapshot, assignment.WorkerID)
	}
	if !workerAccepts(snapshot, commit.CommittedAt) {
		return domain.Assignment{}, fmt.Errorf("%w: worker %q is disconnected, stale, or not ready", ErrWorkerUnavailable, assignment.WorkerID)
	}
	if !slices.Contains(snapshot.Inventory.Capabilities, workerproto.CapabilityCampaignSupervision) {
		return domain.Assignment{}, fmt.Errorf("%w: worker %q does not advertise capability %q",
			ErrActivationDispatch, assignment.WorkerID, workerproto.CapabilityCampaignSupervision)
	}
	enrolled, err := workerEnrolledTx(ctx, tx, assignment.WorkerID)
	if err != nil {
		return domain.Assignment{}, err
	}
	if !enrolled {
		return domain.Assignment{}, fmt.Errorf("%w: worker %q is not enrolled for the effective catalog",
			ErrActivationDispatch, assignment.WorkerID)
	}
	if err := requireExecutorCapacityTx(ctx, tx, assignment.WorkerID, commit.CommittedAt, assignment.WorkerEpoch, domain.ResourceDemand{}, true); err != nil {
		return domain.Assignment{}, fmt.Errorf("%w: %v", ErrActivationDispatch, err)
	}
	if commit.QuotaMaxConcurrent > 0 {
		rows, err := tx.QueryContext(ctx, `SELECT a.record, t.record FROM coordinator_assignments a JOIN coordinator_attempts t ON t.id = a.attempt_id`)
		if err != nil {
			return domain.Assignment{}, fmt.Errorf("read quota occupancy: %w", err)
		}
		active := 0
		for rows.Next() {
			var assignmentRaw, attemptRaw []byte
			if err := rows.Scan(&assignmentRaw, &attemptRaw); err != nil {
				rows.Close()
				return domain.Assignment{}, err
			}
			var occupied domain.Assignment
			var owner domain.Attempt
			if err := json.Unmarshal(assignmentRaw, &occupied); err != nil {
				rows.Close()
				return domain.Assignment{}, err
			}
			if err := json.Unmarshal(attemptRaw, &owner); err != nil {
				rows.Close()
				return domain.Assignment{}, err
			}
			if occupied.Route.QuotaPoolID == assignment.Route.QuotaPoolID &&
				(occupied.State == domain.AssignmentUnknown ||
					(occupied.State == domain.AssignmentOffered || occupied.State == domain.AssignmentClaimed) &&
						owner.Control.HoldsProviderSlot()) {
				active++
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return domain.Assignment{}, fmt.Errorf("iterate quota occupancy: %w", err)
		}
		if err := rows.Close(); err != nil {
			return domain.Assignment{}, err
		}
		if active >= commit.QuotaMaxConcurrent {
			return domain.Assignment{}, fmt.Errorf("%w: quota pool %q has no free slot", ErrActivationDispatch, assignment.Route.QuotaPoolID)
		}
	}
	attempt.AssignmentID = assignment.ID
	attempt.Revision = 1
	attempt.UpdatedAt = commit.CommittedAt
	attemptRaw, err := json.Marshal(attempt)
	if err != nil {
		return domain.Assignment{}, fmt.Errorf("encode activation attempt %q: %w", attempt.ID, err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO coordinator_attempts(id, workflow_run_id, task_id, number, revision, record)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		attempt.ID, attempt.WorkflowRunID, attempt.TaskID, attempt.Number, attempt.Revision, attemptRaw); err != nil {
		return domain.Assignment{}, fmt.Errorf("insert activation attempt %q: %w", attempt.ID, err)
	}
	assignment.CreatedAt = commit.CommittedAt
	assignment.UpdatedAt = commit.CommittedAt
	assignmentRaw, err := json.Marshal(assignment)
	if err != nil {
		return domain.Assignment{}, fmt.Errorf("encode activation assignment %q: %w", assignment.ID, err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO coordinator_assignments(
			id, attempt_id, dispatch_token, dispatch_revision, dispatch_state,
			worker_id, worker_epoch, assignment_epoch, assignment_state, lease_expires_at, record
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?)`,
		assignment.ID, assignment.AttemptID, assignment.DispatchToken, 0, "",
		assignment.WorkerID, assignment.WorkerEpoch, assignment.Epoch, assignment.State, assignmentRaw); err != nil {
		return domain.Assignment{}, fmt.Errorf("commit activation assignment %q: %w", assignment.ID, err)
	}
	if _, err := insertNativeAuditEventTx(ctx, tx, nativeAuditInput{
		ID: "activation-offer:" + assignment.ID, Kind: "supervision-activation-offered",
		WorkflowRunID: attempt.WorkflowRunID, TaskID: attempt.TaskID, AttemptID: attempt.ID,
		TargetType: domain.AdminTargetAssignment, TargetID: assignment.ID,
		Actor:  "coordinator",
		Reason: "supervision activation offered as assigned work",
		// The slot cost is recorded where the offer is, so an operator reading
		// the audit trail can see that an active review occupies one executor
		// slot on this worker for as long as it runs.
		CreatedAt: assignment.CreatedAt,
		Detail: nativeAuditDetail{CoordinatorEpoch: commit.CoordinatorEpoch, WorkerEpoch: commit.WorkerEpoch,
			AssignmentEpoch: assignment.Epoch, WorkerSequence: commit.WorkerSnapshotSequence,
			Revision: attempt.Revision, IdempotencyIdentity: "activation-offer:" + assignment.ID,
			Outcome: string(domain.AssignmentOffered)},
	}); err != nil {
		return domain.Assignment{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Assignment{}, fmt.Errorf("commit activation dispatch %q: %w", assignment.ID, err)
	}
	return assignment, nil
}

func loadOptionalAssignmentTx(ctx context.Context, tx *sql.Tx, id string) (domain.Assignment, bool, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT record FROM coordinator_assignments WHERE id = ?`, id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Assignment{}, false, nil
	}
	if err != nil {
		return domain.Assignment{}, false, fmt.Errorf("load assignment %q: %w", id, err)
	}
	var assignment domain.Assignment
	if err := json.Unmarshal(raw, &assignment); err != nil {
		return domain.Assignment{}, false, fmt.Errorf("decode assignment %q: %w", id, err)
	}
	return assignment, true, nil
}

func loadActivationRunTx(ctx context.Context, tx *sql.Tx, runID string) (domain.WorkflowRun, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT record FROM coordinator_workflow_runs WHERE id = ?`, runID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.WorkflowRun{}, fmt.Errorf("%w: run %q does not exist", ErrActivationDispatch, runID)
	}
	if err != nil {
		return domain.WorkflowRun{}, fmt.Errorf("load run %q: %w", runID, err)
	}
	var run domain.WorkflowRun
	if err := json.Unmarshal(raw, &run); err != nil {
		return domain.WorkflowRun{}, fmt.Errorf("decode run %q: %w", runID, err)
	}
	return run, nil
}
