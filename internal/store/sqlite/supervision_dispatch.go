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
