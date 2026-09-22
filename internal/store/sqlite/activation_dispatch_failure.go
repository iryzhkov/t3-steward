package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var ErrActivationDispatchFailureConflict = errors.New("activation dispatch failure identity conflict")

func (s *Store) LoadActivationDispatchFailure(ctx context.Context, assignmentID string, assignmentEpoch int64) (domain.ActivationDispatchFailure, bool, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, "SELECT record FROM coordinator_activation_dispatch_failures WHERE assignment_id = ? AND assignment_epoch = ?", assignmentID, assignmentEpoch).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ActivationDispatchFailure{}, false, nil
	}
	if err != nil {
		return domain.ActivationDispatchFailure{}, false, err
	}
	var failure domain.ActivationDispatchFailure
	if err := json.Unmarshal(raw, &failure); err != nil {
		return domain.ActivationDispatchFailure{}, false, err
	}
	return failure, true, nil
}

func (s *Store) RecordActivationDispatchFailure(ctx context.Context, coordinatorEpoch int64, failure domain.ActivationDispatchFailure) (domain.ActivationDispatchFailure, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.ActivationDispatchFailure{}, false, err
	}
	defer tx.Rollback()
	if err := requireCoordinatorEpoch(ctx, tx, coordinatorEpoch); err != nil {
		return domain.ActivationDispatchFailure{}, false, err
	}
	var assignmentRaw, attemptRaw []byte
	err = tx.QueryRowContext(ctx, "SELECT assignment.record, attempt.record FROM coordinator_assignments AS assignment JOIN coordinator_attempts AS attempt ON attempt.id = assignment.attempt_id WHERE assignment.id = ? AND assignment.assignment_epoch = ?", failure.AssignmentID, failure.AssignmentEpoch).Scan(&assignmentRaw, &attemptRaw)
	if err != nil {
		return domain.ActivationDispatchFailure{}, false, fmt.Errorf("%w: assignment is no longer current", ErrActivationDispatchFailureConflict)
	}
	var assignment domain.Assignment
	var attempt domain.Attempt
	if err := json.Unmarshal(assignmentRaw, &assignment); err != nil {
		return domain.ActivationDispatchFailure{}, false, err
	}
	if err := json.Unmarshal(attemptRaw, &attempt); err != nil {
		return domain.ActivationDispatchFailure{}, false, err
	}
	if assignment.State != domain.AssignmentOffered || attempt.ID != failure.AttemptID ||
		attempt.WorkflowRunID != failure.RunID || attempt.SupervisionActivationID != failure.ActivationID ||
		attempt.SupervisionActivationEpoch != failure.ActivationEpoch {
		return domain.ActivationDispatchFailure{}, false, fmt.Errorf("%w: assignment/attempt/activation identity changed", ErrActivationDispatchFailureConflict)
	}
	run, err := loadActivationRunTx(ctx, tx, failure.RunID)
	if err != nil {
		return domain.ActivationDispatchFailure{}, false, err
	}
	if run.GraphRevision != failure.GraphRevision {
		return domain.ActivationDispatchFailure{}, false, fmt.Errorf("%w: graph revision changed", ErrActivationDispatchFailureConflict)
	}
	var existingRaw []byte
	err = tx.QueryRowContext(ctx, "SELECT record FROM coordinator_activation_dispatch_failures WHERE assignment_id = ? AND assignment_epoch = ?", failure.AssignmentID, failure.AssignmentEpoch).Scan(&existingRaw)
	if err == nil {
		var existing domain.ActivationDispatchFailure
		if err := json.Unmarshal(existingRaw, &existing); err != nil {
			return domain.ActivationDispatchFailure{}, false, err
		}
		if existing.Code != failure.Code || existing.ActivationID != failure.ActivationID ||
			existing.GraphRevision != failure.GraphRevision || existing.EvidenceRef != failure.EvidenceRef {
			return domain.ActivationDispatchFailure{}, false, ErrActivationDispatchFailureConflict
		}
		if err := tx.Commit(); err != nil {
			return domain.ActivationDispatchFailure{}, false, err
		}
		return existing, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return domain.ActivationDispatchFailure{}, false, err
	}
	raw, err := json.Marshal(failure)
	if err != nil {
		return domain.ActivationDispatchFailure{}, false, err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO coordinator_activation_dispatch_failures(id, assignment_id, assignment_epoch, run_id, activation_id, record) VALUES (?, ?, ?, ?, ?, ?)", failure.ID, failure.AssignmentID, failure.AssignmentEpoch, failure.RunID, failure.ActivationID, raw); err != nil {
		return domain.ActivationDispatchFailure{}, false, err
	}
	if _, err := insertNativeAuditEventTx(ctx, tx, nativeAuditInput{
		ID:   "activation-dispatch-failure:" + failure.ID,
		Kind: "activation-dispatch-failure", WorkflowRunID: failure.RunID,
		AttemptID: failure.AttemptID, TargetType: domain.AuditTargetArtifact,
		TargetID: failure.AssignmentID, Actor: "coordinator",
		Reason: failure.SafeMessage, CreatedAt: failure.FirstSeenAt,
		Detail: nativeAuditDetail{
			CoordinatorEpoch: coordinatorEpoch, AssignmentEpoch: failure.AssignmentEpoch,
			ExpectedRevision: failure.ActivationEpoch, Revision: failure.GraphRevision,
			IdempotencyIdentity: failure.ID, Outcome: failure.NextAction,
		},
	}); err != nil {
		return domain.ActivationDispatchFailure{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return domain.ActivationDispatchFailure{}, false, err
	}
	return failure, true, nil
}
