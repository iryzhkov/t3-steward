package sqlite

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var (
	ErrInvalidUnknownRecovery = errors.New("invalid unknown-assignment recovery")
	ErrStaleUnknownRecovery   = errors.New("stale unknown-assignment recovery")
)

type unknownRecoveryAuditDetail struct {
	Recovery domain.UnknownAssignmentRecovery `json:"recovery"`
	Attempt  domain.Attempt                   `json:"attempt"`
}

// RecoverUnknownAssignment applies one authorization-ready, revision-fenced
// resolution. It never starts, resumes, claims, or dispatches work, and cannot
// mutate quota admission.
func (s *Store) RecoverUnknownAssignment(ctx context.Context, recovery domain.UnknownAssignmentRecovery) (domain.UnknownAssignmentRecoveryDecision, error) {
	if err := validateUnknownRecovery(recovery); err != nil {
		return domain.UnknownAssignmentRecoveryDecision{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.UnknownAssignmentRecoveryDecision{}, fmt.Errorf("begin unknown recovery: %w", err)
	}
	defer tx.Rollback()
	eventID := "unknown-recovery:" + recovery.ID
	if event, found, err := loadAuditEventTx(ctx, tx, eventID); err != nil {
		return domain.UnknownAssignmentRecoveryDecision{}, err
	} else if found {
		var detail unknownRecoveryAuditDetail
		if err := json.Unmarshal(event.Detail, &detail); err != nil {
			return domain.UnknownAssignmentRecoveryDecision{}, fmt.Errorf("decode unknown recovery replay: %w", err)
		}
		if !sameUnknownRecoveryIdentity(detail.Recovery, recovery) {
			return domain.UnknownAssignmentRecoveryDecision{}, fmt.Errorf("%w: id %q already has different content", ErrInvalidUnknownRecovery, recovery.ID)
		}
		assignment, err := loadAssignmentTx(ctx, tx, recovery.AssignmentID)
		if err != nil {
			return domain.UnknownAssignmentRecoveryDecision{}, err
		}
		if err := tx.Commit(); err != nil {
			return domain.UnknownAssignmentRecoveryDecision{}, err
		}
		return domain.UnknownAssignmentRecoveryDecision{Recovery: detail.Recovery, Assignment: assignment, Attempt: detail.Attempt, Event: event, Replay: true}, nil
	}
	if err := requireCoordinatorEpoch(ctx, tx, recovery.CoordinatorEpoch); err != nil {
		return domain.UnknownAssignmentRecoveryDecision{}, err
	}
	assignment, err := loadAssignmentTx(ctx, tx, recovery.AssignmentID)
	if err != nil {
		return domain.UnknownAssignmentRecoveryDecision{}, err
	}
	if assignment.State != domain.AssignmentUnknown || assignment.Epoch != recovery.ExpectedAssignmentEpoch {
		return domain.UnknownAssignmentRecoveryDecision{}, fmt.Errorf("%w: assignment %q state or epoch changed", ErrStaleUnknownRecovery, assignment.ID)
	}
	attempt, err := loadAttemptTx(ctx, tx, assignment.AttemptID)
	if err != nil {
		return domain.UnknownAssignmentRecoveryDecision{}, err
	}
	if attempt.Revision != recovery.ExpectedAttemptRevision || attempt.AssignmentID != assignment.ID {
		return domain.UnknownAssignmentRecoveryDecision{}, fmt.Errorf("%w: attempt %q changed", ErrStaleUnknownRecovery, attempt.ID)
	}
	assignment.State = domain.AssignmentReleased
	assignment.LeaseExpiresAt = time.Time{}
	assignment.UpdatedAt = recovery.RecoveredAt
	attempt.AssignmentID = ""
	attempt.Revision++
	attempt.UpdatedAt = recovery.RecoveredAt
	switch recovery.Outcome {
	case domain.UnknownRecoveryStopped:
		assignment.DispatchState = domain.DispatchStopped
		attempt.Progress = domain.ProgressReady
		attempt.Control = domain.ControlUnassigned
		attempt.ThreadID = ""
		attempt.Failure = ""
	case domain.UnknownRecoveryFailed:
		attempt.Progress = domain.ProgressFailed
		attempt.Control = domain.ControlStopped
		attempt.Failure = "unknown execution resolved as failed by reviewed evidence"
		completed := recovery.RecoveredAt
		attempt.CompletedAt = &completed
	}
	assignmentRaw, err := json.Marshal(assignment)
	if err != nil {
		return domain.UnknownAssignmentRecoveryDecision{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE coordinator_assignments SET assignment_state = ?, dispatch_state = ?, lease_expires_at = '', record = ? WHERE id = ? AND assignment_epoch = ? AND assignment_state = ?`,
		assignment.State, assignment.DispatchState, assignmentRaw, assignment.ID, recovery.ExpectedAssignmentEpoch, domain.AssignmentUnknown)
	if err != nil {
		return domain.UnknownAssignmentRecoveryDecision{}, fmt.Errorf("release unknown assignment %q: %w", assignment.ID, err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return domain.UnknownAssignmentRecoveryDecision{}, fmt.Errorf("%w: assignment %q changed concurrently", ErrStaleUnknownRecovery, assignment.ID)
	}
	if err := updateAttemptTx(ctx, tx, attempt, recovery.ExpectedAttemptRevision); err != nil {
		return domain.UnknownAssignmentRecoveryDecision{}, fmt.Errorf("%w: %v", ErrStaleUnknownRecovery, err)
	}
	detail, err := json.Marshal(unknownRecoveryAuditDetail{Recovery: recovery, Attempt: attempt})
	if err != nil {
		return domain.UnknownAssignmentRecoveryDecision{}, err
	}
	event, err := insertAuditEventTx(ctx, tx, domain.AuditEvent{
		ID: eventID, Kind: "unknown-assignment-recovered", WorkflowRunID: attempt.WorkflowRunID,
		TaskID: attempt.TaskID, AttemptID: attempt.ID, TargetType: domain.AdminTargetAssignment,
		TargetID: assignment.ID, Actor: recovery.Actor, Reason: recovery.Reason,
		Detail: detail, CreatedAt: recovery.RecoveredAt,
	})
	if err != nil {
		return domain.UnknownAssignmentRecoveryDecision{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.UnknownAssignmentRecoveryDecision{}, fmt.Errorf("commit unknown recovery: %w", err)
	}
	return domain.UnknownAssignmentRecoveryDecision{Recovery: recovery, Assignment: assignment, Attempt: attempt, Event: event}, nil
}

func sameUnknownRecoveryIdentity(left, right domain.UnknownAssignmentRecovery) bool {
	left.RecoveredAt = time.Time{}
	right.RecoveredAt = time.Time{}
	return reflect.DeepEqual(left, right)
}

func validateUnknownRecovery(recovery domain.UnknownAssignmentRecovery) error {
	if strings.TrimSpace(recovery.ID) != recovery.ID || recovery.ID == "" ||
		strings.TrimSpace(recovery.AssignmentID) != recovery.AssignmentID || recovery.AssignmentID == "" ||
		recovery.CoordinatorEpoch < 1 || recovery.ExpectedAssignmentEpoch < 1 || recovery.ExpectedAttemptRevision < 0 ||
		strings.TrimSpace(recovery.EvidenceID) != recovery.EvidenceID || recovery.EvidenceID == "" ||
		strings.TrimSpace(recovery.Actor) != recovery.Actor || recovery.Actor == "" ||
		strings.TrimSpace(recovery.Reason) != recovery.Reason || recovery.Reason == "" || recovery.RecoveredAt.IsZero() {
		return ErrInvalidUnknownRecovery
	}
	if recovery.Outcome != domain.UnknownRecoveryStopped && recovery.Outcome != domain.UnknownRecoveryFailed {
		return ErrInvalidUnknownRecovery
	}
	if len(recovery.EvidenceSHA256) != 64 {
		return ErrInvalidUnknownRecovery
	}
	if _, err := hex.DecodeString(recovery.EvidenceSHA256); err != nil {
		return ErrInvalidUnknownRecovery
	}
	return nil
}
