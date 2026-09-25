package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// ReleaseDeadActivationOffers releases every overseer activation offer that no
// worker ever claimed and that no activation can use any more: its activation
// is closed, spent or revoked, is missing, or has moved to another epoch. It
// returns the released assignment IDs.
//
// Closing an activation clears its lease but never touched its outstanding
// offer. An offer that was not claimed before the activation closed stayed
// "offered" forever, and a retained assignment refuses every coordinator
// reload that changes its worker's catalog (S13: an overseer offer to homelab
// from 2026-09-22 blocked every later catalog change on homelab). An offered
// assignment was never claimed, so no worker holds work for it and releasing it
// cannot abandon anything. Only the assignment changes, exactly as when an
// operator reassessment supersedes an undelivered offer; each release is one
// audit event.
func (s *Store) ReleaseDeadActivationOffers(ctx context.Context, coordinatorEpoch int64, now time.Time) ([]string, error) {
	if coordinatorEpoch < 1 || now.IsZero() {
		return nil, errors.New("release dead activation offers: coordinator epoch and time are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin dead activation offer release: %w", err)
	}
	defer tx.Rollback()
	if err := requireCoordinatorEpoch(ctx, tx, coordinatorEpoch); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT record FROM coordinator_assignments WHERE assignment_state = ?`,
		string(domain.AssignmentOffered))
	if err != nil {
		return nil, fmt.Errorf("load offered assignments: %w", err)
	}
	var offered []domain.Assignment
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			rows.Close()
			return nil, err
		}
		var assignment domain.Assignment
		if err := json.Unmarshal(raw, &assignment); err != nil {
			rows.Close()
			return nil, fmt.Errorf("decode offered assignment: %w", err)
		}
		offered = append(offered, assignment)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var released []string
	for _, assignment := range offered {
		attempt, err := loadAttemptTx(ctx, tx, assignment.AttemptID)
		if err != nil {
			return nil, err
		}
		if !attempt.IsSupervisionActivation() {
			continue
		}
		dead, why, err := activationOfferIsDeadTx(ctx, tx, attempt)
		if err != nil {
			return nil, err
		}
		if !dead {
			continue
		}
		next := assignment
		next.State = domain.AssignmentReleased
		next.LeaseExpiresAt = time.Time{}
		next.UpdatedAt = now.UTC()
		raw, err := json.Marshal(next)
		if err != nil {
			return nil, fmt.Errorf("encode released activation offer %q: %w", next.ID, err)
		}
		result, err := tx.ExecContext(ctx, `UPDATE coordinator_assignments
			SET assignment_state = ?, lease_expires_at = '', record = ?
			WHERE id = ? AND assignment_epoch = ? AND assignment_state = ?`,
			next.State, raw, assignment.ID, assignment.Epoch, domain.AssignmentOffered)
		if err != nil {
			return nil, fmt.Errorf("release dead activation offer %q: %w", assignment.ID, err)
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			continue
		}
		auditID := "activation-offer-released:" + assignment.ID
		if _, err := insertNativeAuditEventTx(ctx, tx, nativeAuditInput{
			ID: auditID, Kind: "assignment-released", WorkflowRunID: attempt.WorkflowRunID,
			AttemptID: attempt.ID, TargetType: domain.AdminTargetAssignment, TargetID: assignment.ID,
			Actor: "coordinator", Reason: "an unclaimed overseer offer outlived its activation: " + why,
			CreatedAt: now.UTC(),
			Detail: nativeAuditDetail{CoordinatorEpoch: coordinatorEpoch, AssignmentEpoch: assignment.Epoch,
				ExpectedRevision:    attempt.SupervisionActivationEpoch,
				IdempotencyIdentity: auditID, Outcome: string(domain.AssignmentReleased)},
		}); err != nil {
			return nil, err
		}
		released = append(released, assignment.ID)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit dead activation offer release: %w", err)
	}
	return released, nil
}

// activationOfferIsDeadTx reports whether the activation an offered attempt
// belongs to can no longer use the offer. Pending dispatch, active, escalated
// and recovery-required activations keep theirs: those are the states an offer
// is made in, claimed in, or recovered from.
func activationOfferIsDeadTx(ctx context.Context, tx *sql.Tx, attempt domain.Attempt) (bool, string, error) {
	var state string
	var epoch int64
	err := tx.QueryRowContext(ctx, `SELECT state, epoch FROM coordinator_supervision_activations WHERE id = ?`,
		attempt.SupervisionActivationID).Scan(&state, &epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return true, "the activation no longer exists", nil
	}
	if err != nil {
		return false, "", fmt.Errorf("load activation %q: %w", attempt.SupervisionActivationID, err)
	}
	if epoch != attempt.SupervisionActivationEpoch {
		return true, fmt.Sprintf("the activation moved from epoch %d to %d", attempt.SupervisionActivationEpoch, epoch), nil
	}
	switch domain.ActivationState(state) {
	case domain.ActivationClosed, domain.ActivationSpent, domain.ActivationRevoked:
		return true, "the activation is " + state, nil
	}
	return false, "", nil
}
