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
// cannot abandon anything. Each release is one audit event.
//
// The offer's attempt ends in the same transaction. An activation attempt is
// never planned again once its offer is gone -- a later wake is a new attempt
// at a new epoch -- so an attempt left ready/unassigned beside a released
// assignment is not waiting for anything; it is an inconsistency that coordinator
// planning and quota planning both reported on every boundary (rc.96 released
// the offer and left the attempt, and logged it 374 times in 30 minutes). The
// same sweep repairs that state: an already released activation assignment
// whose never-started attempt is still nonterminal and whose activation is dead
// by the same judgement has its attempt ended too.
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
	// Only activation attempts are read. A released assignment is read only
	// while its attempt still says unassigned, which is the never-started
	// shape the repair looks for; every released task assignment and every
	// activation attempt already ended is left out of the scan.
	rows, err := tx.QueryContext(ctx, `SELECT assignment.record FROM coordinator_assignments AS assignment
		JOIN coordinator_attempts AS attempt ON attempt.id = assignment.attempt_id
		WHERE COALESCE(json_extract(attempt.record, '$.supervisionActivationId'), '') != ''
		  AND (assignment.assignment_state = ?
		    OR (assignment.assignment_state = ? AND json_extract(attempt.record, '$.control') = ?))
		ORDER BY assignment.id`,
		string(domain.AssignmentOffered), string(domain.AssignmentReleased), string(domain.ControlUnassigned))
	if err != nil {
		return nil, fmt.Errorf("load activation assignments: %w", err)
	}
	var candidates []domain.Assignment
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			rows.Close()
			return nil, err
		}
		var assignment domain.Assignment
		if err := json.Unmarshal(raw, &assignment); err != nil {
			rows.Close()
			return nil, fmt.Errorf("decode activation assignment: %w", err)
		}
		candidates = append(candidates, assignment)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var released []string
	for _, assignment := range candidates {
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
		if assignment.State == domain.AssignmentReleased {
			if err := endUnstartedActivationAttemptTx(ctx, tx, attempt, assignment, coordinatorEpoch,
				"the activation attempt's offer was released and its activation ended: "+why, now); err != nil {
				return nil, err
			}
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
		auditID := fmt.Sprintf("activation-offer-released:%s:epoch:%d", assignment.ID, assignment.Epoch)
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
		if err := endUnstartedActivationAttemptTx(ctx, tx, attempt, next, coordinatorEpoch,
			"the activation attempt's unclaimed offer outlived its activation: "+why, now); err != nil {
			return nil, err
		}
		released = append(released, assignment.ID)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit dead activation offer release: %w", err)
	}
	return released, nil
}

// endUnstartedActivationAttemptTx cancels the attempt of an activation offer
// that has been released without ever being claimed, and reports nothing when
// the attempt is already terminal or has started.
//
// Cancelled and stopped is what cancelling an unfinished task writes
// (DAGExecution.CancelTask): the work did not fail, it will never run. Failure
// is left empty for the same reason, and the reason is carried by the audit
// event instead. The attempt keeps its assignment reference, so the released
// assignment stays the record of the offer it was.
//
// Only a never-started attempt is ended here: one still unassigned, whose
// assignment is the released one. A claim moves the attempt out of unassigned
// in the claim transaction, so an attempt that ever reached a worker is never
// touched, and a released assignment is required so that a live offer is never
// orphaned from the attempt it belongs to.
func endUnstartedActivationAttemptTx(
	ctx context.Context,
	tx *sql.Tx,
	attempt domain.Attempt,
	assignment domain.Assignment,
	coordinatorEpoch int64,
	reason string,
	now time.Time,
) error {
	if !attempt.IsSupervisionActivation() || attempt.Progress.Terminal() ||
		attempt.Control != domain.ControlUnassigned || attempt.AssignmentID != assignment.ID ||
		assignment.AttemptID != attempt.ID || assignment.State != domain.AssignmentReleased {
		return nil
	}
	expected := attempt.Revision
	completed := now.UTC()
	attempt.Progress = domain.ProgressCancelled
	attempt.Control = domain.ControlStopped
	attempt.Failure = ""
	attempt.Revision++
	attempt.UpdatedAt = completed
	attempt.CompletedAt = &completed
	if err := updateAttemptTx(ctx, tx, attempt, expected); err != nil {
		return err
	}
	auditID := "activation-attempt-cancelled:" + attempt.ID
	_, err := insertNativeAuditEventTx(ctx, tx, nativeAuditInput{
		ID: auditID, Kind: "attempt-cancelled", WorkflowRunID: attempt.WorkflowRunID,
		TaskID: attempt.TaskID, AttemptID: attempt.ID,
		TargetType: domain.AdminTargetAttempt, TargetID: attempt.ID,
		Actor: "coordinator", Reason: reason, CreatedAt: completed,
		Detail: nativeAuditDetail{CoordinatorEpoch: coordinatorEpoch, AssignmentEpoch: assignment.Epoch,
			ExpectedRevision: expected, Revision: attempt.Revision,
			IdempotencyIdentity: auditID, Outcome: string(domain.ProgressCancelled)},
	})
	return err
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
	// Raising the epoch writes a new activation row and leaves the old one as
	// it was, so the old row can still say pending-dispatch at its own epoch.
	// The run's supervision record holds the current epoch.
	var current int64
	err = tx.QueryRowContext(ctx, `SELECT activation_epoch FROM coordinator_supervision WHERE run_id = ?`,
		attempt.WorkflowRunID).Scan(&current)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, "", fmt.Errorf("load supervision of run %q: %w", attempt.WorkflowRunID, err)
	}
	if err == nil && attempt.SupervisionActivationEpoch < current {
		return true, fmt.Sprintf("the run's activation moved from epoch %d to %d", attempt.SupervisionActivationEpoch, current), nil
	}
	switch domain.ActivationState(state) {
	case domain.ActivationClosed, domain.ActivationSpent, domain.ActivationRevoked:
		return true, "the activation is " + state, nil
	}
	return false, "", nil
}
