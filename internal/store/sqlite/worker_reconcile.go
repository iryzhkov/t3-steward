package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var ErrStaleWorkerStateTransition = errors.New("stale worker state transition")

// CommitWorkerStateTransitions atomically applies assignment and attempt
// projections derived from one exact, fresh worker snapshot.
func (s *Store) CommitWorkerStateTransitions(
	ctx context.Context,
	transitions []domain.WorkerStateTransition,
) ([]domain.Assignment, error) {
	if len(transitions) == 0 {
		return nil, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin worker state transitions: %w", err)
	}
	defer tx.Rollback()

	seenAssignments := make(map[string]struct{}, len(transitions))
	seenAttempts := make(map[string]struct{}, len(transitions))
	applied := make([]domain.Assignment, 0, len(transitions))
	for _, transition := range transitions {
		if err := validateWorkerStateTransition(transition); err != nil {
			return nil, err
		}
		if _, exists := seenAssignments[transition.Assignment.ID]; exists {
			return nil, fmt.Errorf("worker state transitions repeat assignment %q", transition.Assignment.ID)
		}
		if _, exists := seenAttempts[transition.Attempt.ID]; exists {
			return nil, fmt.Errorf("worker state transitions repeat attempt %q", transition.Attempt.ID)
		}
		seenAssignments[transition.Assignment.ID] = struct{}{}
		seenAttempts[transition.Attempt.ID] = struct{}{}

		if err := requireCoordinatorEpoch(ctx, tx, transition.CoordinatorEpoch); err != nil {
			return nil, err
		}
		snapshot, exists, err := loadWorkerSnapshotTx(ctx, tx, transition.WorkerID)
		if err != nil {
			return nil, err
		}
		if !exists || snapshot.WorkerEpoch != transition.WorkerEpoch ||
			snapshot.CoordinatorEpoch != transition.CoordinatorEpoch ||
			snapshot.Sequence != transition.WorkerSequence ||
			!snapshot.Connected || !snapshot.ValidUntil.After(transition.TransitionedAt) {
			return nil, fmt.Errorf("%w: worker %q snapshot changed or expired", ErrStaleWorkerStateTransition, transition.WorkerID)
		}

		currentAssignment, err := loadAssignmentTx(ctx, tx, transition.Assignment.ID)
		if err != nil {
			return nil, err
		}
		currentAttempt, err := loadAttemptTx(ctx, tx, transition.Attempt.ID)
		if err != nil {
			return nil, err
		}
		if reflect.DeepEqual(currentAssignment, transition.Assignment) &&
			reflect.DeepEqual(currentAttempt, transition.Attempt) {
			if err := requireNativeAuditEventTx(ctx, tx, workerStateAuditID(transition)); err != nil {
				return nil, err
			}
			applied = append(applied, currentAssignment)
			continue
		}
		if !reflect.DeepEqual(currentAssignment, transition.ExpectedAssignment) {
			return nil, fmt.Errorf("%w: assignment %q changed concurrently", ErrStaleWorkerStateTransition, transition.Assignment.ID)
		}
		if currentAttempt.Revision != transition.ExpectedAttemptRevision ||
			currentAttempt.ID != transition.Attempt.ID ||
			currentAttempt.ID != transition.ExpectedAssignment.AttemptID {
			return nil, fmt.Errorf("%w: attempt %q expected revision %d, current %d",
				ErrStaleWorkerStateTransition, transition.Attempt.ID,
				transition.ExpectedAttemptRevision, currentAttempt.Revision)
		}

		if transition.Assignment.State == domain.AssignmentReleased &&
			(transition.Attempt.AssignmentID != "" || unfinishedWorkerPark(currentAttempt)) {
			if !durableParkReleaseHandoff(currentAttempt, currentAssignment, transition) {
				return nil, fmt.Errorf("%w: released assignment does not preserve current parked custody", ErrStaleWorkerStateTransition)
			}
		}

		if transition.Assignment.State == domain.AssignmentClaimed &&
			(currentAttempt.Control == domain.ControlDraining || currentAttempt.Control == domain.ControlPaused ||
				currentAttempt.Control == domain.ControlPausedUncheckpointed || currentAttempt.Control == domain.ControlResuming) &&
			(transition.Attempt.Control == domain.ControlPreparing || transition.Attempt.Control == domain.ControlRunning ||
				transition.Attempt.Control == domain.ControlResuming && currentAttempt.Control != domain.ControlResuming) {
			return nil, fmt.Errorf("%w: worker projection cannot resume coordinator intent", ErrStaleWorkerStateTransition)
		}
		if transition.Reason == "worker-observed-stopped" || transition.Reason == "stop-accepted" ||
			transition.Assignment.State == domain.AssignmentReleased &&
				(currentAttempt.Control == domain.ControlDraining || currentAttempt.Control == domain.ControlPaused ||
					currentAttempt.Control == domain.ControlPausedUncheckpointed || currentAttempt.Control == domain.ControlResuming ||
					currentAttempt.Progress == domain.ProgressCancelled) {
			proven := false
			for _, observation := range snapshot.Assignments {
				if observation.AssignmentID == currentAssignment.ID && observation.AssignmentEpoch == currentAssignment.Epoch &&
					observation.State == domain.AssignmentReleased && snapshot.WorkerEpoch == currentAssignment.WorkerEpoch &&
					observation.ThreadID == currentAssignment.ThreadID {
					proven = true
				}
			}
			if !proven {
				return nil, fmt.Errorf("%w: stopped observation execution identity changed", ErrStaleWorkerStateTransition)
			}
		}
		// Attention-stop owns its exact cancelled revision until its stop observation.
		// Normal worker custody updates must not invalidate that existing fence.
		if currentAttempt.Progress == domain.ProgressCancelled && currentAttempt.Control == domain.ControlDraining {
			records, err := loadThrottleAttemptRecordsTx(ctx, tx)
			if err != nil {
				return nil, err
			}
			pendingAttentionStop := false
			for _, record := range records {
				binding := record.Command.AttentionStop
				if binding != nil && record.AttemptID == currentAttempt.ID &&
					binding.AppliedRevision == currentAttempt.Revision &&
					binding.CoordinatorEpoch == transition.CoordinatorEpoch &&
					binding.WorkflowRunID == currentAttempt.WorkflowRunID && binding.TaskID == currentAttempt.TaskID &&
					record.Command.AttemptID == currentAttempt.ID &&
					reflect.DeepEqual(record.Command.Route, currentAssignment.Route) &&
					record.Command.AssignmentID == currentAssignment.ID &&
					record.Command.AssignmentEpoch == currentAssignment.Epoch &&
					record.Command.WorkerID == currentAssignment.WorkerID &&
					record.Command.ThreadID == currentAssignment.ThreadID &&
					binding.CommandDigest != "" && binding.CommandDigest == domain.AttentionStopCommandDigest(record.Command) {
					pendingAttentionStop = true
					break
				}
			}
			if pendingAttentionStop {
				applied = append(applied, currentAssignment)
				continue
			}
		}

		// Worker evidence may settle custody, but cannot rewrite a finished
		// attempt or discard completion and artifact evidence.
		if currentAttempt.Progress.Terminal() || currentAttempt.CompletedAt != nil {
			next := transition.Attempt
			preservedStopIntent := transition.Assignment.State == domain.AssignmentClaimed &&
				next.Control == currentAttempt.Control &&
				(currentAttempt.Control == domain.ControlDraining || currentAttempt.Control == domain.ControlPaused ||
					currentAttempt.Control == domain.ControlPausedUncheckpointed)
			if next.Progress != currentAttempt.Progress || next.Control != domain.ControlStopped && !preservedStopIntent ||
				!reflect.DeepEqual(next.CompletedAt, currentAttempt.CompletedAt) ||
				next.Failure != currentAttempt.Failure ||
				next.CheckpointArtifactID != currentAttempt.CheckpointArtifactID ||
				next.FinalSummaryArtifactID != currentAttempt.FinalSummaryArtifactID ||
				!reflect.DeepEqual(next.ReviewVerdict, currentAttempt.ReviewVerdict) {
				return nil, fmt.Errorf("%w: finished attempt %q cannot be projected live or lose evidence",
					ErrStaleWorkerStateTransition, currentAttempt.ID)
			}
		}

		nextRaw, err := json.Marshal(transition.Assignment)
		if err != nil {
			return nil, fmt.Errorf("encode assignment transition %q: %w", transition.Assignment.ID, err)
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE coordinator_assignments
			SET worker_epoch = ?, assignment_state = ?, dispatch_state = ?,
			    lease_expires_at = ?, record = ?
			WHERE id = ? AND worker_id = ? AND worker_epoch = ?
			  AND assignment_epoch = ? AND assignment_state = ?
		
		`, transition.Assignment.WorkerEpoch, transition.Assignment.State,
			transition.Assignment.DispatchState,
			transition.Assignment.LeaseExpiresAt.UTC().Format(time.RFC3339Nano),
			string(nextRaw), transition.ExpectedAssignment.ID, transition.ExpectedAssignment.WorkerID,
			transition.ExpectedAssignment.WorkerEpoch, transition.ExpectedAssignment.Epoch,
			transition.ExpectedAssignment.State)
		if err != nil {
			return nil, fmt.Errorf("update worker assignment state %q: %w", transition.Assignment.ID, err)
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			return nil, fmt.Errorf("%w: assignment %q changed concurrently", ErrStaleWorkerStateTransition, transition.Assignment.ID)
		}
		if err := updateAttemptTx(ctx, tx, transition.Attempt, transition.ExpectedAttemptRevision); err != nil {
			if errors.Is(err, ErrStaleAttemptRevision) {
				return nil, fmt.Errorf("%w: %v", ErrStaleWorkerStateTransition, err)
			}
			return nil, err
		}
		if _, err := insertWorkerStateAuditEvent(ctx, tx, transition); err != nil {
			return nil, err
		}
		applied = append(applied, transition.Assignment)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit worker state transitions: %w", err)
	}
	return applied, nil
}

// unfinishedWorkerPark identifies a candidate shape only. Permission to retain
// a Released binding requires durableParkReleaseHandoff against CURRENT rows.
func unfinishedWorkerPark(attempt domain.Attempt) bool {
	return !attempt.Progress.Terminal() && attempt.CompletedAt == nil &&
		(attempt.Progress == domain.ProgressWaitingExternal || attempt.Control == domain.ControlWaitingExternal)
}

// durableParkReleaseHandoff runs after exact assignment/revision fences in the
// same transaction. The wait owner needs this binding to detect abandonment and
// revoke authority. Release may change only custody state/lease/time and advance
// the attempt revision/time; every other field remains durable evidence.
// No worker epoch transfer is required for a historical release receipt.
func durableParkReleaseHandoff(current domain.Attempt, custody domain.Assignment, transition domain.WorkerStateTransition) bool {
	if !unfinishedWorkerPark(current) || current.AssignmentID != custody.ID ||
		transition.Attempt.AssignmentID != custody.ID || !transition.Assignment.LeaseExpiresAt.IsZero() ||
		!transition.Attempt.UpdatedAt.Equal(transition.TransitionedAt) ||
		!transition.Assignment.UpdatedAt.Equal(transition.TransitionedAt) {
		return false
	}
	nextAttempt := transition.Attempt
	nextAttempt.Revision = current.Revision
	nextAttempt.UpdatedAt = current.UpdatedAt
	nextCustody := transition.Assignment
	nextCustody.State = custody.State
	nextCustody.LeaseExpiresAt = custody.LeaseExpiresAt
	nextCustody.UpdatedAt = custody.UpdatedAt
	return reflect.DeepEqual(nextAttempt, current) && reflect.DeepEqual(nextCustody, custody)
}

func workerStateAuditID(transition domain.WorkerStateTransition) string {
	return fmt.Sprintf("worker-state:%s:%d", transition.Assignment.ID, transition.Attempt.Revision)
}

func insertWorkerStateAuditEvent(ctx context.Context, tx *sql.Tx, transition domain.WorkerStateTransition) (domain.AuditEvent, error) {
	identity := workerStateAuditID(transition)
	return insertNativeAuditEventTx(ctx, tx, nativeAuditInput{
		ID: identity, Kind: "worker-state-" + string(transition.Assignment.State),
		WorkflowRunID: transition.Attempt.WorkflowRunID, TaskID: transition.Attempt.TaskID,
		AttemptID: transition.Attempt.ID, TargetType: domain.AdminTargetAssignment,
		TargetID: transition.Assignment.ID, Actor: "worker:" + transition.WorkerID,
		Reason: transition.Reason, CreatedAt: transition.TransitionedAt,
		Detail: nativeAuditDetail{CoordinatorEpoch: transition.CoordinatorEpoch, WorkerEpoch: transition.WorkerEpoch,
			AssignmentEpoch: transition.Assignment.Epoch, WorkerSequence: transition.WorkerSequence,
			ExpectedRevision: transition.ExpectedAttemptRevision, Revision: transition.Attempt.Revision,
			IdempotencyIdentity: identity, Outcome: string(transition.Assignment.State)},
	})
}

func validateWorkerStateTransition(transition domain.WorkerStateTransition) error {
	expected := transition.ExpectedAssignment
	next := transition.Assignment
	attempt := transition.Attempt
	if transition.CoordinatorEpoch < 1 || transition.WorkerID == "" ||
		transition.WorkerEpoch == "" || transition.WorkerSequence < 1 ||
		transition.TransitionedAt.IsZero() || transition.Reason == "" {
		return fmt.Errorf("%w: incomplete worker snapshot identity", ErrStaleWorkerStateTransition)
	}
	if expected.ID == "" || expected.AttemptID == "" || expected.WorkerID != transition.WorkerID ||
		expected.Epoch < 1 ||
		(expected.State != domain.AssignmentClaimed && expected.State != domain.AssignmentUnknown) {
		return fmt.Errorf("%w: invalid expected assignment", ErrStaleWorkerStateTransition)
	}
	if next.ID != expected.ID || next.AttemptID != expected.AttemptID ||
		next.WorkerID != expected.WorkerID || next.Epoch != expected.Epoch ||
		next.LeaseToken != expected.LeaseToken || next.DispatchToken != expected.DispatchToken {
		return fmt.Errorf("%w: assignment identity changed", ErrStaleWorkerStateTransition)
	}
	switch next.State {
	case domain.AssignmentClaimed, domain.AssignmentUnknown, domain.AssignmentReleased, domain.AssignmentCompleted:
	default:
		return fmt.Errorf("%w: invalid next assignment state %q", ErrStaleWorkerStateTransition, next.State)
	}
	if (next.State == domain.AssignmentClaimed || next.State == domain.AssignmentUnknown) &&
		next.WorkerEpoch != transition.WorkerEpoch {
		return fmt.Errorf("%w: recovered assignment is not bound to current worker epoch", ErrStaleWorkerStateTransition)
	}
	if attempt.ID != expected.AttemptID || attempt.AssignmentID != next.ID && next.State != domain.AssignmentReleased ||
		attempt.Revision != transition.ExpectedAttemptRevision+1 {
		return fmt.Errorf("%w: invalid attempt projection", ErrStaleWorkerStateTransition)
	}
	if next.State == domain.AssignmentReleased && attempt.AssignmentID != "" &&
		(attempt.AssignmentID != next.ID || !unfinishedWorkerPark(attempt)) {
		return fmt.Errorf("%w: released assignment remains attached to attempt", ErrStaleWorkerStateTransition)
	}
	return nil
}
