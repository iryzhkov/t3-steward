package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const coordinatorMigrationV7 = `
ALTER TABLE coordinator_assignments ADD COLUMN dispatch_revision INTEGER NOT NULL DEFAULT 0;
ALTER TABLE coordinator_assignments ADD COLUMN dispatch_state TEXT NOT NULL DEFAULT '';
UPDATE coordinator_assignments
SET dispatch_revision = CASE
		WHEN COALESCE(json_extract(record, '$.dispatchRevision'), 0) > 0
			THEN CAST(json_extract(record, '$.dispatchRevision') AS INTEGER)
		ELSE 1
	END,
	dispatch_state = CASE
		WHEN COALESCE(json_extract(record, '$.dispatchState'), '') <> ''
			THEN json_extract(record, '$.dispatchState')
		WHEN COALESCE(json_extract(record, '$.threadId'), '') <> ''
			THEN 'confirmed'
		ELSE 'prepared'
	END,
	record = json_set(
		record,
		'$.dispatchRevision',
		CASE
			WHEN COALESCE(json_extract(record, '$.dispatchRevision'), 0) > 0
				THEN CAST(json_extract(record, '$.dispatchRevision') AS INTEGER)
			ELSE 1
		END,
		'$.dispatchState',
		CASE
			WHEN COALESCE(json_extract(record, '$.dispatchState'), '') <> ''
				THEN json_extract(record, '$.dispatchState')
			WHEN COALESCE(json_extract(record, '$.threadId'), '') <> ''
				THEN 'confirmed'
			ELSE 'prepared'
		END,
		'$.dispatchConfirmedAt',
		CASE
			WHEN COALESCE(json_extract(record, '$.threadId'), '') <> ''
				THEN COALESCE(
					json_extract(record, '$.dispatchConfirmedAt'),
					json_extract(record, '$.updatedAt')
				)
			ELSE json_extract(record, '$.dispatchConfirmedAt')
		END
	);
CREATE INDEX coordinator_assignments_dispatch
	ON coordinator_assignments(dispatch_state, id);
`

// ErrStaleAssignmentDispatchRevision means dispatch knowledge changed after the
// coordinator loaded it.
var ErrStaleAssignmentDispatchRevision = errors.New("stale assignment dispatch revision")

// PrepareAssignmentDispatch persists deterministic dispatch identity before any
// worker call. Repeated preparation returns the current durable assignment.
func (s *Store) PrepareAssignmentDispatch(ctx context.Context, input domain.Assignment) (domain.Assignment, error) {
	if err := validatePreparedAssignment(input); err != nil {
		return domain.Assignment{}, err
	}
	prepared := input
	prepared.DispatchState = domain.DispatchPrepared
	prepared.DispatchRevision = 1
	prepared.DispatchConfirmedAt = nil
	prepared.DispatchError = ""

	raw, err := json.Marshal(prepared)
	if err != nil {
		return domain.Assignment{}, fmt.Errorf("encode assignment dispatch %q: %w", prepared.ID, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Assignment{}, fmt.Errorf("begin assignment dispatch preparation: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO coordinator_assignments(
			id, attempt_id, dispatch_token, dispatch_revision, dispatch_state, record
		) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
		prepared.ID, prepared.AttemptID, prepared.DispatchToken,
		prepared.DispatchRevision, prepared.DispatchState, raw,
	); err != nil {
		return domain.Assignment{}, fmt.Errorf("prepare assignment dispatch %q: %w", prepared.ID, err)
	}
	current, err := loadAssignmentDispatchTx(ctx, tx, prepared.ID)
	if err != nil {
		return domain.Assignment{}, err
	}
	if !sameDispatchIdentity(current, prepared) {
		return domain.Assignment{}, fmt.Errorf("assignment dispatch %q identity conflicts with durable record", prepared.ID)
	}
	if err := tx.Commit(); err != nil {
		return domain.Assignment{}, fmt.Errorf("commit assignment dispatch preparation %q: %w", prepared.ID, err)
	}
	return current, nil
}

// LoadAssignmentDispatch loads one assignment's current dispatch projection.
func (s *Store) LoadAssignmentDispatch(ctx context.Context, assignmentID string) (domain.Assignment, error) {
	var raw string
	if err := s.db.QueryRowContext(ctx,
		`SELECT record FROM coordinator_assignments WHERE id = ?`, assignmentID,
	).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.Assignment{}, fmt.Errorf("assignment dispatch %q not found", assignmentID)
		}
		return domain.Assignment{}, fmt.Errorf("load assignment dispatch %q: %w", assignmentID, err)
	}
	return decodeAssignmentDispatch(assignmentID, raw)
}

// CommitAssignmentDispatch advances one durable dispatch projection. Exact
// replay is a successful no-op.
func (s *Store) CommitAssignmentDispatch(ctx context.Context, transition domain.AssignmentDispatchTransition) error {
	next := transition.Assignment
	if transition.ExpectedRevision < 1 || next.DispatchRevision != transition.ExpectedRevision+1 {
		return fmt.Errorf("assignment dispatch %q has invalid revision %d after %d",
			next.ID, next.DispatchRevision, transition.ExpectedRevision)
	}
	if err := validateDispatchProjection(next); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin assignment dispatch transition: %w", err)
	}
	defer tx.Rollback()
	current, err := loadAssignmentDispatchTx(ctx, tx, next.ID)
	if err != nil {
		return err
	}
	if reflect.DeepEqual(current, next) {
		return tx.Commit()
	}
	if current.DispatchRevision != transition.ExpectedRevision {
		return staleAssignmentDispatchError(next.ID, transition.ExpectedRevision, current.DispatchRevision)
	}
	if !sameDispatchIdentity(current, next) {
		return fmt.Errorf("assignment dispatch %q transition changes immutable identity", next.ID)
	}
	if !validDispatchAdvance(current.DispatchState, next.DispatchState) {
		return fmt.Errorf("assignment dispatch %q cannot transition from %q to %q",
			next.ID, current.DispatchState, next.DispatchState)
	}

	raw, err := json.Marshal(next)
	if err != nil {
		return fmt.Errorf("encode assignment dispatch %q: %w", next.ID, err)
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE coordinator_assignments
		 SET dispatch_revision = ?, dispatch_state = ?, record = ?
		 WHERE id = ? AND dispatch_revision = ?`,
		next.DispatchRevision, next.DispatchState, raw, next.ID, transition.ExpectedRevision,
	)
	if err != nil {
		return fmt.Errorf("save assignment dispatch %q: %w", next.ID, err)
	}
	if changed, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("count assignment dispatch update %q: %w", next.ID, err)
	} else if changed != 1 {
		return staleAssignmentDispatchError(next.ID, transition.ExpectedRevision, current.DispatchRevision)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit assignment dispatch %q: %w", next.ID, err)
	}
	return nil
}

func loadAssignmentDispatchTx(ctx context.Context, tx *sql.Tx, assignmentID string) (domain.Assignment, error) {
	var raw string
	if err := tx.QueryRowContext(ctx,
		`SELECT record FROM coordinator_assignments WHERE id = ?`, assignmentID,
	).Scan(&raw); err != nil {
		return domain.Assignment{}, fmt.Errorf("load assignment dispatch %q: %w", assignmentID, err)
	}
	return decodeAssignmentDispatch(assignmentID, raw)
}

func decodeAssignmentDispatch(assignmentID, raw string) (domain.Assignment, error) {
	var assignment domain.Assignment
	if err := json.Unmarshal([]byte(raw), &assignment); err != nil {
		return domain.Assignment{}, fmt.Errorf("decode assignment dispatch %q: %w", assignmentID, err)
	}
	return assignment, nil
}

func validatePreparedAssignment(assignment domain.Assignment) error {
	if strings.TrimSpace(assignment.ID) != assignment.ID || assignment.ID == "" ||
		strings.TrimSpace(assignment.AttemptID) != assignment.AttemptID || assignment.AttemptID == "" ||
		strings.TrimSpace(assignment.WorkerID) != assignment.WorkerID || assignment.WorkerID == "" ||
		strings.TrimSpace(assignment.DispatchToken) != assignment.DispatchToken || assignment.DispatchToken == "" ||
		strings.TrimSpace(assignment.ThreadID) != assignment.ThreadID || assignment.ThreadID == "" {
		return fmt.Errorf("assignment dispatch requires trimmed assignment, attempt, worker, token, and thread IDs")
	}
	if assignment.State != domain.AssignmentClaimed {
		return fmt.Errorf("assignment dispatch %q must be claimed before preparation", assignment.ID)
	}
	if assignment.DispatchRevision != 0 || assignment.DispatchState != "" ||
		assignment.DispatchConfirmedAt != nil || assignment.DispatchError != "" {
		return fmt.Errorf("assignment dispatch %q preparation requires empty dispatch state", assignment.ID)
	}
	return nil
}

func validateDispatchProjection(assignment domain.Assignment) error {
	if assignment.ID == "" || assignment.AttemptID == "" || assignment.WorkerID == "" ||
		assignment.DispatchToken == "" || assignment.ThreadID == "" {
		return fmt.Errorf("assignment dispatch has incomplete identity")
	}
	switch assignment.DispatchState {
	case domain.DispatchPrepared, domain.DispatchCreating:
		if assignment.State != domain.AssignmentClaimed {
			return fmt.Errorf("assignment dispatch %q state %q requires claimed assignment", assignment.ID, assignment.DispatchState)
		}
	case domain.DispatchConfirmed:
		if assignment.State != domain.AssignmentClaimed || assignment.DispatchConfirmedAt == nil {
			return fmt.Errorf("confirmed assignment dispatch %q requires claimed assignment and confirmation time", assignment.ID)
		}
	case domain.DispatchUnknown:
		if assignment.State != domain.AssignmentUnknown {
			return fmt.Errorf("unknown assignment dispatch %q requires unknown assignment state", assignment.ID)
		}
	case domain.DispatchStopped:
		if assignment.State != domain.AssignmentReleased {
			return fmt.Errorf("stopped assignment dispatch %q requires released assignment state", assignment.ID)
		}
	default:
		return fmt.Errorf("assignment dispatch %q has invalid state %q", assignment.ID, assignment.DispatchState)
	}
	return nil
}

func sameDispatchIdentity(left, right domain.Assignment) bool {
	return left.ID == right.ID &&
		left.AttemptID == right.AttemptID &&
		left.WorkerID == right.WorkerID &&
		reflect.DeepEqual(left.Route, right.Route) &&
		left.Epoch == right.Epoch &&
		left.LeaseToken == right.LeaseToken &&
		left.LeaseExpiresAt.Equal(right.LeaseExpiresAt) &&
		left.DispatchToken == right.DispatchToken &&
		left.ThreadID == right.ThreadID &&
		left.CreatedAt.Equal(right.CreatedAt)
}

func validDispatchAdvance(from, to domain.DispatchState) bool {
	switch from {
	case domain.DispatchPrepared:
		return to == domain.DispatchCreating || to == domain.DispatchConfirmed ||
			to == domain.DispatchUnknown || to == domain.DispatchStopped
	case domain.DispatchCreating:
		return to == domain.DispatchConfirmed || to == domain.DispatchUnknown || to == domain.DispatchStopped
	case domain.DispatchUnknown:
		return to == domain.DispatchCreating || to == domain.DispatchConfirmed ||
			to == domain.DispatchUnknown || to == domain.DispatchStopped
	case domain.DispatchConfirmed:
		return to == domain.DispatchUnknown || to == domain.DispatchStopped
	default:
		return false
	}
}

func staleAssignmentDispatchError(assignmentID string, expected, actual int64) error {
	return fmt.Errorf("%w for assignment %q: expected %d, current %d",
		ErrStaleAssignmentDispatchRevision, assignmentID, expected, actual)
}
