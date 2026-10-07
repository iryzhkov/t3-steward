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
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// V33 is append-only. Null is an explicit, durable omission, not an absent decision.
const coordinatorMigrationV33 = `
CREATE TABLE IF NOT EXISTS coordinator_assignment_displays (
 assignment_id TEXT NOT NULL,
 assignment_epoch INTEGER NOT NULL CHECK(assignment_epoch > 0),
 binding TEXT NOT NULL,
 display TEXT NOT NULL CHECK(json_valid(display)),
 PRIMARY KEY(assignment_id, assignment_epoch)
);
`

// FreezeAssignmentDisplay commits the first proposal before an offer can escape.
// It serializes with assignment writes, fences coordinator authority and binds the
// decision to the actual durable assignment and execution identity. Only lease
// extension/update timestamps may vary. Inventory is never read in this transaction.
func (s *Store) FreezeAssignmentDisplay(ctx context.Context, epoch int64, coordinatorID string, assignment domain.Assignment, identity workerproto.ExecutionIdentity, proposed *workerproto.SessionDisplay) (*workerproto.SessionDisplay, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	current, binding, err := bindAssignmentDecision(ctx, tx, epoch, coordinatorID, assignment, identity, "assignment display")
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(proposed)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO coordinator_assignment_displays(assignment_id, assignment_epoch, binding, display)
 VALUES(?, ?, ?, ?) ON CONFLICT(assignment_id, assignment_epoch) DO NOTHING`, current.ID, current.Epoch, string(binding), string(raw)); err != nil {
		return nil, err
	}
	var retainedBinding, retained string
	if err := tx.QueryRowContext(ctx, "SELECT binding, display FROM coordinator_assignment_displays WHERE assignment_id = ? AND assignment_epoch = ?", current.ID, current.Epoch).Scan(&retainedBinding, &retained); err != nil {
		return nil, err
	}
	if retainedBinding != binding {
		return nil, errors.New("assignment display: frozen binding mismatch")
	}
	var display *workerproto.SessionDisplay
	if err := json.Unmarshal([]byte(retained), &display); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return display, nil
}

// bindAssignmentDecision is the shared opening of a frozen per-assignment
// decision: it takes the writer lock, fences coordinator authority, checks
// that the offered assignment and execution identity are the durable ones,
// and returns the binding the decision is stored under. subject prefixes
// every refusal.
func bindAssignmentDecision(ctx context.Context, tx *sql.Tx, epoch int64, coordinatorID string, assignment domain.Assignment, identity workerproto.ExecutionIdentity, subject string) (domain.Assignment, string, error) {
	// Acquire the SQLite writer lock before reading. Concurrent connections must
	// see the winner rather than upgrading stale read snapshots into writers.
	result, err := tx.ExecContext(ctx, "UPDATE coordinator_assignments SET record = record WHERE id = ?", assignment.ID)
	if err != nil {
		return domain.Assignment{}, "", fmt.Errorf("lock %s: %w", subject, err)
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return domain.Assignment{}, "", fmt.Errorf("%s: durable assignment missing", subject)
	}
	if err := requireCoordinatorEpoch(ctx, tx, epoch); err != nil {
		return domain.Assignment{}, "", err
	}
	current, err := loadAssignmentTx(ctx, tx, assignment.ID)
	if err != nil {
		return domain.Assignment{}, "", err
	}
	normalize := func(a domain.Assignment) domain.Assignment {
		a.LeaseExpiresAt = time.Time{}
		a.UpdatedAt = time.Time{}
		return a
	}
	if current.State != domain.AssignmentOffered || !reflect.DeepEqual(normalize(current), normalize(assignment)) {
		return domain.Assignment{}, "", fmt.Errorf("%s: durable assignment binding mismatch", subject)
	}
	attempt, err := loadAttemptTx(ctx, tx, assignment.AttemptID)
	if err != nil {
		return domain.Assignment{}, "", err
	}
	if identity.AssignmentID != current.ID || identity.AssignmentEpoch != current.Epoch ||
		identity.AttemptID != current.AttemptID || identity.DispatchToken != current.DispatchToken ||
		identity.ThreadID != current.ThreadID || identity.AttemptRevision != attempt.Revision ||
		identity.WorkflowRunID != attempt.WorkflowRunID || identity.TaskID != attempt.TaskID ||
		attempt.AssignmentID != current.ID {
		return domain.Assignment{}, "", fmt.Errorf("%s: execution identity mismatch", subject)
	}
	var runRaw string
	if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_workflow_runs WHERE id = ?", identity.WorkflowRunID).Scan(&runRaw); err != nil {
		return domain.Assignment{}, "", err
	}
	var run domain.WorkflowRun
	if err := json.Unmarshal([]byte(runRaw), &run); err != nil {
		return domain.Assignment{}, "", err
	}
	if identity.WorkflowID != run.WorkflowID || coordinatorID == "" {
		return domain.Assignment{}, "", fmt.Errorf("%s: workflow/coordinator binding mismatch", subject)
	}
	binding, err := json.Marshal(struct {
		CoordinatorID string
		Assignment    domain.Assignment
		Identity      workerproto.ExecutionIdentity
	}{coordinatorID, normalize(current), identity})
	if err != nil {
		return domain.Assignment{}, "", err
	}
	return current, string(binding), nil
}
