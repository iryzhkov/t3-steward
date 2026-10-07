package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// V39 is append-only, like V33: the continuation checkpoint decision of an
// assignment is frozen when its first offer is built, so a replayed offer is
// the same package even when the worker's inventory cannot be read any more
// or a later checkpoint has arrived since.
const coordinatorMigrationV39 = `
CREATE TABLE IF NOT EXISTS coordinator_assignment_continuations (
 assignment_id TEXT NOT NULL,
 assignment_epoch INTEGER NOT NULL CHECK(assignment_epoch > 0),
 binding TEXT NOT NULL,
 decision TEXT NOT NULL CHECK(json_valid(decision)),
 PRIMARY KEY(assignment_id, assignment_epoch)
);
`

// ContinuationDecision is what an assignment's package says about the
// continuation checkpoint contract: whether it declares the capability and,
// for a retry, which earlier snapshot it carries.
type ContinuationDecision struct {
	Offered    bool                           `json:"offered"`
	ArtifactID string                         `json:"artifactId,omitempty"`
	Input      *workerproto.ContinuationInput `json:"input,omitempty"`
}

// ContinuationDispatch is the dispatch an assignment's V39 row was frozen
// for: the assignment as it was first offered at that epoch, the execution
// identity the package carried, and whether the package declared the
// continuation checkpoint capability. The row proves the offer, not the claim.
type ContinuationDispatch struct {
	Assignment domain.Assignment
	Identity   workerproto.ExecutionIdentity
	Offered    bool
}

// AssignmentContinuationBinding reads the dispatch an assignment's epoch was
// first offered as. It is how a continuation snapshot taken by an earlier
// epoch is authenticated after the assignment was offered again: the current
// row carries only the latest dispatch. A missing row is not found, never an
// error; a row that does not parse or does not describe that dispatch is an
// error, never an acceptance.
func (s *Store) AssignmentContinuationBinding(ctx context.Context, assignmentID string, epoch int64) (ContinuationDispatch, bool, error) {
	return loadContinuationDispatch(ctx, s.db, assignmentID, epoch)
}

// loadContinuationDispatch reads a V39 row through q, which is the database
// or the transaction a fence runs in.
func loadContinuationDispatch(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, assignmentID string, epoch int64) (ContinuationDispatch, bool, error) {
	var binding, decision string
	err := q.QueryRowContext(ctx, "SELECT binding, decision FROM coordinator_assignment_continuations WHERE assignment_id = ? AND assignment_epoch = ?", assignmentID, epoch).Scan(&binding, &decision)
	if errors.Is(err, sql.ErrNoRows) {
		return ContinuationDispatch{}, false, nil
	}
	if err != nil {
		return ContinuationDispatch{}, false, fmt.Errorf("load continuation dispatch %s@%d: %w", assignmentID, epoch, err)
	}
	var frozen struct {
		CoordinatorID string
		Assignment    domain.Assignment
		Identity      workerproto.ExecutionIdentity
	}
	var offered ContinuationDecision
	if err := json.Unmarshal([]byte(binding), &frozen); err != nil {
		return ContinuationDispatch{}, false, fmt.Errorf("continuation dispatch %s@%d: binding: %w", assignmentID, epoch, err)
	}
	if err := json.Unmarshal([]byte(decision), &offered); err != nil {
		return ContinuationDispatch{}, false, fmt.Errorf("continuation dispatch %s@%d: decision: %w", assignmentID, epoch, err)
	}
	a, identity := frozen.Assignment, frozen.Identity
	if a.ID != assignmentID || a.Epoch != epoch || a.AttemptID == "" || a.WorkerID == "" ||
		identity.AssignmentID != assignmentID || identity.AssignmentEpoch != epoch || identity.AttemptID != a.AttemptID {
		return ContinuationDispatch{}, false, fmt.Errorf("continuation dispatch %s@%d: binding does not describe this dispatch", assignmentID, epoch)
	}
	return ContinuationDispatch{Assignment: a, Identity: identity, Offered: offered.Offered}, true, nil
}

// FreezeAssignmentContinuation commits the first proposal for an assignment
// and returns whatever was committed first. It has the binding and fencing of
// FreezeAssignmentDisplay; inventory is never read in this transaction.
func (s *Store) FreezeAssignmentContinuation(ctx context.Context, epoch int64, coordinatorID string, assignment domain.Assignment, identity workerproto.ExecutionIdentity, proposed ContinuationDecision) (ContinuationDecision, error) {
	if (proposed.ArtifactID == "") != (proposed.Input == nil) || (!proposed.Offered && proposed.Input != nil) {
		return ContinuationDecision{}, errors.New("assignment continuation: incomplete proposal")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ContinuationDecision{}, err
	}
	defer tx.Rollback()
	current, binding, err := bindAssignmentDecision(ctx, tx, epoch, coordinatorID, assignment, identity, "assignment continuation")
	if err != nil {
		return ContinuationDecision{}, err
	}
	raw, err := json.Marshal(proposed)
	if err != nil {
		return ContinuationDecision{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO coordinator_assignment_continuations(assignment_id, assignment_epoch, binding, decision)
 VALUES(?, ?, ?, ?) ON CONFLICT(assignment_id, assignment_epoch) DO NOTHING`, current.ID, current.Epoch, binding, string(raw)); err != nil {
		return ContinuationDecision{}, err
	}
	var retainedBinding, retained string
	if err := tx.QueryRowContext(ctx, "SELECT binding, decision FROM coordinator_assignment_continuations WHERE assignment_id = ? AND assignment_epoch = ?", current.ID, current.Epoch).Scan(&retainedBinding, &retained); err != nil {
		return ContinuationDecision{}, err
	}
	if retainedBinding != binding {
		return ContinuationDecision{}, errors.New("assignment continuation: frozen binding mismatch")
	}
	var decision ContinuationDecision
	if err := json.Unmarshal([]byte(retained), &decision); err != nil {
		return ContinuationDecision{}, err
	}
	if err := tx.Commit(); err != nil {
		return ContinuationDecision{}, err
	}
	return decision, nil
}
