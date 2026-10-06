package sqlite

import (
	"context"
	"encoding/json"
	"errors"

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
