package sqlite

import (
	"context"
	"database/sql"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
)

// reviewParentAttemptTx streams indexed and runtime identities in the caller's
// writer transaction. Decode before selecting ownership: an indexed-away retry
// must not disappear. Corrupt unrelated JSON fails closed; memory grows only
// with distinct parent attempt numbers, not the entire attempts table.
func reviewParentAttemptTx(ctx context.Context, tx *sql.Tx, p review.ParentBinding) (domain.Attempt, error) {
	var bound domain.Attempt
	rows, err := tx.QueryContext(ctx, "SELECT id,workflow_run_id,task_id,number,revision,record FROM coordinator_attempts")
	if err != nil {
		return bound, err
	}
	defer rows.Close()
	seen := map[int]bool{}
	latest, boundCount := 0, 0
	owns := func(id, run, task string) bool {
		return id == p.AttemptID || run == p.RunID && task == p.TaskID
	}
	for rows.Next() {
		var id, run, task string
		var number int
		var revision int64
		var raw []byte
		if err := rows.Scan(&id, &run, &task, &number, &revision, &raw); err != nil {
			return bound, err
		}
		a, err := decodeReviewChildRecord[domain.Attempt](raw, reviewChildAttemptKeys)
		if err != nil {
			return bound, ErrReviewAuthorityIdentity
		}
		if !owns(id, run, task) && !owns(a.ID, a.WorkflowRunID, a.TaskID) {
			continue
		}
		if id == "" || a.ID != id || a.WorkflowRunID != run || a.TaskID != task ||
			a.Number != number || a.Revision != revision || run != p.RunID || task != p.TaskID ||
			number < 1 || revision < 1 || seen[number] {
			return bound, ErrReviewAuthorityIdentity
		}
		seen[number] = true
		if number > latest {
			latest = number
		}
		if id == p.AttemptID {
			bound = a
			boundCount++
		}
	}
	if err := rows.Err(); err != nil {
		return bound, err
	}
	if err := rows.Close(); err != nil {
		return bound, err
	}
	if boundCount != 1 || bound.Number != latest {
		return bound, ErrReviewAuthorityIdentity
	}
	return bound, nil
}
