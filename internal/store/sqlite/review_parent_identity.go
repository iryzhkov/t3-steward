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
	bound, latest, err := reviewParentAttemptSnapshotTx(ctx, tx, p)
	if err != nil {
		return bound, err
	}
	if bound.Number != latest {
		return bound, ErrReviewAuthorityIdentity
	}
	return bound, nil
}

// reviewParentAttemptSnapshotTx validates history without authorizing the
// original attempt: coherent supersession is observable only after this scan.
func reviewParentAttemptSnapshotTx(ctx context.Context, tx *sql.Tx, p review.ParentBinding) (domain.Attempt, int, error) {
	var bound domain.Attempt
	latest := 0
	rows, err := tx.QueryContext(ctx, "SELECT id,workflow_run_id,task_id,number,revision,record FROM coordinator_attempts")
	if err != nil {
		return bound, latest, err
	}
	defer rows.Close()
	seen := map[int]bool{}
	boundCount := 0
	owns := func(id, run, task string) bool {
		return id == p.AttemptID || run == p.RunID && task == p.TaskID
	}
	for rows.Next() {
		var id, run, task string
		var number int
		var revision int64
		var raw []byte
		if err := rows.Scan(&id, &run, &task, &number, &revision, &raw); err != nil {
			return bound, latest, err
		}
		a, err := decodeReviewChildRecord[domain.Attempt](raw, reviewChildAttemptKeys)
		if err != nil {
			return bound, latest, ErrReviewAuthorityIdentity
		}
		if !owns(id, run, task) && !owns(a.ID, a.WorkflowRunID, a.TaskID) {
			continue
		}
		if id == "" || a.ID != id || a.WorkflowRunID != run || a.TaskID != task ||
			a.Number != number || a.Revision != revision || run != p.RunID || task != p.TaskID ||
			number < 1 || revision < 1 || seen[number] {
			return bound, latest, ErrReviewAuthorityIdentity
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
		return bound, latest, err
	}
	if err := rows.Close(); err != nil {
		return bound, latest, err
	}
	if boundCount != 1 {
		return bound, latest, ErrReviewAuthorityIdentity
	}
	return bound, latest, nil
}
