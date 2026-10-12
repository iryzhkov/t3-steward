package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
)

type reviewChildrenTickKey struct{}
type reviewChildrenTick struct {
	mu         sync.Mutex
	reconciled bool
}

// WithReviewChildrenReconciledOncePerTick marks one boundary tick. The first
// successful reconciliation runs the pass; subsequent calls do not read.
// Failures may be retried. Unmarked contexts reconcile on every call.
// A parent ending later in this tick has its children cancelled by the next
// boundary tick, one interval later.
func WithReviewChildrenReconciledOncePerTick(ctx context.Context) context.Context {
	return context.WithValue(ctx, reviewChildrenTickKey{}, &reviewChildrenTick{})
}

// Both queries are indexed by round/run identity, never by the full history.
func reviewCheckpointSettledTx(ctx context.Context, tx *sql.Tx, cp review.CheckpointAuthority) (bool, error) {
	round, err := loadReviewJSONTx[review.Round](ctx, tx, "SELECT record FROM coordinator_review_rounds WHERE id=?", cp.RoundID)
	if err != nil || !round.Terminal() {
		return false, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT record FROM coordinator_attempts WHERE workflow_run_id=?", cp.RoundID)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return false, err
		}
		var attempt domain.Attempt
		if err := json.Unmarshal(raw, &attempt); err != nil {
			return false, err
		}
		if !attempt.Progress.Terminal() {
			return false, nil
		}
	}
	return true, rows.Err()
}
