package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"reflect"

	"github.com/iryzhkov/t3-steward/internal/review"
)

// ReviewCheckpointReplay reads, in one read-only transaction, what a checkpoint
// ID already holds under one frozen authority: its allocation, if any, and its
// materialization receipt, if the child was created.
//
// It writes nothing. A checkpoint operation uses it to answer a repeated call
// without staging again, and to refuse a reused ID whose head moved before any
// row is touched. A missing authority or checkpoint is reported as not found;
// a stored record that does not belong to the expected authority is a conflict.
func (s *Store) ReviewCheckpointReplay(ctx context.Context, expected review.FrozenAuthority, checkpointID string) (review.CheckpointAuthority, *ReviewMaterialization, bool, error) {
	var zero review.CheckpointAuthority
	expected, err := expected.Canonical()
	if err != nil {
		return zero, nil, false, err
	}
	if !review.IDPattern.MatchString(checkpointID) {
		return zero, nil, false, errors.New("invalid checkpoint identity")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return zero, nil, false, err
	}
	defer tx.Rollback()
	// A replay answer carries authority, so a coordinator epoch fence bound to
	// ctx is compared in the same snapshot the answer is read from.
	if err := requireReviewEpochFenceTx(ctx, tx); err != nil {
		return zero, nil, false, err
	}
	frozen, err := expectedReviewAuthorityTx(ctx, tx, expected)
	if errors.Is(err, sql.ErrNoRows) {
		return zero, nil, false, nil
	}
	if err != nil {
		return zero, nil, false, err
	}
	key := review.CheckpointAuthority{AuthorityKey: frozen.Key(), Checkpoint: review.Checkpoint{ID: checkpointID}}.Key()
	cp, err := loadReviewJSONTx[review.CheckpointAuthority](ctx, tx, "SELECT record FROM coordinator_review_checkpoints WHERE id=? AND authority_id=? AND checkpoint_id=?", key, frozen.Key(), checkpointID)
	if errors.Is(err, sql.ErrNoRows) {
		return zero, nil, false, nil
	}
	if err != nil {
		return zero, nil, false, err
	}
	if cp.AuthorityKey != frozen.Key() || cp.Checkpoint.ID != checkpointID || cp.RoundID != cp.Key() || cp.Key() != key {
		return zero, nil, false, ErrReviewAuthorityConflict
	}
	receipt, err := loadReviewJSONTx[ReviewMaterialization](ctx, tx, "SELECT record FROM coordinator_review_materializations WHERE checkpoint_id=? AND workflow_id=? AND run_id=?", cp.Key(), review.ChildWorkflowID(cp), cp.RoundID)
	if errors.Is(err, sql.ErrNoRows) {
		return cp, nil, true, tx.Commit()
	}
	if err != nil {
		return zero, nil, false, err
	}
	if !reflect.DeepEqual(receipt.Authority, frozen) || receipt.Checkpoint != cp || receipt.Graph.Run.ID != cp.RoundID ||
		receipt.Graph.Workflow.ID != review.ChildWorkflowID(cp) || len(receipt.Graph.Tasks) == 0 || receipt.Graph.Tasks[0].Deadline == nil {
		return zero, nil, false, ErrReviewMaterialization
	}
	if err := validateReviewChildTx(ctx, tx, receipt); err != nil {
		return zero, nil, false, err
	}
	return cp, &receipt, true, tx.Commit()
}
