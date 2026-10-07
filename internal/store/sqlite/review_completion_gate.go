package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
)

// LatestReviewRoundHead reads, in one read-only snapshot, the newest review
// round a task opened through its frozen review authority, for the completion
// gate. found is false when the task has no authority or no round yet.
//
// Acceptance is not taken from the stored combined verdict. The round is
// re-validated from its retained results against the frozen requirements and
// its own checkpoint head, exactly as CheckReviewAuthorityEvidence does, so a
// corrupted or forged "accept" does not let work complete.
func (s *Store) LatestReviewRoundHead(ctx context.Context, runID, taskID string) (domain.ReviewRoundHead, bool, error) {
	var zero domain.ReviewRoundHead
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return zero, false, err
	}
	defer tx.Rollback()
	stored, err := loadReviewJSONTx[review.FrozenAuthority](ctx, tx, "SELECT record FROM coordinator_review_authorities WHERE run_id=? AND task_id=?", runID, taskID)
	if errors.Is(err, sql.ErrNoRows) {
		return zero, false, tx.Commit()
	}
	if err != nil {
		return zero, false, err
	}
	frozen, err := stored.Canonical()
	if err != nil {
		return zero, false, err
	}
	if frozen.Parent.RunID != runID || frozen.Parent.TaskID != taskID {
		return zero, false, ErrReviewAuthorityIdentity
	}
	checkpoint, err := loadReviewJSONTx[review.CheckpointAuthority](ctx, tx, "SELECT record FROM coordinator_review_checkpoints WHERE authority_id=? ORDER BY number DESC LIMIT 1", frozen.Key())
	if errors.Is(err, sql.ErrNoRows) {
		return zero, false, tx.Commit()
	}
	if err != nil {
		return zero, false, err
	}
	if checkpoint.AuthorityKey != frozen.Key() || checkpoint.RoundID != checkpoint.Key() {
		return zero, false, ErrReviewAuthorityConflict
	}
	round, err := loadReviewJSONTx[review.Round](ctx, tx, "SELECT record FROM coordinator_review_rounds WHERE id=?", checkpoint.RoundID)
	if err != nil {
		return zero, false, fmt.Errorf("load review round %q: %w", checkpoint.RoundID, err)
	}
	head := domain.ReviewRoundHead{
		RoundID: checkpoint.RoundID, Number: checkpoint.Number, CheckpointID: checkpoint.Checkpoint.ID,
		BaseCommit: frozen.Parent.BaseCommit, HeadCommit: checkpoint.Checkpoint.HeadCommit,
		Verdict: round.CombinedVerdict(),
	}
	verdict, validationErr := review.ValidateBoundEvidence(frozen, checkpoint, round, checkpoint.Checkpoint.HeadCommit)
	switch {
	case validationErr == nil:
		head.Accepted, head.Verdict = true, verdict
	case head.Verdict == "accept" || head.Verdict == "accept-with-changes":
		// The stored results read as accepting but do not re-validate.
		head.Verdict, head.Detail = "invalid", validationErr.Error()
	}
	return head, true, tx.Commit()
}
