package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/review"
)

const coordinatorMigrationV31 = `
CREATE TABLE IF NOT EXISTS coordinator_review_rounds (
 id TEXT PRIMARY KEY,
 revision INTEGER NOT NULL,
 record TEXT NOT NULL
);
`

var ErrReviewRoundNotFound = errors.New("review round not found")
var ErrReviewRoundConflict = errors.New("review round revision conflict")

// CreateReviewRound is coordinator-owned; it grants no new authority kind.
// Submission orchestration supplies immutable requirements and actual routes.
func (s *Store) CreateReviewRound(ctx context.Context, round review.Round) (review.Round, error) {
	if err := round.Initialize(s.now()); err != nil {
		return review.Round{}, err
	}
	raw, err := json.Marshal(round)
	if err != nil {
		return review.Round{}, err
	}
	if _, err := s.db.ExecContext(ctx, "INSERT INTO coordinator_review_rounds(id,revision,record) VALUES(?,?,?)", round.ID, round.Revision, string(raw)); err != nil {
		return review.Round{}, fmt.Errorf("create review round: %w", err)
	}
	return round, nil
}
func (s *Store) GetReviewRound(ctx context.Context, id string) (review.Round, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, "SELECT record FROM coordinator_review_rounds WHERE id=?", id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return review.Round{}, ErrReviewRoundNotFound
	}
	if err != nil {
		return review.Round{}, err
	}
	var round review.Round
	if err := json.Unmarshal([]byte(raw), &round); err != nil {
		return round, err
	}
	round.Combined = round.CombinedVerdict()
	return round, nil
}

// ListUncollectedReviewRounds is bounded by pending work, not history.
func (s *Store) ListUncollectedReviewRounds(ctx context.Context) ([]review.Round, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT record FROM coordinator_review_rounds WHERE COALESCE(json_extract(record,'$.replyText'),'')='' ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []review.Round
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var r review.Round
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// ListReviewRoundsForRun returns every round of one run, collected or not,
// with its combined verdict and without bulk evidence. It is bounded by the
// run, not by history.
func (s *Store) ListReviewRoundsForRun(ctx context.Context, runID string) ([]review.Round, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT record FROM coordinator_review_rounds WHERE json_extract(record,'$.workflowRunId')=? ORDER BY id", runID)
	if err != nil {
		return nil, fmt.Errorf("list review rounds of run %s: %w", runID, err)
	}
	defer rows.Close()
	var result []review.Round
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var r review.Round
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, err
		}
		r.Combined = r.CombinedVerdict()
		result = append(result, r.Metadata())
	}
	return result, rows.Err()
}

func (s *Store) PublishReviewReply(ctx context.Context, id string, revision int64, text string) error {
	if len(text) > 16<<10 {
		return errors.New("review reply exceeds 16 KiB")
	}
	r, err := s.GetReviewRound(ctx, id)
	if err != nil {
		return err
	}
	if r.Revision != revision || !r.Terminal() {
		return ErrReviewRoundConflict
	}
	r.ReplyText = text
	r.Revision++
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, "UPDATE coordinator_review_rounds SET revision=?,record=? WHERE id=? AND revision=?", r.Revision, raw, id, revision)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrReviewRoundConflict
	}
	return nil
}

// RecordReviewResult validates evidence inside a revision-fenced transaction.
// Concurrent completions cannot lose another reviewer's result.
func (s *Store) RecordReviewResult(ctx context.Context, id, reviewer string, expected int64, result review.Result) (review.Round, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return review.Round{}, err
	}
	defer tx.Rollback()
	var raw string
	err = tx.QueryRowContext(ctx, "SELECT record FROM coordinator_review_rounds WHERE id=?", id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return review.Round{}, ErrReviewRoundNotFound
	}
	if err != nil {
		return review.Round{}, err
	}
	var round review.Round
	if err := json.Unmarshal([]byte(raw), &round); err != nil {
		return round, err
	}
	if round.Revision != expected {
		return review.Round{}, ErrReviewRoundConflict
	}
	if err := round.ApplyResult(reviewer, result, s.now()); err != nil {
		return review.Round{}, err
	}
	encoded, err := json.Marshal(round)
	if err != nil {
		return review.Round{}, err
	}
	updated, err := tx.ExecContext(ctx, "UPDATE coordinator_review_rounds SET revision=?,record=? WHERE id=? AND revision=?", round.Revision, string(encoded), id, expected)
	if err != nil {
		return review.Round{}, err
	}
	count, err := updated.RowsAffected()
	if err != nil {
		return review.Round{}, err
	}
	if count != 1 {
		return review.Round{}, ErrReviewRoundConflict
	}
	if err := tx.Commit(); err != nil {
		return review.Round{}, err
	}
	return round, nil
}
