package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// AttemptFailureStamp is one classification to record on a terminal attempt.
type AttemptFailureStamp struct {
	AttemptID      string
	Revision       int64
	Classification domain.FailureClassification
}

// RecordAttemptFailureClassifications records the coordinator's failure
// classification on terminal attempts and reports how many it wrote.
//
// The classification is derived from the attempt's own recorded failure, so
// writing it is not a state transition and does not move the attempt's
// revision: an operator command fenced on that revision stays valid. A stamp
// whose revision no longer matches, or whose attempt is no longer failed or
// cancelled, is skipped; the next boundary classifies the attempt again.
func (s *Store) RecordAttemptFailureClassifications(ctx context.Context, stamps []AttemptFailureStamp) (int, error) {
	if len(stamps) == 0 {
		return 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin failure classification: %w", err)
	}
	defer tx.Rollback()
	written := 0
	for _, stamp := range stamps {
		if stamp.Classification.Class == "" || stamp.Classification.Code == "" {
			return 0, fmt.Errorf("failure classification of attempt %q is incomplete", stamp.AttemptID)
		}
		attempt, err := loadAttemptTx(ctx, tx, stamp.AttemptID)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return 0, err
		}
		if attempt.Revision != stamp.Revision || attempt.FailureClass != "" ||
			(attempt.Progress != domain.ProgressFailed && attempt.Progress != domain.ProgressCancelled) {
			continue
		}
		attempt.FailureClass, attempt.FailureReason = stamp.Classification.Class, stamp.Classification.Code
		if err := updateAttemptTx(ctx, tx, attempt, attempt.Revision); err != nil {
			return 0, err
		}
		written++
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit failure classification: %w", err)
	}
	return written, nil
}
