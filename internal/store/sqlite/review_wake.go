package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
)

// Review wakes wait for collection even if the ordinary run sink settled first.
func reviewNodeObservationTx(ctx context.Context, tx *sql.Tx, target domain.NodeRef, obs *domain.NodeObservation) error {
	if target.TaskID != domain.SinkTaskName && target.TaskID != domain.SinkTaskID(target.RunID) {
		return nil
	}
	var raw []byte
	err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_review_rounds WHERE id=?", target.RunID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var r review.Round
	if err := json.Unmarshal(raw, &r); err != nil {
		return err
	}
	if !r.Terminal() || r.ReplyText == "" {
		obs.ExitCode = 1
		obs.Reason = "review collection pending"
		return nil
	}
	obs.Reason = r.ReplyText
	obs.ExitCode = 0
	obs.Outcome = domain.TaskWaitMet
	for _, v := range r.Reviewers {
		if v.State != "succeeded" {
			obs.ExitCode = 2
			obs.Outcome = domain.TaskWaitGaveUp
			break
		}
	}
	return nil
}
