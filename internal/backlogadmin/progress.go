package backlogadmin

import (
	"context"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// progressMirror uses the existing workflows read authorization and never
// opens retained artifacts or invokes any writer.
func (s *Service) progressMirror(ctx context.Context, records sqlite.CoordinatorRecords, filter backlog.ProgressFilter, now time.Time) (backlog.ProgressDocument, error) {
	var waits []domain.NodeWait
	if filter.Owner != "" {
		reader, ok := s.reader.(interface {
			ListNodeWaits(context.Context) ([]domain.NodeWait, error)
		})
		if !ok {
			return backlog.ProgressDocument{}, fmt.Errorf("%w: notify threads unavailable on this coordinator", ErrInvalidQuery)
		}
		var err error
		waits, err = reader.ListNodeWaits(ctx)
		if err != nil {
			return backlog.ProgressDocument{}, fmt.Errorf("load notify threads: %w", err)
		}
	}
	var rounds []review.Round
	reader, reported := s.reader.(interface {
		ListReviewRoundsForRun(context.Context, string) ([]review.Round, error)
	})
	if reported {
		// Only fetch rounds for selected runs; reuse the builder's selection rules.
		selected, err := backlog.BuildProgress(records, nil, false, waits, filter, now)
		if err != nil {
			return backlog.ProgressDocument{}, err
		}
		for _, run := range selected.Runs {
			own, err := reader.ListReviewRoundsForRun(ctx, run.ID)
			if err != nil {
				return backlog.ProgressDocument{}, fmt.Errorf("load review rounds of %s: %w", run.ID, err)
			}
			rounds = append(rounds, own...)
		}
	}
	return backlog.BuildProgress(records, rounds, reported, waits, filter, now)
}
