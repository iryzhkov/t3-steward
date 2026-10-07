package sqlite

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/review"
	"testing"
)

func TestReviewRoundBudgetCountsFailedAndTimedOutRounds(t *testing.T) {
	for _, state := range []string{"failed", "timed-out"} {
		t.Run(state, func(t *testing.T) {
			s, f := reviewAuthorityFixture(t)
			spec := f.Requirements
			spec.RoundLimit = 1
			req, err := review.NewRequirements(spec)
			if err != nil {
				t.Fatal(err)
			}
			f, err = review.NewFrozenAuthority(f.Parent, req)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if _, err := s.FreezeReviewAuthority(ctx, f); err != nil {
				t.Fatal(err)
			}
			cp, err := s.AllocateReviewCheckpoint(ctx, f, reviewCheckpoint("first"))
			if err != nil {
				t.Fatal(err)
			}
			round, err := s.GetReviewRound(ctx, cp.RoundID)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range round.Reviewers {
				round, err = s.RecordReviewResult(ctx, cp.RoundID, m.ID, round.Revision, review.Result{State: state, Failure: "executor ended"})
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.AllocateReviewCheckpoint(ctx, f, reviewCheckpoint("second")); !errors.Is(err, ErrReviewAuthorityLimit) {
				t.Fatalf("limit %v", err)
			}
			head, found, err := s.LatestReviewRoundHead(ctx, f.Parent.RunID, f.Parent.TaskID)
			if err != nil || !found || head.RoundsUsed != 1 || head.RoundLimit != 1 {
				t.Fatalf("head %+v found %v err %v", head, found, err)
			}
		})
	}
}
