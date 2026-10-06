package sqlite

import (
	"context"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/review"
)

// answerReviewRound records one result per member of a checkpoint round.
func answerReviewRound(t *testing.T, s *Store, cp review.CheckpointAuthority, verdict string) {
	t.Helper()
	ctx := context.Background()
	round, err := s.GetReviewRound(ctx, cp.RoundID)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range round.Reviewers {
		round, err = s.RecordReviewResult(ctx, cp.RoundID, m.ID, round.Revision, review.Result{State: "succeeded", ReviewMD: "Reviewed", VerdictJSON: []byte(`{"schema":"review-verdict/v1","verdict":"` + verdict + `","findings":[],"inputManifestDigest":"` + cp.Checkpoint.InputDigest + `","reviewerRoute":"` + m.Route + `"}`)})
		if err != nil {
			t.Fatal(err)
		}
	}
}

// The completion gate reads the task's latest round, not its best one, and
// re-validates acceptance from the retained results.
func TestLatestReviewRoundHeadFollowsTheLatestRound(t *testing.T) {
	ctx := context.Background()
	s, f := reviewAuthorityFixture(t)
	run, task := f.Parent.RunID, f.Parent.TaskID
	if _, found, err := s.LatestReviewRoundHead(ctx, run, task); err != nil || found {
		t.Fatalf("no authority: found=%v err=%v", found, err)
	}
	if _, err := s.FreezeReviewAuthority(ctx, f); err != nil {
		t.Fatal(err)
	}
	if _, found, err := s.LatestReviewRoundHead(ctx, run, task); err != nil || found {
		t.Fatalf("no checkpoint: found=%v err=%v", found, err)
	}
	first, err := s.AllocateReviewCheckpoint(ctx, f, reviewCheckpoint("cp-1"))
	if err != nil {
		t.Fatal(err)
	}
	head, found, err := s.LatestReviewRoundHead(ctx, run, task)
	if err != nil || !found || head.Accepted || head.Verdict != "pending" || head.Number != 1 || head.RoundID != first.RoundID ||
		head.CheckpointID != "cp-1" || head.HeadCommit != first.Checkpoint.HeadCommit || head.BaseCommit != f.Parent.BaseCommit {
		t.Fatalf("pending round: %+v found=%v err=%v", head, found, err)
	}
	answerReviewRound(t, s, first, "accept")
	if head, _, err = s.LatestReviewRoundHead(ctx, run, task); err != nil || !head.Accepted || head.Verdict != "accept" {
		t.Fatalf("accepted round: %+v err=%v", head, err)
	}
	// A later checkpoint on a new head supersedes the accepted one even while
	// it is still pending.
	second, err := s.AllocateReviewCheckpoint(ctx, f, review.Checkpoint{ID: "cp-2", HeadCommit: strings.Repeat("f", 40), InputDigest: strings.Repeat("e", 64)})
	if err != nil {
		t.Fatal(err)
	}
	if head, _, err = s.LatestReviewRoundHead(ctx, run, task); err != nil || head.Accepted || head.Number != 2 || head.HeadCommit != strings.Repeat("f", 40) {
		t.Fatalf("second round: %+v err=%v", head, err)
	}
	answerReviewRound(t, s, second, "reject")
	if head, _, err = s.LatestReviewRoundHead(ctx, run, task); err != nil || head.Accepted || head.Verdict != "reject" {
		t.Fatalf("rejected round: %+v err=%v", head, err)
	}
}

// A restart between the round's acceptance and collection reads the same
// accepted head back from the durable rows.
func TestLatestReviewRoundHeadSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	s, f := reviewAuthorityFixture(t)
	if _, err := s.FreezeReviewAuthority(ctx, f); err != nil {
		t.Fatal(err)
	}
	cp, err := s.AllocateReviewCheckpoint(ctx, f, reviewCheckpoint("cp-1"))
	if err != nil {
		t.Fatal(err)
	}
	answerReviewRound(t, s, cp, "accept")
	path := s.path
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	head, found, err := reopened.LatestReviewRoundHead(ctx, f.Parent.RunID, f.Parent.TaskID)
	if err != nil || !found || !head.Accepted || head.HeadCommit != cp.Checkpoint.HeadCommit {
		t.Fatalf("reopened: %+v found=%v err=%v", head, found, err)
	}
}
