package sqlite

import (
	"context"
	"errors"
	"testing"
)

func reviewRowCounts(t *testing.T, s *Store) [3]int {
	t.Helper()
	var counts [3]int
	for i, table := range []string{"coordinator_review_authorities", "coordinator_review_checkpoints", "coordinator_review_rounds"} {
		if err := s.db.QueryRow("SELECT count(*) FROM " + table).Scan(&counts[i]); err != nil {
			t.Fatal(err)
		}
	}
	return counts
}

// The review writers compare a bound coordinator epoch inside their own
// transaction and write nothing when it is stale; callers that bind no epoch
// keep their behaviour.
func TestReviewWritersRefuseStaleCoordinatorEpochFence(t *testing.T) {
	ctx := context.Background()
	s, f := reviewAuthorityFixture(t)
	epoch, err := s.CoordinatorEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stale := WithCoordinatorEpochFence(ctx, epoch-1)
	if _, err := s.FreezeReviewAuthority(stale, f); !errors.Is(err, ErrStaleCoordinatorEpoch) {
		t.Fatalf("stale freeze: %v", err)
	}
	if counts := reviewRowCounts(t, s); counts != [3]int{} {
		t.Fatalf("stale freeze wrote %v", counts)
	}

	current := WithCoordinatorEpochFence(ctx, epoch)
	if _, err := s.FreezeReviewAuthority(current, f); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AdvanceCoordinatorEpoch(ctx, epoch); err != nil {
		t.Fatal(err)
	}
	// current is now the superseded coordinator's fence.
	before := reviewRowCounts(t, s)
	if _, err := s.AllocateReviewCheckpoint(current, f, reviewCheckpoint("cp")); !errors.Is(err, ErrStaleCoordinatorEpoch) {
		t.Fatalf("stale allocation: %v", err)
	}
	if _, err := s.FreezeReviewAuthority(current, f); !errors.Is(err, ErrStaleCoordinatorEpoch) {
		t.Fatalf("stale freeze replay: %v", err)
	}
	if _, _, _, err := s.ReviewCheckpointReplay(current, f, "cp"); !errors.Is(err, ErrStaleCoordinatorEpoch) {
		t.Fatalf("stale checkpoint replay: %v", err)
	}
	if after := reviewRowCounts(t, s); after != before {
		t.Fatalf("stale writers changed rows %v -> %v", before, after)
	}

	if _, err := s.AllocateReviewCheckpoint(WithCoordinatorEpochFence(ctx, epoch+1), f, reviewCheckpoint("cp")); err != nil {
		t.Fatalf("current allocation: %v", err)
	}
	if _, err := s.AllocateReviewCheckpoint(ctx, f, reviewCheckpoint("cp")); err != nil {
		t.Fatalf("unfenced allocation replay: %v", err)
	}
}
