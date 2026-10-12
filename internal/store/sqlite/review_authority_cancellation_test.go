package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/testtiming"
)

// Wait for database/sql's cancellation rollback, rather than racing its goroutine.
func awaitReviewRollback(t *testing.T, s *Store) {
	t.Helper()
	until := time.Now().Add(testtiming.Bound(5 * time.Second))
	for s.db.Stats().InUse != 0 {
		if time.Now().After(until) {
			t.Fatal("canceled transaction did not release its connection")
		}
		runtime.Gosched()
	}
}

func reviewCancellationSnapshot(t *testing.T, s *Store) map[string][]string {
	t.Helper()
	result := map[string][]string{}
	for _, table := range []string{"coordinator_review_authorities", "coordinator_review_checkpoints", "coordinator_review_rounds", "coordinator_attempts"} {
		rows, err := s.db.Query("SELECT record FROM " + table + " ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var record string
			if err := rows.Scan(&record); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			result[table] = append(result[table], record)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	return result
}

func TestReviewAuthorityTxErrorControls(t *testing.T) {
	live := context.Background()
	ctx, cancel := context.WithCancel(live)
	cancel()
	other := errors.New("other SQL failure")
	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
	}{
		{"live lifetime failure", live, sql.ErrTxDone},
		{"canceled other failure", ctx, other},
		{"successful commit then cancellation", ctx, nil},
		{"existing cancellation", ctx, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := reviewAuthorityTxError(tc.ctx, tc.err); got != tc.err {
				t.Fatalf("rewrote %v as %v", tc.err, got)
			}
		})
	}
	joined := reviewAuthorityTxError(ctx, fmt.Errorf("commit: %w", sql.ErrTxDone))
	if !errors.Is(joined, sql.ErrTxDone) || !errors.Is(joined, context.Canceled) {
		t.Fatal(joined)
	}
	if got := reviewAuthorityTxError(ctx, joined); got != joined {
		t.Fatal("setup/owner double normalization")
	}
}

func TestReviewAuthorityCanceledCommit(t *testing.T) {
	s, frozen := reviewAuthorityFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx, err := s.reviewAuthorityWriteTx(ctx, frozen.Parent)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	cancel()
	awaitReviewRollback(t, s)
	err = tx.Commit()
	if !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("real canceled commit: %v", err)
	}
	wrapped := fmt.Errorf("review child staging phase allocate: %w", reviewAuthorityTxError(ctx, err))
	if !errors.Is(wrapped, sql.ErrTxDone) || !errors.Is(wrapped, context.Canceled) {
		t.Fatalf("canceled commit lost diagnostic or cancellation: %v", wrapped)
	}
}

func TestReviewAuthorityAllocationCancellationRollback(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprint("deadline=", deadline), func(t *testing.T) {
			s, frozen := reviewAuthorityFixture(t)
			live := context.Background()
			if _, err := s.FreezeReviewAuthority(live, frozen); err != nil {
				t.Fatal(err)
			}
			before := reviewCancellationSnapshot(t, s)
			ctx, cancel := context.WithCancel(live)
			want := context.Canceled
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(live, testtiming.Bound(time.Second))
				want = context.DeadlineExceeded
			}
			defer cancel()
			clock := s.now
			reached := false
			s.now = func() time.Time {
				reached = true
				if !deadline {
					cancel()
				}
				<-ctx.Done()
				awaitReviewRollback(t, s)
				return clock()
			}
			cp := reviewCheckpoint("canceled")
			_, err := s.AllocateReviewCheckpoint(ctx, frozen, cp)
			s.now = clock
			if !reached || !errors.Is(err, want) {
				t.Fatalf("allocation cancellation: reached=%v err=%v; want %v", reached, err, want)
			}
			if after := reviewCancellationSnapshot(t, s); !reflect.DeepEqual(before, after) {
				t.Fatalf("canceled allocation changed logical evidence: before=%v after=%v", before, after)
			}
			got, err := s.AllocateReviewCheckpoint(live, frozen, cp)
			if err != nil || got.Number != 1 {
				t.Fatalf("fresh retry consumed numbering: %+v %v", got, err)
			}
			round, err := s.GetReviewRound(live, got.RoundID)
			if err != nil || round.ID != got.RoundID {
				t.Fatalf("retry missing round: %+v %v", round, err)
			}
			before = reviewCancellationSnapshot(t, s)
			replayCtx, replayCancel := context.WithCancel(live)
			replayCancel()
			if _, err := s.AllocateReviewCheckpoint(replayCtx, frozen, cp); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled replay: %v", err)
			}
			if _, err := s.FreezeReviewAuthority(replayCtx, frozen); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled freeze replay: %v", err)
			}
			if after := reviewCancellationSnapshot(t, s); !reflect.DeepEqual(before, after) {
				t.Fatal("canceled replay changed evidence")
			}
			replay, err := s.AllocateReviewCheckpoint(live, frozen, cp)
			if err != nil || !reflect.DeepEqual(replay, got) {
				t.Fatalf("fresh replay: %+v %v", replay, err)
			}
			next, err := s.AllocateReviewCheckpoint(live, frozen, reviewCheckpoint("next"))
			if err != nil || next.Number != got.Number+1 {
				t.Fatalf("next numbering: %+v %v", next, err)
			}
		})
	}
}
