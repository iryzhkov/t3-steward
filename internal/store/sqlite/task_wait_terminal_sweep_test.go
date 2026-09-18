package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A live task wait whose attempt became terminal by any path other than the
// cancel command (a crash between the cancellation and its wait settlement,
// or another terminal transition) is settled cancelled by the coordinator's
// next settlement pass rather than left live until its deadline. A wait on an
// attempt that is still parked is untouched.
func TestSettlementPassCancelsTheLiveWaitOfATerminalAttempt(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	registered, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, "park-1", domain.WakeEach), now)
	if err != nil {
		t.Fatal(err)
	}
	// Still parked: the pass leaves the wait live.
	if err := store.SettleNodeWaits(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	waits, _ := store.ListTaskWaits(ctx)
	if len(waits) != 1 || waits[0].Settled() {
		t.Fatalf("a wait on a parked attempt was settled: %+v", waits)
	}
	// The attempt is cancelled behind the wait's back, as a crash between
	// ApplyAdminCommand and settleCancelledAttemptWaits leaves it.
	parked := loadAttempt(t, store, attempt.ID)
	parked.Progress, parked.Control, parked.Revision = domain.ProgressCancelled, domain.ControlStopped, parked.Revision+1
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{parked}}); err != nil {
		t.Fatal(err)
	}
	if err := store.SettleNodeWaits(ctx, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	waits, _ = store.ListTaskWaits(ctx)
	if len(waits) != 1 || waits[0].ID != registered.ID || waits[0].Result == nil || waits[0].Result.Outcome != domain.TaskWaitCancelled {
		t.Fatalf("the live wait of a terminal attempt was not settled: %+v", waits)
	}
	if !strings.Contains(waits[0].Result.Reason, "cancelled") {
		t.Fatalf("the settlement does not say how the attempt ended: %q", waits[0].Result.Reason)
	}
	// The pass is idempotent and the wake closes as abandoned.
	if err := store.SettleNodeWaits(ctx, now.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.WakeTaskWaits(ctx, now.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	waits, _ = store.ListTaskWaits(ctx)
	if waits[0].Result.Outcome != domain.TaskWaitCancelled || waits[0].Delivery != "abandoned" {
		t.Fatalf("after the wake: %+v", waits[0])
	}
	if live, err := store.LiveTaskWaitAttempts(ctx); err != nil || len(live) != 0 {
		t.Fatalf("live waits after the sweep: %v err=%v", live, err)
	}
}
