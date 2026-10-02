package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A wake whose thread the archive deleted is ended whether or not its wait has
// settled: a thread that no longer exists has nowhere to receive an outcome.
// The ended wait is then left alone by settlement, releases its retention pin,
// and says why it ended and what to do instead.
func TestAnUnsettledWakeCanBeRejectedAndStaysEnded(t *testing.T) {
	ctx := context.Background()
	store, before, now := sinkStoreFixture(t)
	a := before.Attempts[0]
	a.Progress = domain.ProgressActive
	a.Control = domain.ControlRunning
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{a}}); err != nil {
		t.Fatal(err)
	}
	req := domain.NodeWaitRequest{ID: "nw-gone", ThreadID: "thread", Name: "task", Target: domain.NodeRef{RunID: "r", TaskID: "t"}, Timeout: time.Hour}
	if _, err := store.RegisterNodeWait(ctx, req, "operator", "host", now); err != nil {
		t.Fatal(err)
	}
	ok, err := store.TransitionNodeWake(ctx, req.ID, "pending", "rejected", now)
	if err != nil || !ok {
		t.Fatalf("an unsettled pending wake could not be rejected: ok=%v err=%v", ok, err)
	}
	// Past the deadline, settlement must not touch an ended wake.
	if err := store.SettleNodeWaits(ctx, now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	waits, err := store.ListNodeWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w := waits[0]
	if w.Delivery != "rejected" || w.SettledAt != nil {
		t.Fatalf("the ended wake is delivery=%s settled=%v", w.Delivery, w.SettledAt)
	}
	if !strings.Contains(w.DeliveryNextAction, "t3-steward wait add") {
		t.Fatalf("the next action does not say how to replace the wake: %q", w.DeliveryNextAction)
	}
	var pins int
	if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM coordinator_retention_pins WHERE owner=?", "wait:"+req.ID).Scan(&pins); err != nil {
		t.Fatal(err)
	}
	if pins != 0 {
		t.Fatalf("a rejected wake still pins its run (%d pins)", pins)
	}
}
