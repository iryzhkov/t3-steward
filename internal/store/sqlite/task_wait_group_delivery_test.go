package sqlite

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestAllWaitsShareOneAtomicDelivery(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	for _, request := range []string{"all-a", "all-b"} {
		wait, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, request, domain.WakeAll), now)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.SettleTaskWait(ctx, wait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.WakeTaskWaits(ctx, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	pending, err := store.TaskWakesAwaitingDelivery(ctx, now.Add(time.Second))
	if err != nil || len(pending) != 1 || len(pending[0].Waits) != 2 {
		t.Fatalf("%+v %v", pending, err)
	}
	first, second := pending[0].Waits[0], pending[0].Waits[1]
	if first.DeliveryID == "" || first.DeliveryID != second.DeliveryID {
		t.Fatalf("one all wake has separate turn identities: %q / %q", first.DeliveryID, second.DeliveryID)
	}
	ok, err := store.TransitionTaskWake(ctx, first.ID, "pending", "sending", now)
	if err != nil || !ok {
		t.Fatalf("claim: %t %v", ok, err)
	}
	ok, err = store.TransitionTaskWake(ctx, second.ID, "pending", "sending", now)
	if err != nil || ok {
		t.Fatalf("second member acquired a second send: %t %v", ok, err)
	}
	pending, err = store.TaskWakesAwaitingDelivery(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, wait := range pending[0].Waits {
		if wait.Delivery != "sending" {
			t.Fatalf("partial claim: %+v", wait)
		}
	}
	// After an atomic claim, a downgraded observer may settle only one row.
	// The immutable claim proves the original sender covered every member.
	second.Delivery = "recovery-required"
	raw, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, "UPDATE coordinator_task_waits SET record=? WHERE id=?", raw, second.ID); err != nil {
		t.Fatal(err)
	}
	pending, err = store.TaskWakesAwaitingDelivery(ctx, now)
	if err != nil || pending[0].Waits[0].Delivery != "recovery-required" {
		t.Fatalf("claimed group cannot recover: %+v %v", pending, err)
	}
	if ok, err := store.TransitionTaskWake(ctx, second.ID, "recovery-required", "delivered", now); err != nil || !ok {
		t.Fatalf("delivery: %t %v", ok, err)
	}
	pending, err = store.TaskWakesAwaitingDelivery(ctx, now)
	if err != nil || len(pending) != 0 {
		t.Fatalf("partial delivery: %+v %v", pending, err)
	}
}
