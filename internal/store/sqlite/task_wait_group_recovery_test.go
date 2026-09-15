package sqlite

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestSharedWakeRecoversPartialLegacyDeliveryWithoutAnotherSend(t *testing.T) {
	for _, partial := range []string{"sending", "recovery-required", "delivered"} {
		t.Run(partial, func(t *testing.T) {
			ctx := context.Background()
			store, attempt, now := taskWaitFixture(t)
			for _, request := range []string{"all-a", "all-b"} {
				w, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, request, domain.WakeAll), now)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := store.SettleTaskWait(ctx, w.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now.Add(time.Second)); err != nil {
					t.Fatal(err)
				}
			}
			returned, err := store.WakeTaskWaits(ctx, now.Add(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if returned[0].Waits[0].DeliveryID == "" || returned[0].Waits[0].DeliveryID != returned[0].Waits[1].DeliveryID {
				t.Fatal("returned group does not carry persisted delivery identity")
			}
			pending, err := store.TaskWakesAwaitingDelivery(ctx, now)
			if err != nil {
				t.Fatal(err)
			}
			member := pending[0].Waits[1] // simulate older per-record transition
			member.Delivery = partial
			raw, err := json.Marshal(member)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.ExecContext(ctx, "UPDATE coordinator_task_waits SET record=? WHERE id=?", raw, member.ID); err != nil {
				t.Fatal(err)
			}
			pending, err = store.TaskWakesAwaitingDelivery(ctx, now)
			if err != nil {
				t.Fatal(err)
			}
			if partial == "delivered" {
				if len(pending) != 0 {
					t.Fatalf("positive delivery was not propagated: %+v", pending)
				}
				return
			}
			for _, w := range pending[0].Waits {
				if w.Delivery != "recovery-required" {
					t.Fatalf("partial send can be sent again: %+v", w)
				}
			}
			if claimed, err := store.TransitionTaskWake(ctx, pending[0].Waits[0].ID, "pending", "sending", now); err != nil || claimed {
				t.Fatalf("uncertain group claimed for resend: %t %v", claimed, err)
			}
			if claimed, err := store.TransitionTaskWake(ctx, member.ID, "recovery-required", "delivered", now); err != nil || !claimed {
				t.Fatalf("positive observation could not settle group: %t %v", claimed, err)
			}
		})
	}
}
