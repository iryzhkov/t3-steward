package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestDistinctLegacyWakeIdentitiesRemainIndependent(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	for _, id := range []string{"first", "second"} {
		w, err := store.RegisterTaskWait(ctx, taskWaitRegistration(attempt, id, domain.WakeAll), now)
		if err != nil {
			t.Fatal(err)
		}
		w, err = store.SettleTaskWait(ctx, w.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now)
		if err != nil {
			t.Fatal(err)
		}
	}
	wakes, err := store.WakeTaskWaits(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	for i, w := range wakes[0].Waits {
		w.DeliveryID = "legacy:" + w.ID
		if i == 0 {
			w.Delivery = "delivered"
		}
		raw, err := json.Marshal(w)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.ExecContext(ctx, "UPDATE coordinator_task_waits SET record=? WHERE id=?", raw, w.ID); err != nil {
			t.Fatal(err)
		}
	}
	pending, err := store.TaskWakesAwaitingDelivery(ctx, now)
	if err != nil || len(pending) != 1 || len(pending[0].Waits) != 1 {
		t.Fatalf("legacy wakes regrouped: %+v %v", pending, err)
	}
	w := pending[0].Waits[0]
	if w.Delivery != "pending" || w.DeliveryID != "legacy:"+w.ID {
		t.Fatalf("legacy identity rewritten: %+v", w)
	}
}

func TestSharedWakeRecoversPartialLegacyDeliveryWithoutAnotherSend(t *testing.T) {
	for _, partial := range []string{"sending", "recovery-required", "delivered", "held"} {
		for _, uniform := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/uniform=%t", partial, uniform), func(t *testing.T) {
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
				if uniform {
					first := pending[0].Waits[0]
					first.Delivery = partial
					raw, err := json.Marshal(first)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := store.db.ExecContext(ctx, "UPDATE coordinator_task_waits SET record=? WHERE id=?", raw, first.ID); err != nil {
						t.Fatal(err)
					}
				}
				parked, err := store.ParkedTaskWaitAttempts(ctx)
				if err != nil || parked[attempt.ID] == "" {
					t.Fatalf("ambiguous wake lost collection fence: %+v %v", parked, err)
				}
				pending, err = store.TaskWakesAwaitingDelivery(ctx, now)
				if err != nil {
					t.Fatal(err)
				}
				if len(pending) != 1 || len(pending[0].Waits) != 2 {
					t.Fatalf("group lost: %+v", pending)
				}
				expected := "manual-recovery-required"
				if partial == "held" {
					expected = "held"
				}
				for _, w := range pending[0].Waits {
					if w.Delivery != expected {
						t.Fatalf("unsafe group state: %+v", w)
					}
				}
				if partial != "held" {
					if _, err := store.RevokeTaskAuthority(ctx, attempt.ID, "test terminal while ambiguous", now); err != nil {
						t.Fatal(err)
					}
					stale, err := store.TaskWakesAwaitingDelivery(ctx, now)
					if err != nil || len(stale) != 1 || len(stale[0].Waits) != 2 {
						t.Fatalf("terminal attempt hid ambiguity: %+v %v", stale, err)
					}
					if claimed, _ := store.TransitionTaskWake(ctx, member.ID, expected, "delivered", now); claimed {
						t.Fatal("message existence cannot prove grouped payload")
					}
				}
			})
		}
	}
}
