package sqlite

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func askPhaseEvents(t *testing.T, store *Store, askID string) []domain.NodeWait {
	t.Helper()
	waits, err := store.ListNodeWaits(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var events []domain.NodeWait
	for _, w := range waits {
		if strings.HasPrefix(w.Request.ID, askNotificationPrefix) && strings.HasPrefix(w.Request.Name, "ask "+askID+" ") {
			events = append(events, w)
		}
	}
	return events
}

func askPhaseEvent(t *testing.T, store *Store, askID, phase string) domain.NodeWait {
	t.Helper()
	for _, event := range askPhaseEvents(t, store, askID) {
		if event.Request.ID == askNotificationID(askID, phase) {
			return event
		}
	}
	t.Fatalf("missing %s event for %s", phase, askID)
	return domain.NodeWait{}
}

func TestAskMidpointDurableReconciliation(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "registered", true: "legacy"}[legacy], func(t *testing.T) {
			ctx := context.Background()
			store, attempt, _ := taskWaitFixture(t)
			now := time.Date(2026, 10, 12, 15, 4, 0, 0, time.UTC)
			askSinkNotify(t, store, "notify", "r", "A", now)
			reg := askRegistration(attempt, "midpoint", pickAsk(), 4*time.Hour)
			ask, err := store.RegisterTaskWait(ctx, reg, now)
			if err != nil {
				t.Fatal(err)
			}
			initial := askPhaseEvent(t, store, ask.ID, "registered")
			if legacy {
				if _, err := store.db.Exec("DELETE FROM coordinator_node_waits WHERE id=?", initial.Request.ID); err != nil {
					t.Fatal(err)
				}
			}
			reopen := func() {
				t.Helper()
				path := store.path
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				store, err = Open(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { store.Close() })
			}
			reopen()
			midpoint := now.Add(2 * time.Hour)
			if !legacy {
				if err := store.SettleNodeWaits(ctx, midpoint.Add(-time.Second)); err != nil {
					t.Fatal(err)
				}
				if len(askPhaseEvents(t, store, ask.ID)) != 1 {
					t.Fatal("reminder before midpoint")
				}
			}
			if err := store.SettleNodeWaits(ctx, midpoint); err != nil {
				t.Fatal(err)
			}
			first := askPhaseEvent(t, store, ask.ID, "registered")
			second := askPhaseEvent(t, store, ask.ID, "midpoint")
			if len(askPhaseEvents(t, store, ask.ID)) != 2 || !strings.HasPrefix(second.DeliveryPayload, "Reminder:") || first.CreatedAt.After(second.CreatedAt) {
				t.Fatal(first, second)
			}
			var firstRow, secondRow int
			if err := store.db.QueryRow("SELECT rowid FROM coordinator_node_waits WHERE id=?", first.Request.ID).Scan(&firstRow); err != nil {
				t.Fatal(err)
			}
			if err := store.db.QueryRow("SELECT rowid FROM coordinator_node_waits WHERE id=?", second.Request.ID).Scan(&secondRow); err != nil || firstRow >= secondRow {
				t.Fatal("initial must be enqueued before midpoint", err)
			}
			if !legacy && !reflect.DeepEqual(first, initial) {
				t.Fatal("initial payload or state reset")
			}
			reopen()
			if _, err := store.RegisterTaskWait(ctx, reg, midpoint.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			if err := store.SettleNodeWaits(ctx, midpoint.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			if len(askPhaseEvents(t, store, ask.ID)) != 2 || !reflect.DeepEqual(second, askPhaseEvent(t, store, ask.ID, "midpoint")) {
				t.Fatal("restart reset or duplicated reminder")
			}
		})
	}
}

func TestAskMidpointSettlementCleanup(t *testing.T) {
	for _, mode := range []string{"answer", "cancel", "expire", "deadline", "after-deadline", "busy", "sending", "recovery-required"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			store, attempt, now := taskWaitFixture(t)
			askSinkNotify(t, store, "notify", "r", "A", now)
			ask, err := store.RegisterTaskWait(ctx, askRegistration(attempt, mode, pickAsk(), 4*time.Hour), now)
			if err != nil {
				t.Fatal(err)
			}
			queued := mode == "busy" || mode == "sending" || mode == "recovery-required"
			at := now.Add(time.Hour)
			if queued {
				at = now.Add(2 * time.Hour)
				if err := store.SettleNodeWaits(ctx, at); err != nil {
					t.Fatal(err)
				}
				id := askNotificationID(ask.ID, "midpoint")
				if mode != "recovery-required" {
					if _, err := store.TransitionNodeWake(ctx, id, "pending", mode, at); err != nil {
						t.Fatal(err)
					}
				} else {
					if _, err := store.TransitionNodeWake(ctx, id, "pending", "sending", at); err != nil {
						t.Fatal(err)
					}
					if _, err := store.TransitionNodeWake(ctx, id, "sending", mode, at); err != nil {
						t.Fatal(err)
					}
				}
			}
			switch mode {
			case "cancel":
				_, err = store.CancelTaskWait(ctx, ask.ID, at)
			case "expire":
				_, err = store.ExpireTaskWaits(ctx, ask.Deadline)
			case "deadline", "after-deadline":
			default:
				_, err = store.AnswerAsk(ctx, domain.AskAnswer{AskID: ask.ID, Options: []string{ask.Ask.Options[0]}, Source: domain.AskSourceCLI}, "operator", false, at)
			}
			if err != nil {
				t.Fatal(err)
			}
			detect := ask.Deadline
			if mode == "after-deadline" {
				detect = detect.Add(time.Second)
			} else if mode != "deadline" && mode != "expire" {
				detect = now.Add(3 * time.Hour)
			}
			if err := store.SettleNodeWaits(ctx, detect); err != nil {
				t.Fatal(err)
			}
			if !queued {
				if len(askPhaseEvents(t, store, ask.ID)) != 1 {
					t.Fatal("settled or expired ask gained reminder")
				}
				return
			}
			event := askPhaseEvent(t, store, ask.ID, "midpoint")
			want, pins := mode, 1
			if mode == "busy" {
				want, pins = "cancelled", 0
			}
			var gotPins int
			if err := store.db.QueryRow("SELECT count(*) FROM coordinator_retention_pins WHERE owner=?", "wait:"+event.Request.ID).Scan(&gotPins); err != nil || gotPins != pins || event.Delivery != want {
				t.Fatalf("event=%+v pins=%d err=%v", event, gotPins, err)
			}
		})
	}
}
