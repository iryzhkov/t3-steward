package wait

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// goneControl is a T3 whose answer about one thread the test chooses per tick:
// an error (T3 did not answer), a nil thread (T3 answered and does not hold it)
// or a live thread.
type goneControl struct {
	nativeControl
	thread *domain.Thread
	getErr error
	gets   int
}

func (c *goneControl) GetThread(context.Context, string) (*domain.Thread, error) {
	c.gets++
	return c.thread, c.getErr
}

// ReconcileNodeWake answers unknown, which is what a receipt lookup on a
// thread T3 no longer holds can say.
func (c *goneControl) ReconcileNodeWake(context.Context, string, string) (WakeReceiptStatus, error) {
	return WakeReceiptUnknown, nil
}

func goneWait(id string, created time.Time, delivery string) domain.NodeWait {
	settled := created.Add(time.Hour)
	return domain.NodeWait{
		Request:     domain.NodeWaitRequest{ID: id, ThreadID: "thread", Name: "window", Group: "g", Wake: domain.WakeAll},
		Host:        "here",
		CreatedAt:   created,
		SettledAt:   &settled,
		Observation: &domain.NodeObservation{ExitCode: 2, Outcome: domain.TaskWaitTimedOut, Reason: "timed out"},
		DeliveryID:  "node-wake:" + id,
		Delivery:    delivery,
	}
}

func deliveries(store *nativeGroupMemory) map[string]string {
	out := map[string]string{}
	for _, w := range store.waits {
		out[w.Request.ID] = w.Delivery
	}
	return out
}

// The field case of 2026-09-24: a --wake all group of three on a thread the
// archive deleted. The earliest member carries the send, so it alone went
// offline; the other two stayed pending behind it. Every tick T3 answered and
// did not hold the thread, and every tick the group was retried, forever.
//
// Once T3 has answered without the thread for longer than the confirmation
// window, the thread is gone and nothing will ever deliver the wake: the whole
// group is rejected, nothing is sent, and the next tick does not ask again.
func TestAWakeForAThreadT3NoLongerHoldsIsRejected(t *testing.T) {
	now := time.Date(2026, 9, 26, 10, 30, 0, 0, time.UTC)
	store := &nativeGroupMemory{waits: []domain.NodeWait{
		goneWait("nw-4fa1908a", now.Add(-72*time.Hour), "offline"),
		goneWait("nw-f2bf0274", now.Add(-72*time.Hour+13*time.Second), "pending"),
		goneWait("nw-23326045", now.Add(-72*time.Hour+29*time.Second), "pending"),
	}}
	control := &goneControl{}
	runner := New(store, control, nil)
	runner.NodeHost = "here"
	runner.SetClock(func() time.Time { return now })

	// First answer without the thread: not yet confirmed, so retryable.
	runner.Tick(context.Background(), nil, nil)
	if got := deliveries(store); got["nw-4fa1908a"] != "offline" || got["nw-f2bf0274"] != "pending" {
		t.Fatalf("one answer without the thread already ended delivery: %v", got)
	}
	// Still absent after the confirmation window: terminal for every member.
	now = now.Add(threadGoneConfirm + time.Second)
	runner.Tick(context.Background(), nil, nil)
	for id, delivery := range deliveries(store) {
		if delivery != "rejected" {
			t.Fatalf("%s is %s after T3 confirmed its thread is gone: %v", id, delivery, deliveries(store))
		}
	}
	if control.sends != 0 {
		t.Fatalf("a wake was sent into a thread T3 does not hold (%d sends)", control.sends)
	}
	gets := control.gets
	now = now.Add(time.Hour)
	runner.Tick(context.Background(), nil, nil)
	if control.gets != gets {
		t.Fatalf("a rejected wake was retried: %d more thread lookups", control.gets-gets)
	}
}

// A T3 that does not answer -- a transport fault, a refused credential, a
// server error -- says nothing about the thread. However long it lasts, the
// wake stays retryable, and it is delivered once T3 answers with the thread.
func TestATransientT3FailureNeverRejectsAWake(t *testing.T) {
	now := time.Date(2026, 9, 26, 10, 30, 0, 0, time.UTC)
	wake := goneWait("nw-1", now.Add(-time.Hour), "pending")
	wake.Request.Group, wake.Request.Wake = "", ""
	store := &nativeGroupMemory{waits: []domain.NodeWait{wake}}
	control := &goneControl{getErr: errors.New("T3 API 503")}
	runner := New(store, control, nil)
	runner.NodeHost = "here"
	runner.SetClock(func() time.Time { return now })
	for i := 0; i < 10; i++ {
		runner.Tick(context.Background(), nil, nil)
		if got := store.waits[0].Delivery; got != "offline" {
			t.Fatalf("tick %d: a T3 that did not answer moved the wake to %s", i, got)
		}
		now = now.Add(threadGoneConfirm)
	}
	control.getErr = nil
	control.thread = &domain.Thread{ID: "thread"}
	runner.Tick(context.Background(), nil, nil)
	if store.waits[0].Delivery != "sending" || control.sends != 1 {
		t.Fatalf("the wake was not delivered once T3 recovered: %s, %d sends", store.waits[0].Delivery, control.sends)
	}
}

// One answer without the thread followed by one with it is a T3 that was not
// ready (a server still loading its read model answers with an empty list),
// not a deleted thread. The confirmation starts again and the wake is sent.
func TestAThreadThatReappearsIsNotRejected(t *testing.T) {
	now := time.Date(2026, 9, 26, 10, 30, 0, 0, time.UTC)
	wake := goneWait("nw-1", now.Add(-time.Hour), "pending")
	wake.Request.Group, wake.Request.Wake = "", ""
	store := &nativeGroupMemory{waits: []domain.NodeWait{wake}}
	control := &goneControl{}
	runner := New(store, control, nil)
	runner.NodeHost = "here"
	runner.SetClock(func() time.Time { return now })
	runner.Tick(context.Background(), nil, nil)
	now = now.Add(threadGoneConfirm / 2)
	control.thread = &domain.Thread{ID: "thread"}
	runner.Tick(context.Background(), nil, nil)
	if store.waits[0].Delivery != "sending" || control.sends != 1 {
		t.Fatalf("a thread that reappeared was not woken: %s, %d sends", store.waits[0].Delivery, control.sends)
	}
}

// A wake whose send outcome is unknown is reconciled against its receipt and
// never resent. When the thread itself is gone there is no receipt to find and
// nothing to resend into, so it is rejected rather than reconciled forever.
func TestAnUnknownSendIntoAGoneThreadIsRejected(t *testing.T) {
	now := time.Date(2026, 9, 26, 10, 30, 0, 0, time.UTC)
	wake := goneWait("nw-1", now.Add(-time.Hour), "recovery-required")
	wake.Request.Group, wake.Request.Wake = "", ""
	store := &nativeGroupMemory{waits: []domain.NodeWait{wake}}
	control := &goneControl{}
	runner := New(store, control, nil)
	runner.NodeHost = "here"
	runner.SetClock(func() time.Time { return now })
	runner.Tick(context.Background(), nil, nil)
	now = now.Add(threadGoneConfirm + time.Second)
	runner.Tick(context.Background(), nil, nil)
	if store.waits[0].Delivery != "rejected" || control.sends != 0 {
		t.Fatalf("an unknown send into a gone thread is %s after %d sends", store.waits[0].Delivery, control.sends)
	}
}

// racingMemory moves one wait to another delivery state immediately before
// the first transition it is asked for, which is what a delivery tick in
// another process does between the archive's list and its transition.
type racingMemory struct {
	*nativeGroupMemory
	raceID, raceTo string
	raced          bool
}

func (s *racingMemory) TransitionNodeWake(ctx context.Context, id, from, to string, at time.Time) (bool, error) {
	if !s.raced && id == s.raceID {
		s.raced = true
		for i := range s.waits {
			if s.waits[i].Request.ID == id {
				s.waits[i].Delivery = s.raceTo
			}
		}
	}
	return s.nativeGroupMemory.TransitionNodeWake(ctx, id, from, to, at)
}

// The archive deletes a thread and ends every wake this host still owes it,
// whatever state its delivery is in, including one a delivery tick claimed
// between the archive's read and its transition. Delivered, cancelled and
// other threads' wakes are left alone.
func TestArchiveRejectsTheWakesOfAThreadItDeletes(t *testing.T) {
	now := time.Date(2026, 9, 26, 10, 30, 0, 0, time.UTC)
	unsettled := goneWait("nw-unsettled", now, "pending")
	unsettled.SettledAt, unsettled.Observation = nil, nil
	other := goneWait("nw-other", now, "pending")
	other.Request.ThreadID = "another"
	foreign := goneWait("nw-foreign", now, "pending")
	foreign.Host = "elsewhere"
	store := &racingMemory{nativeGroupMemory: &nativeGroupMemory{waits: []domain.NodeWait{
		goneWait("nw-pending", now, "pending"),
		goneWait("nw-offline", now, "offline"),
		goneWait("nw-delivered", now, "delivered"),
		goneWait("nw-cancelled", now, "cancelled"),
		unsettled, other, foreign,
	}}, raceID: "nw-pending", raceTo: "sending"}
	runner := New(store, &goneControl{}, nil)
	runner.NodeHost = "here"
	runner.SetClock(func() time.Time { return now })

	rejected, err := runner.RejectThreadWakes(context.Background(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"nw-pending": "rejected", "nw-offline": "rejected", "nw-unsettled": "rejected",
		"nw-delivered": "delivered", "nw-cancelled": "cancelled", "nw-other": "pending", "nw-foreign": "pending",
	}
	got := deliveries(store.nativeGroupMemory)
	for id, delivery := range want {
		if got[id] != delivery {
			t.Fatalf("%s is %s, want %s (%v)", id, got[id], delivery, got)
		}
	}
	if rejected != 3 {
		t.Fatalf("reported %d rejected wakes, want 3", rejected)
	}
}

// The other order of the same race: the archive ends the wake first, and the
// delivery tick that follows finds nothing to send.
func TestADeliveryTickAfterTheArchiveSendsNothing(t *testing.T) {
	now := time.Date(2026, 9, 26, 10, 30, 0, 0, time.UTC)
	wake := goneWait("nw-1", now.Add(-time.Hour), "pending")
	wake.Request.Group, wake.Request.Wake = "", ""
	store := &nativeGroupMemory{waits: []domain.NodeWait{wake}}
	// T3 still lists the thread: the tick read it a moment before the delete.
	control := &goneControl{thread: &domain.Thread{ID: "thread"}}
	runner := New(store, control, nil)
	runner.NodeHost = "here"
	runner.SetClock(func() time.Time { return now })
	if _, err := runner.RejectThreadWakes(context.Background(), "thread"); err != nil {
		t.Fatal(err)
	}
	runner.Tick(context.Background(), nil, nil)
	if store.waits[0].Delivery != "rejected" || control.sends != 0 {
		t.Fatalf("a wake the archive ended was %s after %d sends", store.waits[0].Delivery, control.sends)
	}
}

// A store that cannot be reached leaves the wakes as they were and says so;
// the delivery tick's own confirmation still ends them later.
func TestArchiveRejectionReportsAnUnreachableStore(t *testing.T) {
	runner := New(localOnlyMemory{}, &goneControl{}, nil)
	runner.NodeHost = "here"
	runner.NodeStore = failingNodeStore{}
	if _, err := runner.RejectThreadWakes(context.Background(), "thread"); err == nil {
		t.Fatal("an unreachable store was reported as success")
	}
}

type failingNodeStore struct{}

func (failingNodeStore) SettleNodeWaits(context.Context, time.Time) error { return nil }
func (failingNodeStore) ListNodeWaits(context.Context) ([]domain.NodeWait, error) {
	return nil, errors.New("unavailable (node-wait on coordinator): connection refused")
}
func (failingNodeStore) TransitionNodeWake(context.Context, string, string, string, time.Time) (bool, error) {
	return false, errors.New("unavailable")
}
