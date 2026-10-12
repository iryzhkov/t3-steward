package wait

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type coalesceMemory struct{ nativeGroupMemory }

func (s *coalesceMemory) ClaimNodeWakeGroup(_ context.Context, leader, from, delivery, payload string, ids []string, _ time.Time) (bool, error) {
	for i := range s.waits {
		w := &s.waits[i]
		for _, id := range ids {
			if w.Request.ID == id {
				if w.Delivery != from {
					return false, nil
				}
				w.Delivery, w.DeliveryID, w.DeliveryPayload, w.DeliveryGroupMembers = "sending", delivery, payload, append([]string(nil), ids...)
			}
		}
	}
	return true, nil
}

func coalesceWait(id string, at time.Time) domain.NodeWait {
	ref := domain.NodeRef{RunID: id}
	return domain.NodeWait{Request: domain.NodeWaitRequest{ID: id, ThreadID: "thread", Target: ref}, Host: "host", SettledAt: &at,
		Delivery: "pending", DeliveryID: id, Observation: &domain.NodeObservation{Target: ref, Progress: domain.ProgressSucceeded}}
}

func TestCoalescedWakesOneMessage(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	now := start.Add(time.Minute)
	store := &coalesceMemory{nativeGroupMemory{[]domain.NodeWait{coalesceWait("c", now), coalesceWait("a", start), coalesceWait("b", start)}}}
	control := &receiptControl{thread: &domain.Thread{ID: "thread"}, receipt: WakeReceiptKnownNoEffect}
	newRunner := func() *Runner {
		r := New(store, control, nil)
		r.NodeHost = "host"
		r.NodeCoalesceWindow = 2 * time.Minute
		r.SetClock(func() time.Time { return now })
		return r
	}
	r := newRunner()
	r.tickNodes(ctx)
	deadline, ok := r.NextNodeWakeDeadline()
	if !ok || !deadline.Equal(start.Add(2*time.Minute)) || control.sends != 0 {
		t.Fatal(deadline, ok, control.sends)
	}
	r = newRunner()
	r.tickNodes(ctx)
	if d, _ := r.NextNodeWakeDeadline(); !d.Equal(deadline) {
		t.Fatal("restart changed deadline")
	}
	now = deadline
	r.tickNodes(ctx)
	if control.sends != 1 {
		t.Fatal(control.texts)
	}
	text := control.texts[0]
	if !strings.Contains(text, "count=3") || strings.Count(text, wakeGuidance) != 1 ||
		strings.Index(text, "Run \"a\"") >= strings.Index(text, "Run \"b\"") || strings.Index(text, "Run \"b\"") >= strings.Index(text, "Run \"c\"") {
		t.Fatal(text)
	}
	store.waits = append(store.waits, coalesceWait("d", now))
	r.tickNodes(ctx) // known no effect: return frozen members to offline
	r.tickNodes(ctx) // resend the exact batch; d waits its own window
	if control.sends != 2 || control.texts[1] != text {
		t.Fatal(control.texts)
	}
	control.receipt = WakeReceiptDelivered
	r.tickNodes(ctx)
	for _, w := range store.waits[:3] {
		if w.Delivery != "delivered" {
			t.Fatal(w)
		}
	}
	if d, ok := r.NextNodeWakeDeadline(); !ok || !d.Equal(now.Add(2*time.Minute)) {
		t.Fatal(d, ok)
	}
}

func TestCoalescingFallsBackWithoutGroupClaim(t *testing.T) {
	for _, unsupported := range []bool{false, true} {
		for _, window := range []time.Duration{0, time.Minute} {
			now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
			waits := []domain.NodeWait{coalesceWait("a", now), coalesceWait("b", now)}
			base := &nativeGroupMemory{waits}
			var store NodeStore = base
			if unsupported {
				store = &unsupportedGroupStore{nativeGroupMemory: *base}
			}
			control := &nativeControl{}
			r := New(base, control, nil)
			r.NodeStore = store
			r.NodeHost = "host"
			r.NodeCoalesceWindow = window
			r.SetClock(func() time.Time { return now })
			r.tickNodes(context.Background())
			if window > 0 && control.sends != 0 {
				t.Fatal("early send")
			}
			now = now.Add(window)
			r.tickNodes(context.Background())
			if control.sends != len(waits) {
				t.Fatal(control.texts)
			}
			for i, text := range control.texts {
				if text != nodeWakeMessage(waits[i], nil) {
					t.Fatal(text)
				}
			}
		}
	}
}

func TestCoalescedWakeUsesRecordedStateLine(t *testing.T) {
	w := coalesceWait("unsafe;run", time.Time{})
	text := coalescedNodeMessage([]domain.NodeWait{w, coalesceWait("safe", time.Time{})}, []*WakeSummary{nil, {Headline: "actual headline"}})
	if strings.Contains(text, "task result unsafe") || !strings.Contains(text, "recorded state") || !strings.Contains(text, "actual headline") {
		t.Fatal(text)
	}
	w.Observation = nil
	if text := recordedNodeStateLine(w); strings.Contains(text, "succeeded") || !strings.Contains(text, "unavailable") {
		t.Fatal(text)
	}
}
