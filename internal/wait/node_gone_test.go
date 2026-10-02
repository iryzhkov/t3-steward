package wait

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// goneControl is a T3 whose answer about one thread the test chooses per tick:
// an error (T3 did not answer, or answered with no thread at all), or one of
// the presences LookupThread distinguishes. An empty presence is absent.
type goneControl struct {
	nativeControl
	presence domain.ThreadPresence
	getErr   error
	lookups  int
}

func (c *goneControl) LookupThread(_ context.Context, id string) (*domain.Thread, domain.ThreadPresence, error) {
	c.lookups++
	if c.getErr != nil {
		return nil, "", c.getErr
	}
	switch c.presence {
	case "", domain.ThreadAbsent:
		return nil, domain.ThreadAbsent, nil
	case domain.ThreadLive:
		return &domain.Thread{ID: id}, domain.ThreadLive, nil
	default:
		return nil, c.presence, nil
	}
}

// GetThread is never the path a lookup-capable control is asked through.
func (c *goneControl) GetThread(context.Context, string) (*domain.Thread, error) {
	panic("GetThread used where LookupThread is available")
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

func single(w domain.NodeWait) domain.NodeWait {
	w.Request.Group, w.Request.Wake = "", ""
	return w
}

func deliveries(store *nativeGroupMemory) map[string]string {
	out := map[string]string{}
	for _, w := range store.waits {
		out[w.Request.ID] = w.Delivery
	}
	return out
}

// clockedRunner is a runner on a clock the test moves.
func clockedRunner(store Store, control Control, now *time.Time) *Runner {
	runner := New(store, control, nil)
	runner.NodeHost = "here"
	runner.SetClock(func() time.Time { return *now })
	return runner
}

// The field case of 2026-09-24: a --wake all group of three on a thread the
// archive deleted. The earliest member carries the send, so it alone went
// offline; the other two stayed pending behind it, forever.
//
// A thread T3 answers without is not gone at once: only three consecutive
// answers without it, spanning at least threadGoneConfirm, are. Then the
// whole group is rejected, nothing is sent, and the next tick does not ask.
func TestAWakeForAThreadT3NoLongerHoldsIsRejected(t *testing.T) {
	now := time.Date(2026, 9, 26, 10, 30, 0, 0, time.UTC)
	store := &nativeGroupMemory{waits: []domain.NodeWait{
		goneWait("nw-4fa1908a", now.Add(-72*time.Hour), "offline"),
		goneWait("nw-f2bf0274", now.Add(-72*time.Hour+13*time.Second), "pending"),
		goneWait("nw-23326045", now.Add(-72*time.Hour+29*time.Second), "pending"),
	}}
	control := &goneControl{presence: domain.ThreadAbsent}
	runner := clockedRunner(store, control, &now)

	for i := 0; i < 2; i++ {
		runner.Tick(context.Background(), nil, nil)
		if got := deliveries(store); got["nw-4fa1908a"] != "offline" || got["nw-f2bf0274"] != "pending" {
			t.Fatalf("answer %d without the thread already ended delivery: %v", i+1, got)
		}
		now = now.Add(threadGoneConfirm / 2)
	}
	// Three answers, but only a minute apart counts: the window is now met.
	runner.Tick(context.Background(), nil, nil)
	for id, delivery := range deliveries(store) {
		if delivery != "rejected" {
			t.Fatalf("%s is %s after T3 confirmed its thread is gone: %v", id, delivery, deliveries(store))
		}
	}
	if control.sends != 0 {
		t.Fatalf("a wake was sent into a thread T3 does not hold (%d sends)", control.sends)
	}
	lookups := control.lookups
	now = now.Add(time.Hour)
	runner.Tick(context.Background(), nil, nil)
	if control.lookups != lookups {
		t.Fatalf("a rejected wake was looked up again: %d more lookups", control.lookups-lookups)
	}
}

// Three answers inside the window are not enough either: the absence has to
// last a minute.
func TestThreeQuickAnswersDoNotConfirmAThreadGone(t *testing.T) {
	now := time.Date(2026, 9, 26, 10, 30, 0, 0, time.UTC)
	store := &nativeGroupMemory{waits: []domain.NodeWait{single(goneWait("nw-1", now.Add(-time.Hour), "pending"))}}
	runner := clockedRunner(store, &goneControl{presence: domain.ThreadAbsent}, &now)
	for i := 0; i < 5; i++ {
		runner.Tick(context.Background(), nil, nil)
		now = now.Add(10 * time.Second)
	}
	if got := store.waits[0].Delivery; got != "offline" {
		t.Fatalf("five answers in 50 seconds moved the wake to %s", got)
	}
}

// A thread T3 reports deleted is authoritative: the wake ends on the first
// answer, as does one whose thread is archived.
func TestADeletedOrArchivedThreadEndsItsWakeAtOnce(t *testing.T) {
	for _, presence := range []domain.ThreadPresence{domain.ThreadDeleted, domain.ThreadArchived} {
		now := time.Date(2026, 9, 26, 10, 30, 0, 0, time.UTC)
		store := &nativeGroupMemory{waits: []domain.NodeWait{single(goneWait("nw-1", now.Add(-time.Hour), "pending"))}}
		control := &goneControl{presence: presence}
		clockedRunner(store, control, &now).Tick(context.Background(), nil, nil)
		if got := store.waits[0].Delivery; got != "rejected" || control.sends != 0 {
			t.Fatalf("a %s thread left the wake %s after %d sends", presence, got, control.sends)
		}
	}
}

// A T3 that does not answer -- a transport fault, a refused credential, a
// server error, an answer with no thread at all -- says nothing about the
// thread. However long it lasts, the wake stays retryable, and it is
// delivered once T3 answers with the thread.
func TestATransientT3FailureNeverRejectsAWake(t *testing.T) {
	now := time.Date(2026, 9, 26, 10, 30, 0, 0, time.UTC)
	store := &nativeGroupMemory{waits: []domain.NodeWait{single(goneWait("nw-1", now.Add(-time.Hour), "pending"))}}
	control := &goneControl{getErr: errors.New("T3 API 503")}
	runner := clockedRunner(store, control, &now)
	for i := 0; i < 10; i++ {
		runner.Tick(context.Background(), nil, nil)
		if got := store.waits[0].Delivery; got != "offline" {
			t.Fatalf("tick %d: a T3 that did not answer moved the wake to %s", i, got)
		}
		now = now.Add(threadGoneConfirm)
	}
	control.getErr, control.presence = nil, domain.ThreadLive
	runner.Tick(context.Background(), nil, nil)
	if store.waits[0].Delivery != "sending" || control.sends != 1 {
		t.Fatalf("the wake was not delivered once T3 recovered: %s, %d sends", store.waits[0].Delivery, control.sends)
	}
}

// An answer that is not an answer resets the confirmation: missing, then a
// long stretch of errors, then missing again is two short absences, not one
// long one.
func TestAnUnansweredLookupResetsTheConfirmation(t *testing.T) {
	now := time.Date(2026, 9, 26, 10, 30, 0, 0, time.UTC)
	store := &nativeGroupMemory{waits: []domain.NodeWait{single(goneWait("nw-1", now.Add(-time.Hour), "pending"))}}
	control := &goneControl{presence: domain.ThreadAbsent}
	runner := clockedRunner(store, control, &now)
	runner.Tick(context.Background(), nil, nil)
	now = now.Add(20 * time.Second)
	runner.Tick(context.Background(), nil, nil)
	control.getErr = errors.New("connection refused")
	for i := 0; i < 5; i++ {
		now = now.Add(threadGoneConfirm)
		runner.Tick(context.Background(), nil, nil)
	}
	control.getErr = nil
	for i := 0; i < 2; i++ {
		now = now.Add(20 * time.Second)
		runner.Tick(context.Background(), nil, nil)
	}
	if got := store.waits[0].Delivery; got == "rejected" {
		t.Fatal("missing, errors, then missing again was counted as one confirmed absence")
	}
}

// One answer without the thread followed by one with it is a T3 that was not
// ready, not a deleted thread. The confirmation starts again and the wake is
// sent.
func TestAThreadThatReappearsIsNotRejected(t *testing.T) {
	now := time.Date(2026, 9, 26, 10, 30, 0, 0, time.UTC)
	store := &nativeGroupMemory{waits: []domain.NodeWait{single(goneWait("nw-1", now.Add(-time.Hour), "pending"))}}
	control := &goneControl{presence: domain.ThreadAbsent}
	runner := clockedRunner(store, control, &now)
	runner.Tick(context.Background(), nil, nil)
	now = now.Add(threadGoneConfirm / 2)
	control.presence = domain.ThreadLive
	runner.Tick(context.Background(), nil, nil)
	if store.waits[0].Delivery != "sending" || control.sends != 1 {
		t.Fatalf("a thread that reappeared was not woken: %s, %d sends", store.waits[0].Delivery, control.sends)
	}
}

// A wake whose send outcome is unknown is reconciled against its receipt and
// never resent. When the thread itself is confirmed gone there is no receipt
// to find and nothing to resend into, so it is rejected -- but not before the
// confirmation: a sending wake whose thread has been missing for less than the
// window stays where it is.
func TestAnUnknownSendIntoAGoneThreadIsRejectedOnlyOnceConfirmed(t *testing.T) {
	for _, delivery := range []string{"sending", "recovery-required"} {
		now := time.Date(2026, 9, 26, 10, 30, 0, 0, time.UTC)
		store := &nativeGroupMemory{waits: []domain.NodeWait{single(goneWait("nw-1", now.Add(-time.Hour), delivery))}}
		control := &goneControl{presence: domain.ThreadAbsent}
		runner := clockedRunner(store, control, &now)
		for i := 0; i < 3; i++ {
			runner.Tick(context.Background(), nil, nil)
			now = now.Add(15 * time.Second)
		}
		if got := store.waits[0].Delivery; got == "rejected" {
			t.Fatalf("a %s wake was rejected after 30 seconds of absence", delivery)
		}
		now = now.Add(threadGoneConfirm)
		runner.Tick(context.Background(), nil, nil)
		if got := store.waits[0].Delivery; got != "rejected" || control.sends != 0 {
			t.Fatalf("a %s wake into a confirmed gone thread is %s after %d sends", delivery, got, control.sends)
		}
	}
}

// racingMemory moves one wait to another delivery state immediately before
// the transitions it is asked for, which is what a delivery tick in another
// process does between the archive's list and its transition. times is how
// many transitions it races; a negative value races every one.
type racingMemory struct {
	*nativeGroupMemory
	raceID string
	times  int
	raced  int
}

func (s *racingMemory) TransitionNodeWake(ctx context.Context, id, from, to string, at time.Time) (bool, error) {
	if id == s.raceID && (s.times < 0 || s.raced < s.times) {
		s.raced++
		for i := range s.waits {
			if s.waits[i].Request.ID == id {
				// Alternate, so every read sees a state that has moved on.
				if s.waits[i].Delivery == "sending" {
					s.waits[i].Delivery = "recovery-required"
				} else {
					s.waits[i].Delivery = "sending"
				}
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
	}}, raceID: "nw-pending", times: 1}
	runner := clockedRunner(store, &goneControl{}, &now)

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

// A wake that keeps moving under every bounded retry is reported as cleanup
// the archive could not finish, naming the wake, rather than as success.
func TestArchiveReportsCleanupItCouldNotFinish(t *testing.T) {
	now := time.Date(2026, 9, 26, 10, 30, 0, 0, time.UTC)
	store := &racingMemory{nativeGroupMemory: &nativeGroupMemory{waits: []domain.NodeWait{
		goneWait("nw-busy", now, "pending"),
	}}, raceID: "nw-busy", times: -1}
	runner := clockedRunner(store, &goneControl{}, &now)
	_, err := runner.RejectThreadWakes(context.Background(), "thread")
	if err == nil || !strings.Contains(err.Error(), "incomplete") || !strings.Contains(err.Error(), "nw-busy") {
		t.Fatalf("a wake that never stopped moving was reported as %v", err)
	}
}

// The other order of the same race: the archive ends the wake first, and the
// delivery tick that follows finds nothing to send.
func TestADeliveryTickAfterTheArchiveSendsNothing(t *testing.T) {
	now := time.Date(2026, 9, 26, 10, 30, 0, 0, time.UTC)
	store := &nativeGroupMemory{waits: []domain.NodeWait{single(goneWait("nw-1", now.Add(-time.Hour), "pending"))}}
	// T3 still lists the thread: the tick read it a moment before the delete.
	control := &goneControl{presence: domain.ThreadLive}
	runner := clockedRunner(store, control, &now)
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
