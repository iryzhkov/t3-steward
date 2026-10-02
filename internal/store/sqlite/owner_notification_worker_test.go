package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/ownernotify"
)

// workerEvents is what a sink was told about workers, in order.
func workerEvents(sink *recordingSink) string {
	var events []string
	for _, n := range sink.delivered() {
		if ownernotify.WorkerEvent(n.Event) {
			events = append(events, string(n.Event)+":"+n.Worker)
		}
	}
	return strings.Join(events, ",")
}

// The field case of 2026-10-02: two workers did not come back after a reboot
// and nothing said so for over an hour. A worker that stays down past the
// threshold is reported once, however many passes and coordinator restarts
// the outage lasts, and its return is reported once. An outage shorter than
// the threshold, or one in maintenance, is reported to nobody.
func TestAWorkerOutageIsReportedOnceAndItsRecoveryOnce(t *testing.T) {
	store := openOwnerNotificationStore(t)
	now := time.Date(2026, 10, 2, 3, 38, 0, 0, time.UTC)
	lastSeen := now
	var workers []ownernotify.WorkerState
	source := func(context.Context) ([]ownernotify.WorkerState, error) { return workers, nil }
	sink := &recordingSink{name: "command", selection: ownernotify.Selection{Events: ownernotify.DefaultEvents()}}
	notifier := func() *ownernotify.Notifier {
		n := clockedNotifier(store, &now, sink)
		n.Workers = source
		return n
	}
	ticker := notifier()

	// Everyone connected.
	ticker.Tick(context.Background())
	// Disconnected, inside the grace period, and one draining worker.
	now = now.Add(5 * time.Minute)
	workers = []ownernotify.WorkerState{
		{WorkerID: "normandy", Since: lastSeen, LastSeen: lastSeen, DownFor: 5 * time.Minute},
		{WorkerID: "homelab", Since: lastSeen, LastSeen: lastSeen, DownFor: 5 * time.Minute},
	}
	ticker.Tick(context.Background())
	if got := workerEvents(sink); got != "" {
		t.Fatalf("an outage inside the grace period was reported: %s", got)
	}
	// Past the threshold; homelab stays in maintenance and is never Down.
	now = now.Add(10 * time.Minute)
	workers[0].Down, workers[0].DownFor = true, 15*time.Minute
	ticker.Tick(context.Background())
	ticker.Tick(context.Background())
	notifier().Tick(context.Background()) // a coordinator restart
	now = now.Add(time.Hour)
	workers[0].DownFor += time.Hour
	notifier().Tick(context.Background())
	if got := workerEvents(sink); got != "worker-down:normandy" {
		t.Fatalf("one outage was reported as %q, want one worker-down for normandy", got)
	}
	down := sink.delivered()[0]
	if down.LastSeen == nil || !down.LastSeen.Equal(lastSeen) || !strings.Contains(ownernotify.RenderDiscord(down), "systemctl --user restart t3-steward-worker") {
		t.Fatalf("the worker-down event does not say when the worker was last seen and how to restart it: %+v", down)
	}
	if down.Commands()["triage"] != "t3-steward triage" {
		t.Fatalf("the worker-down event names no next command: %v", down.Commands())
	}

	// Back: one recovery, then nothing more.
	workers = workers[1:]
	ticker.Tick(context.Background())
	ticker.Tick(context.Background())
	notifier().Tick(context.Background())
	if got := workerEvents(sink); got != "worker-down:normandy,worker-recovered:normandy" {
		t.Fatalf("the recovery was reported as %q", got)
	}

	// A second outage of the same worker is a new event.
	second := now.Add(time.Minute)
	now = now.Add(20 * time.Minute)
	workers = append(workers, ownernotify.WorkerState{WorkerID: "normandy", Since: second, LastSeen: second, Down: true, DownFor: 19 * time.Minute})
	ticker.Tick(context.Background())
	if got := workerEvents(sink); got != "worker-down:normandy,worker-recovered:normandy,worker-down:normandy" {
		t.Fatalf("a second outage was reported as %q", got)
	}
}

// failingSink refuses every send, so a row stays pending.
type failingSink struct{ recordingSink }

func (s *failingSink) Deliver(context.Context, ownernotify.Notification) error {
	return &ownernotify.DeliveryError{Reason: "receiver down"}
}

// An outage that ends while its notification is still waiting for a retry is
// dropped, and no recovery follows: nobody was told it was down.
func TestAnOutageThatEndsBeforeItWasDeliveredIsDropped(t *testing.T) {
	store := openOwnerNotificationStore(t)
	now := time.Date(2026, 10, 2, 3, 38, 0, 0, time.UTC)
	since := now.Add(-20 * time.Minute)
	workers := []ownernotify.WorkerState{{WorkerID: "normandy", Since: since, LastSeen: since, Down: true, DownFor: 20 * time.Minute}}
	sink := &failingSink{recordingSink{name: "command", selection: ownernotify.Selection{Events: ownernotify.DefaultEvents()}}}
	notifier := clockedNotifier(store, &now, sink)
	notifier.Workers = func(context.Context) ([]ownernotify.WorkerState, error) { return workers, nil }
	notifier.Tick(context.Background())
	id := ownerNotificationID("command", ownernotify.EventWorkerDown, workerOutageSubject("normandy", since))
	if state, attempts := ownerNotificationState(t, store, id); state != "pending" || attempts != 1 {
		t.Fatalf("the failed send is %s after %d attempts", state, attempts)
	}
	workers = nil
	now = now.Add(time.Hour)
	notifier.Tick(context.Background())
	if state, _ := ownerNotificationState(t, store, id); state != "resolved" {
		t.Fatalf("an outage that ended before it was delivered is %s", state)
	}
	var recoveries int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM coordinator_owner_notifications WHERE event = 'worker-recovered'`).Scan(&recoveries); err != nil {
		t.Fatal(err)
	}
	if recoveries != 0 {
		t.Fatalf("a recovery was recorded for an outage nobody was told about (%d rows)", recoveries)
	}
}

// A worker view that cannot be read says nothing about the workers: it must
// not read as every worker having recovered.
func TestAnUnreadableWorkerViewIsNotARecovery(t *testing.T) {
	store := openOwnerNotificationStore(t)
	now := time.Date(2026, 10, 2, 3, 38, 0, 0, time.UTC)
	since := now.Add(-20 * time.Minute)
	sink := &recordingSink{name: "command", selection: ownernotify.Selection{Events: ownernotify.DefaultEvents()}}
	notifier := clockedNotifier(store, &now, sink)
	var fail bool
	notifier.Workers = func(context.Context) ([]ownernotify.WorkerState, error) {
		if fail {
			return nil, context.DeadlineExceeded
		}
		return []ownernotify.WorkerState{{WorkerID: "normandy", Since: since, Down: true, DownFor: 20 * time.Minute}}, nil
	}
	notifier.Tick(context.Background())
	fail = true
	notifier.Tick(context.Background())
	if got := workerEvents(sink); got != "worker-down:normandy" {
		t.Fatalf("an unreadable worker view was reported as %q", got)
	}
}

// A sink that does not select the worker events is told nothing about workers.
func TestWorkerEventsFollowTheSinksSelection(t *testing.T) {
	store := openOwnerNotificationStore(t)
	now := time.Date(2026, 10, 2, 3, 38, 0, 0, time.UTC)
	sink := &recordingSink{name: "command", selection: ownernotify.Selection{Events: []ownernotify.Event{ownernotify.EventRunFailed}}}
	notifier := clockedNotifier(store, &now, sink)
	notifier.Workers = func(context.Context) ([]ownernotify.WorkerState, error) {
		return []ownernotify.WorkerState{{WorkerID: "normandy", Since: now.Add(-time.Hour), Down: true, DownFor: time.Hour}}, nil
	}
	notifier.Tick(context.Background())
	if got := workerEvents(sink); got != "" {
		t.Fatalf("a sink without worker events was told %q", got)
	}
}
