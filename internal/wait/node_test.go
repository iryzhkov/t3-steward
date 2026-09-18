package wait

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type nativeMemory struct {
	w       domain.NodeWait
	settles int
}

func (s *nativeMemory) SaveWait(context.Context, Wait) error                    { return nil }
func (s *nativeMemory) ListWaits(context.Context, string) ([]Wait, error)       { return nil, nil }
func (s *nativeMemory) RecordAction(context.Context, domain.ActionRecord) error { return nil }
func (s *nativeMemory) SettleNodeWaits(context.Context, time.Time) error        { s.settles++; return nil }
func (s *nativeMemory) ListNodeWaits(context.Context) ([]domain.NodeWait, error) {
	return []domain.NodeWait{s.w}, nil
}
func (s *nativeMemory) TransitionNodeWake(_ context.Context, id, from, to string, _ time.Time) (bool, error) {
	if s.w.Request.ID != id || s.w.Delivery != from {
		return false, nil
	}
	s.w.Delivery = to
	return true, nil
}

type nativeControl struct {
	sends int
	seen  bool
	fail  bool
	texts []string
}

func (c *nativeControl) GetThread(context.Context, string) (*domain.Thread, error) {
	return &domain.Thread{ID: "thread"}, nil
}
func (c *nativeControl) ResumeThread(context.Context, domain.Thread, string) error {
	panic("native wake used unsafe legacy send")
}
func (c *nativeControl) ObserveNodeWake(context.Context, string, string) (bool, error) {
	return c.seen, nil
}
func (c *nativeControl) SendNodeWake(_ context.Context, _ domain.Thread, _ string, text string) error {
	c.sends++
	c.texts = append(c.texts, text)
	if c.fail {
		return errors.New("response lost")
	}
	return nil
}
func TestNativeWakeHeldRestartLostResponseAndObservation(t *testing.T) {
	now := time.Now()
	store := &nativeMemory{w: domain.NodeWait{Request: domain.NodeWaitRequest{ID: "nw-test", ThreadID: "thread"}, Host: "host", SettledAt: &now, Observation: &domain.NodeObservation{ExitCode: 0}, DeliveryID: "token", Delivery: "pending"}}
	control := &nativeControl{fail: true}
	runner := New(store, control, slog.New(slog.NewTextHandler(io.Discard, nil)))
	runner.NodeHost = "host"
	runner.NodeDryRun = true
	runner.Tick(context.Background(), nil, nil)
	runner.Tick(context.Background(), nil, nil)
	if store.w.Delivery != "held" || control.sends != 0 {
		t.Fatal(store.w, control.sends)
	}
	// Recreate the process and turn only native delivery on.
	runner = New(store, control, nil)
	runner.NodeHost = "host"
	runner.Tick(context.Background(), nil, nil)
	if control.sends != 1 || store.w.Delivery != "recovery-required" {
		t.Fatal(control.sends, store.w.Delivery)
	}
	runner = New(store, control, nil)
	runner.NodeHost = "host"
	runner.Tick(context.Background(), nil, nil)
	if control.sends != 1 || store.w.Delivery != "recovery-required" {
		t.Fatal("ambiguous absence rearmed send")
	}
	control.seen = true
	runner.Tick(context.Background(), nil, nil)
	runner.Tick(context.Background(), nil, nil)
	if control.sends != 1 || store.w.Delivery != "delivered" {
		t.Fatal(control.sends, store.w.Delivery)
	}
}

// nativeGroupMemory holds several node waits, for a --wake all group.
type nativeGroupMemory struct {
	waits []domain.NodeWait
}

func (s *nativeGroupMemory) SaveWait(context.Context, Wait) error                    { return nil }
func (s *nativeGroupMemory) ListWaits(context.Context, string) ([]Wait, error)       { return nil, nil }
func (s *nativeGroupMemory) RecordAction(context.Context, domain.ActionRecord) error { return nil }
func (s *nativeGroupMemory) SettleNodeWaits(context.Context, time.Time) error        { return nil }
func (s *nativeGroupMemory) ListNodeWaits(context.Context) ([]domain.NodeWait, error) {
	return append([]domain.NodeWait(nil), s.waits...), nil
}
func (s *nativeGroupMemory) TransitionNodeWake(_ context.Context, id, from, to string, _ time.Time) (bool, error) {
	for i := range s.waits {
		if s.waits[i].Request.ID != id || s.waits[i].Delivery != from {
			continue
		}
		s.waits[i].Delivery = to
		return true, nil
	}
	return false, nil
}

// A --wake all group whose earliest member was delivered, but whose other
// members were left pending by a crash between the send and their
// transitions, is sent again on the next tick for the members still
// pending; they are not skipped forever as "not the earliest".
func TestNodeGroupResendsTheMembersACrashLeftPending(t *testing.T) {
	now := time.Now()
	member := func(id string, created time.Time, delivery string) domain.NodeWait {
		return domain.NodeWait{
			Request:     domain.NodeWaitRequest{ID: id, ThreadID: "thread", Group: "pair", Wake: domain.WakeAll, Target: domain.NodeRef{RunID: "r", TaskID: id}},
			Host:        "host",
			CreatedAt:   created,
			SettledAt:   &now,
			Observation: &domain.NodeObservation{ExitCode: 0, Outcome: domain.TaskWaitMet},
			DeliveryID:  "token-" + id,
			Delivery:    delivery,
		}
	}
	store := &nativeGroupMemory{waits: []domain.NodeWait{
		member("nw-first", now.Add(-2*time.Minute), "delivered"),
		member("nw-second", now.Add(-time.Minute), "pending"),
		member("nw-third", now, "pending"),
	}}
	control := &nativeControl{}
	runner := New(store, control, nil)
	runner.NodeHost = "host"
	runner.Tick(context.Background(), nil, nil)
	if control.sends != 1 {
		t.Fatalf("the pending members were not sent: sends=%d texts=%v", control.sends, control.texts)
	}
	text := control.texts[0]
	if !strings.HasPrefix(text, "t3-steward-wait kind=node outcome=met wait=nw-second") || !strings.Contains(text, "count=2") || !strings.Contains(text, "## nw-third") || strings.Contains(text, "## nw-first") {
		t.Fatalf("the resend does not name exactly the pending members: %q", text)
	}
	for _, w := range store.waits {
		if w.Request.ID == "nw-third" && w.Delivery != "delivered" {
			t.Fatalf("the rider was left %s", w.Delivery)
		}
		if w.Request.ID == "nw-second" && w.Delivery != "sending" {
			t.Fatalf("the sender is %s, not awaiting its observation", w.Delivery)
		}
	}
	// Nothing is sent twice once every member is delivered.
	control.seen = true
	runner.Tick(context.Background(), nil, nil)
	runner.Tick(context.Background(), nil, nil)
	if control.sends != 1 {
		t.Fatalf("a delivered group was sent again: %d", control.sends)
	}
}

func TestNativeWakeWrongHostNeverDispatches(t *testing.T) {
	now := time.Now()
	store := &nativeMemory{w: domain.NodeWait{Request: domain.NodeWaitRequest{ID: "nw", ThreadID: "thread"}, Host: "other", SettledAt: &now, Observation: &domain.NodeObservation{ExitCode: 0}, Delivery: "pending"}}
	control := &nativeControl{}
	runner := New(store, control, nil)
	runner.NodeHost = "here"
	runner.Tick(context.Background(), nil, nil)
	if control.sends != 0 {
		t.Fatal("woke a different host's thread")
	}
}

// localOnlyMemory is the store of a host that runs no coordinator: it holds
// this host's own waits and none of the coordinator's node waits, which is why
// it deliberately does not answer NodeStore.
type localOnlyMemory struct{}

func (localOnlyMemory) SaveWait(context.Context, Wait) error                    { return nil }
func (localOnlyMemory) ListWaits(context.Context, string) ([]Wait, error)       { return nil, nil }
func (localOnlyMemory) RecordAction(context.Context, domain.ActionRecord) error { return nil }

// The wake of a wait registered from another host is delivered by the steward
// of that host and by no other, because the T3 thread it names exists only
// there. The coordinator holds the record; the calling host reads it over a
// transport and sends the message.
//
// This is the case the delivery tests were missing: every one of them set the
// wait's host and the runner's host to the same value, so a wait whose host was
// nobody's was indistinguishable from a wait that was delivered.
func TestANodeWakeIsDeliveredByTheRunnerOnTheWaitsOwnHost(t *testing.T) {
	now := time.Now()
	// One record, as the coordinator holds it, naming the calling host.
	coordinatorRecords := &nativeMemory{w: domain.NodeWait{
		Request:     domain.NodeWaitRequest{ID: "nw-caller", ThreadID: "thread", Name: "run-1/__sink"},
		Host:        "caller",
		SettledAt:   &now,
		Observation: &domain.NodeObservation{ExitCode: 0, Reason: "succeeded"},
		DeliveryID:  "token",
		Delivery:    "pending",
	}}

	// The coordinator's own steward reads the same record and leaves it alone.
	coordinatorControl := &nativeControl{}
	coordinator := New(coordinatorRecords, coordinatorControl, nil)
	coordinator.NodeHost = "coordinator"
	coordinator.Tick(context.Background(), nil, nil)
	if coordinatorControl.sends != 0 {
		t.Fatalf("the coordinator sent %d wakes into its own T3 for another host's thread", coordinatorControl.sends)
	}
	if coordinatorRecords.w.Delivery != "pending" {
		t.Fatalf("the coordinator moved another host's wake to %q", coordinatorRecords.w.Delivery)
	}

	// The calling host holds no coordinator records of its own and reads them
	// over its transport, which is what NodeStore is.
	callerControl := &nativeControl{}
	caller := New(localOnlyMemory{}, callerControl, nil)
	caller.NodeStore = coordinatorRecords
	caller.NodeHost = "caller"
	caller.Tick(context.Background(), nil, nil)
	if callerControl.sends != 1 || len(callerControl.texts) != 1 {
		t.Fatalf("the calling host sent %d wakes, want exactly one", callerControl.sends)
	}
	if !strings.HasPrefix(callerControl.texts[0], "t3-steward-wait kind=node outcome=met wait=nw-caller") {
		t.Fatalf("the delivered wake does not begin with the trailer: %q", firstLine(callerControl.texts[0]))
	}
	// The claim is durable in the coordinator's records, so a second steward
	// cannot send the same wake again.
	if coordinatorRecords.w.Delivery != "sending" {
		t.Fatalf("the delivering host left the coordinator's record at %q", coordinatorRecords.w.Delivery)
	}
	coordinator.Tick(context.Background(), nil, nil)
	if coordinatorControl.sends != 0 {
		t.Fatal("the coordinator sent a wake the calling host had claimed")
	}
}
