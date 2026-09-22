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

type receiptControl struct {
	nativeControl
	thread  *domain.Thread
	getErr  error
	receipt WakeReceiptStatus
}

func (c *receiptControl) GetThread(context.Context, string) (*domain.Thread, error) {
	return c.thread, c.getErr
}

func (c *receiptControl) ReconcileNodeWake(context.Context, string, string) (WakeReceiptStatus, error) {
	return c.receipt, nil
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

func TestNodeDeliveryOfflineBusyRecoveryAndReceiptOutcomes(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	newStore := func(id string) *nativeMemory {
		return &nativeMemory{w: domain.NodeWait{
			Request: domain.NodeWaitRequest{ID: id, ThreadID: "thread"},
			Host:    "host", SettledAt: &now, Observation: &domain.NodeObservation{ExitCode: 0},
			DeliveryID: "token-" + id, Delivery: "pending",
		}}
	}

	t.Run("offline then online", func(t *testing.T) {
		store := newStore("offline")
		control := &receiptControl{getErr: errors.New("offline"), receipt: WakeReceiptUnknown}
		runner := New(store, control, nil)
		runner.NodeHost = "host"
		runner.DisableQuotaChecks = true
		runner.Tick(ctx, nil, nil)
		if store.w.Delivery != "offline" || control.sends != 0 {
			t.Fatal(store.w.Delivery, control.sends)
		}
		control.getErr = nil
		control.thread = &domain.Thread{ID: "thread"}
		runner.Tick(ctx, nil, nil)
		if store.w.Delivery != "sending" || control.sends != 1 {
			t.Fatal(store.w.Delivery, control.sends)
		}
		control.receipt = WakeReceiptUnknown
		runner.Tick(ctx, nil, nil)
		if store.w.Delivery != "recovery-required" || control.sends != 1 {
			t.Fatal(store.w.Delivery, control.sends)
		}
		control.receipt = WakeReceiptDelivered
		runner.Tick(ctx, nil, nil)
		if store.w.Delivery != "delivered" || control.sends != 1 {
			t.Fatal(store.w.Delivery, control.sends)
		}
	})

	t.Run("busy then idle", func(t *testing.T) {
		store := newStore("busy")
		control := &receiptControl{thread: &domain.Thread{ID: "thread"}}
		runner := New(store, control, nil)
		runner.NodeHost = "host"
		buckets := []domain.BucketState{{Phase: domain.PhaseStopped}}
		runner.Tick(ctx, nil, buckets)
		if store.w.Delivery != "busy" || control.sends != 0 {
			t.Fatal(store.w.Delivery, control.sends)
		}
		runner.Tick(ctx, nil, nil)
		if store.w.Delivery != "sending" || control.sends != 1 {
			t.Fatal(store.w.Delivery, control.sends)
		}
	})

	t.Run("authoritative no effect retries but rejection settles", func(t *testing.T) {
		store := newStore("receipt")
		store.w.Delivery = "sending"
		control := &receiptControl{thread: &domain.Thread{ID: "thread"}, receipt: WakeReceiptKnownNoEffect}
		runner := New(store, control, nil)
		runner.NodeHost = "host"
		runner.DisableQuotaChecks = true
		runner.Tick(ctx, nil, nil)
		if store.w.Delivery != "offline" || control.sends != 0 {
			t.Fatal(store.w.Delivery, control.sends)
		}
		runner.Tick(ctx, nil, nil)
		if store.w.Delivery != "sending" || control.sends != 1 {
			t.Fatal(store.w.Delivery, control.sends)
		}
		control.receipt = WakeReceiptRejected
		runner.Tick(ctx, nil, nil)
		if store.w.Delivery != "rejected" || control.sends != 1 {
			t.Fatal(store.w.Delivery, control.sends)
		}
	})
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

// scopedNativeMemory is a node-wait store that can answer for one host, as the
// store over the admin transport does. It records what it was asked for.
type scopedNativeMemory struct {
	*nativeMemory
	asked []string
}

func (s *scopedNativeMemory) ListNodeWaitsForHost(_ context.Context, host string) ([]domain.NodeWait, error) {
	s.asked = append(s.asked, host)
	if s.w.Host != host {
		return nil, nil
	}
	return []domain.NodeWait{s.w}, nil
}

// A store that can narrow the list is asked to. The runner is the only party
// that knows which waits it could act on, and on a store that reaches the
// coordinator over the admin transport the difference is the coordinator's whole
// node-wait history, carried once per tick.
func TestTheRunnerAsksAScopedStoreForItsOwnHostsWaits(t *testing.T) {
	now := time.Now()
	records := &scopedNativeMemory{nativeMemory: &nativeMemory{w: domain.NodeWait{
		Request:     domain.NodeWaitRequest{ID: "nw-caller", ThreadID: "thread", Name: "run-1/__sink"},
		Host:        "caller",
		SettledAt:   &now,
		Observation: &domain.NodeObservation{ExitCode: 0, Reason: "succeeded"},
		DeliveryID:  "token",
		Delivery:    "pending",
	}}}
	control := &nativeControl{}
	runner := New(localOnlyMemory{}, control, nil)
	runner.NodeStore = records
	runner.NodeHost = "caller"
	runner.Tick(context.Background(), nil, nil)
	if len(records.asked) != 1 || records.asked[0] != "caller" {
		t.Fatalf("the runner asked the store for %v, want one list scoped to caller", records.asked)
	}
	if control.sends != 1 {
		t.Fatalf("the scoped list delivered %d wakes, want exactly one", control.sends)
	}
}

// refusingNativeMemory answers a delivery transition the way a coordinator that
// does not know the action does: with a plain error, over the network.
type refusingNativeMemory struct{ *nativeMemory }

func (refusingNativeMemory) TransitionNodeWake(context.Context, string, string, string, time.Time) (bool, error) {
	return false, errors.New("unknown native wait action")
}

// A wake that cannot be claimed is a wake that never arrives, and the symptom
// -- delivery=pending forever -- is identical to the defect this path exists to
// fix. Now that the claim crosses the network on every host that is not the
// coordinator, its refusal has to leave a line naming the wait, the transition
// and the host, instead of being swallowed by the loop.
func TestARefusedNodeWakeClaimIsReported(t *testing.T) {
	now := time.Now()
	records := refusingNativeMemory{&nativeMemory{w: domain.NodeWait{
		Request:     domain.NodeWaitRequest{ID: "nw-1", ThreadID: "thread", Name: "run-1/__sink"},
		Host:        "here",
		SettledAt:   &now,
		Observation: &domain.NodeObservation{ExitCode: 0, Reason: "succeeded"},
		DeliveryID:  "token",
		Delivery:    "pending",
	}}}
	handler := &recordingHandler{}
	control := &nativeControl{}
	runner := New(records, control, slog.New(handler))
	runner.NodeHost = "here"
	runner.Tick(context.Background(), nil, nil)
	if control.sends != 0 {
		t.Fatalf("a wake was sent without being claimed (%d sends)", control.sends)
	}
	var reported int
	for _, record := range handler.records {
		if record.Level != slog.LevelError || record.Message != "move a node wake through its delivery states" {
			continue
		}
		reported++
		attributes := map[string]string{}
		record.Attrs(func(a slog.Attr) bool {
			attributes[a.Key] = a.Value.String()
			return true
		})
		for key, want := range map[string]string{
			"wait": "nw-1", "from": "pending", "to": "sending", "host": "here",
			"error": "unknown native wait action",
		} {
			if attributes[key] != want {
				t.Fatalf("the report says %s=%q, want %q (%v)", key, attributes[key], want, attributes)
			}
		}
	}
	if reported != 1 {
		t.Fatalf("a refused claim was reported %d times, want once", reported)
	}
}

// nodeWakeTransitionReports collects the delivery transitions a runner reported
// as refused, so a test can say which transition was named rather than only
// that something was logged.
func nodeWakeTransitionReports(handler *recordingHandler) []map[string]string {
	var reports []map[string]string
	for _, record := range handler.records {
		if record.Level != slog.LevelError || record.Message != "move a node wake through its delivery states" {
			continue
		}
		attributes := map[string]string{}
		record.Attrs(func(a slog.Attr) bool {
			attributes[a.Key] = a.Value.String()
			return true
		})
		reports = append(reports, attributes)
	}
	return reports
}

// refusingMemberMemory refuses one wait's transition into one state and answers
// every other transition normally, which is how a rolled-back coordinator or a
// transport that failed mid-tick refuses exactly one call.
type refusingMemberMemory struct {
	*nativeGroupMemory
	id, to string
}

func (s *refusingMemberMemory) TransitionNodeWake(ctx context.Context, id, from, to string, at time.Time) (bool, error) {
	if id == s.id && to == s.to {
		return false, errors.New("unknown native wait action")
	}
	return s.nativeGroupMemory.TransitionNodeWake(ctx, id, from, to, at)
}

// The members of a --wake all group ride in the earliest member's message and
// are then moved through the same two transitions the sender is, over the same
// network on every host that is not the coordinator. A refusal there is milder
// than a lost wake -- nodeWaitGroups lets the earliest still-pending member
// carry one more send on a later tick -- but the group is then woken twice, and
// a duplicate wake with nothing in the log to explain it is the kind of mystery
// these lines exist to end.
func TestARefusedGroupMemberTransitionIsReported(t *testing.T) {
	now := time.Now()
	member := func(id string, created time.Time) domain.NodeWait {
		return domain.NodeWait{
			Request:     domain.NodeWaitRequest{ID: id, ThreadID: "thread", Group: "pair", Wake: domain.WakeAll, Target: domain.NodeRef{RunID: "r", TaskID: id}},
			Host:        "here",
			CreatedAt:   created,
			SettledAt:   &now,
			Observation: &domain.NodeObservation{ExitCode: 0, Outcome: domain.TaskWaitMet},
			DeliveryID:  "token-" + id,
			Delivery:    "pending",
		}
	}
	for _, tc := range []struct {
		name, refuse, from string
	}{
		{"the rider cannot be claimed", "sending", "pending"},
		{"the rider cannot be marked delivered", "delivered", "sending"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &refusingMemberMemory{nativeGroupMemory: &nativeGroupMemory{waits: []domain.NodeWait{
				member("nw-first", now.Add(-time.Minute)),
				member("nw-second", now),
			}}, id: "nw-second", to: tc.refuse}
			handler := &recordingHandler{}
			control := &nativeControl{}
			runner := New(store, control, slog.New(handler))
			runner.NodeHost = "here"
			runner.Tick(context.Background(), nil, nil)
			if control.sends != 1 {
				t.Fatalf("the group was sent %d times, want once", control.sends)
			}
			reports := nodeWakeTransitionReports(handler)
			if len(reports) != 1 {
				t.Fatalf("a refused group member transition was reported %d times, want once (%v)", len(reports), reports)
			}
			for key, want := range map[string]string{
				"wait": "nw-second", "from": tc.from, "to": tc.refuse, "host": "here",
				"error": "unknown native wait action",
			} {
				if reports[0][key] != want {
					t.Fatalf("the report says %s=%q, want %q (%v)", key, reports[0][key], want, reports[0])
				}
			}
		})
	}
}

// A dry run holds a wake rather than sending it, and the hold is a transition
// on the same store, over the same network. A refusal leaves the wait pending
// while the operator reads "held" into it, so it is reported like the rest.
func TestARefusedDryRunHoldIsReported(t *testing.T) {
	now := time.Now()
	store := refusingNativeMemory{&nativeMemory{w: domain.NodeWait{
		Request:     domain.NodeWaitRequest{ID: "nw-1", ThreadID: "thread", Name: "run-1/__sink"},
		Host:        "here",
		SettledAt:   &now,
		Observation: &domain.NodeObservation{ExitCode: 0, Reason: "succeeded"},
		DeliveryID:  "token",
		Delivery:    "pending",
	}}}
	handler := &recordingHandler{}
	control := &nativeControl{}
	runner := New(store, control, slog.New(handler))
	runner.NodeHost = "here"
	runner.NodeDryRun = true
	runner.Tick(context.Background(), nil, nil)
	if control.sends != 0 {
		t.Fatalf("a dry run sent %d wakes", control.sends)
	}
	reports := nodeWakeTransitionReports(handler)
	if len(reports) != 1 {
		t.Fatalf("a refused hold was reported %d times, want once (%v)", len(reports), reports)
	}
	for key, want := range map[string]string{
		"wait": "nw-1", "from": "pending", "to": "held", "host": "here",
		"error": "unknown native wait action",
	} {
		if reports[0][key] != want {
			t.Fatalf("the report says %s=%q, want %q (%v)", key, reports[0][key], want, reports[0])
		}
	}
}

// The runner records that it is delivering node wakes for its host, so that a
// command on this host can establish that a daemon is here to deliver them. It
// records nothing in a dry run, where a wake is held rather than sent, because
// a promise kept by holding the message is not kept.
func TestTheRunnerRecordsThatItDeliversNodeWakesForItsHost(t *testing.T) {
	now := time.Now()
	records := func() *nativeMemory {
		return &nativeMemory{w: domain.NodeWait{
			Request:     domain.NodeWaitRequest{ID: "nw-1", ThreadID: "thread", Name: "run-1/__sink"},
			Host:        "here",
			SettledAt:   &now,
			Observation: &domain.NodeObservation{ExitCode: 0, Reason: "succeeded"},
			DeliveryID:  "token",
			Delivery:    "pending",
		}}
	}
	var recorded []string
	runner := New(records(), &nativeControl{}, nil)
	runner.NodeHost = "here"
	runner.NodeDelivery = func(_ context.Context, host string) { recorded = append(recorded, host) }
	runner.Tick(context.Background(), nil, nil)
	if len(recorded) != 1 || recorded[0] != "here" {
		t.Fatalf("the runner recorded delivery for %v, want one record for here", recorded)
	}

	recorded = nil
	dry := New(records(), &nativeControl{}, nil)
	dry.NodeHost = "here"
	dry.NodeDryRun = true
	dry.NodeDelivery = func(_ context.Context, host string) { recorded = append(recorded, host) }
	dry.Tick(context.Background(), nil, nil)
	if len(recorded) != 0 {
		t.Fatalf("a dry run recorded delivery for %v", recorded)
	}
}

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
