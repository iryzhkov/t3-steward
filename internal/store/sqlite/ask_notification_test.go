package sqlite

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

func askSinkNotify(t *testing.T, store *Store, id, run, host string, now time.Time) domain.NodeWait {
	t.Helper()
	w, err := store.RegisterNodeWait(context.Background(), domain.NodeWaitRequest{
		ID: id, ThreadID: "submitter-" + host, Target: domain.NodeRef{RunID: run, TaskID: domain.SinkTaskName}, Timeout: 24 * time.Hour,
	}, "actor-"+host, host, now)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func askEvent(t *testing.T, store *Store, askID string) domain.NodeWait {
	t.Helper()
	waits, err := store.ListNodeWaits(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var events []domain.NodeWait
	for _, w := range waits {
		if strings.HasPrefix(w.Request.ID, askNotificationPrefix) && w.Request.Name == "ask "+askID+" registered" {
			events = append(events, w)
		}
	}
	if len(events) != 1 {
		t.Fatalf("ask %s events: %+v", askID, events)
	}
	return events[0]
}

type askDeliveryControl struct {
	fleetControl
	offline bool
	receipt wait.WakeReceiptStatus
}

func (c *askDeliveryControl) GetThread(ctx context.Context, id string) (*domain.Thread, error) {
	if c.offline {
		return nil, errors.New("offline")
	}
	return c.fleetControl.GetThread(ctx, id)
}

func (c *askDeliveryControl) ReconcileNodeWake(context.Context, string, string) (wait.WakeReceiptStatus, error) {
	return c.receipt, nil
}

func TestAskNotificationSubmitterDelivery(t *testing.T) {
	for _, approver := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "approver"}[approver], func(t *testing.T) {
			ctx := context.Background()
			store, attempt, now := taskWaitFixture(t)
			original := askSinkNotify(t, store, "notify", attempt.WorkflowRunID, "A", now)
			question := domain.AskRequest{Question: "Full question: choose\nwith all details.", Options: []string{"it's $HOME; $(echo nope)", "second & | choice"}, Multi: true, OnDeadline: domain.AskDeadlineFail}
			if approver {
				question.Requires = domain.AskRequiresApprover
			}
			registration := askRegistration(attempt, "ask-delivery", question, 4*time.Hour)
			ask, err := store.RegisterTaskWait(ctx, registration, now)
			if err != nil {
				t.Fatal(err)
			}
			event := askEvent(t, store, ask.ID)
			if event.Host != "A" || event.Actor != original.Actor || event.Request.ThreadID != original.Request.ThreadID ||
				event.Request.Target.TaskID != attempt.TaskID || event.Request.Group != "" || event.SettledAt == nil || event.Delivery != "pending" {
				t.Fatalf("event: %+v", event)
			}
			if parked := loadAttempt(t, store, attempt.ID); parked.Progress != domain.ProgressWaitingExternal {
				t.Fatal(parked)
			}
			waits, _ := store.ListNodeWaits(ctx)
			for _, w := range waits {
				if w.Request.ID == original.Request.ID && w.SettledAt != nil {
					t.Fatal("sink notification settled on ask")
				}
			}
			payload := event.DeliveryPayload
			for _, want := range []string{question.Question, "run r awaits an answer", "Task: t", "Ask: " + ask.ID,
				"2026-09-12 16:00:00 UTC", "Multi:", "repeating --option", "'it'\\''s $HOME; $(echo nope)'", "'second & | choice'"} {
				if !strings.Contains(payload, want) {
					t.Fatalf("missing %q in %s", want, payload)
				}
			}
			if strings.Index(payload, "'it'") > strings.Index(payload, "'second") {
				t.Fatal("options reordered")
			}
			if approver && (!strings.Contains(payload, "t3-steward --config <approver-config-path> ask answer") ||
				!strings.Contains(payload, "Replace <approver-config-path>")) {
				t.Fatal(payload)
			}
			if strings.Contains(payload, "OPTION") || strings.Contains(payload, "resume") || strings.Contains(payload, "succeeded") {
				t.Fatal(payload)
			}
			if _, err := store.RegisterTaskWait(ctx, registration, now.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			if got := askEvent(t, store, ask.ID); !reflect.DeepEqual(got, event) {
				t.Fatal("replay changed event")
			}
			reopened, err := Open(store.path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if got := askEvent(t, reopened, ask.ID); !reflect.DeepEqual(got, event) {
				t.Fatal("reopen changed event")
			}
			if _, err := reopened.RegisterTaskWait(ctx, registration, now.Add(2*time.Minute)); err != nil {
				t.Fatal(err)
			}

			clock := now.Add(time.Minute)
			control := &askDeliveryControl{fleetControl: fleetControl{threads: map[string]*domain.Thread{
				original.Request.ThreadID: {ID: original.Request.ThreadID}, attempt.ThreadID: {ID: attempt.ThreadID},
			}}, offline: true, receipt: wait.WakeReceiptUnknown}
			runner := wait.New(reopened, control, nil)
			runner.SetClock(func() time.Time { return clock })
			runner.NodeHost = "B"
			runner.NodeCoalesceWindow = time.Hour
			runner.Tick(ctx, nil, nil)
			if len(control.sends) != 0 || askEvent(t, reopened, ask.ID).Delivery != "pending" {
				t.Fatal("worker B delivered submitter event")
			}
			runner.NodeHost = "A"
			runner.Tick(ctx, nil, nil)
			if askEvent(t, reopened, ask.ID).Delivery != "offline" {
				t.Fatal("not offline")
			}
			control.offline = false
			clock = clock.Add(time.Minute)
			runner.Tick(ctx, nil, []domain.BucketState{{Phase: domain.PhaseStopped}})
			if askEvent(t, reopened, ask.ID).Delivery != "busy" || len(control.sends) != 0 {
				t.Fatal("not held busy")
			}
			clock = clock.Add(time.Minute)
			runner.Tick(ctx, nil, nil)
			if len(control.sends) != 1 || control.sends[0] != event.DeliveryID || control.texts[0] != payload ||
				control.resumed[0] != original.Request.ThreadID {
				t.Fatalf("send: %+v", control)
			}
			// Unknown receipt must reconcile, never authorize another send.
			for i := 0; i < 3; i++ {
				runner.Tick(ctx, nil, nil)
			}
			if len(control.sends) != 1 || askEvent(t, reopened, ask.ID).Delivery != "recovery-required" {
				t.Fatal("unknown receipt duplicated event")
			}
			control.receipt = wait.WakeReceiptDelivered
			runner.Tick(ctx, nil, nil)
			if askEvent(t, reopened, ask.ID).Delivery != "delivered" {
				t.Fatal("receipt not reconciled")
			}
			var pins int
			if err := reopened.db.QueryRow("SELECT count(*) FROM coordinator_retention_pins WHERE owner=?", "wait:"+event.Request.ID).Scan(&pins); err != nil || pins != 0 {
				t.Fatalf("pin %d: %v", pins, err)
			}
			if parked := loadAttempt(t, reopened, attempt.ID); parked.Progress != domain.ProgressWaitingExternal {
				t.Fatal("submitter wake resumed asking attempt")
			}

			// Both phases use the original host without an owner-channel sink.
			clock = now.Add(2 * time.Hour)
			runner.NodeHost = "B"
			runner.Tick(ctx, nil, nil)
			reminder := askPhaseEvent(t, reopened, ask.ID, "midpoint")
			if reminder.Delivery != "pending" || reminder.Host != "A" {
				t.Fatal(reminder)
			}
			if reminder.DeliveryPayload != "Reminder: this ask remains open at half its deadline.\n"+payload {
				t.Fatal(reminder.DeliveryPayload)
			}
			runner.NodeHost = "A"
			runner.Tick(ctx, nil, nil)
			runner.Tick(ctx, nil, nil)
			if got := askPhaseEvent(t, reopened, ask.ID, "midpoint"); got.Delivery != "delivered" {
				t.Fatal(got)
			}
			found := false
			for i, id := range control.sends {
				if id == reminder.DeliveryID && control.texts[i] == reminder.DeliveryPayload && control.resumed[i] == original.Request.ThreadID {
					found = true
				}
			}
			if !found {
				t.Fatal("midpoint did not reach original submitter")
			}

			// The terminal notification remains separate and delivers after completion.
			if _, err := reopened.AnswerAsk(ctx, domain.AskAnswer{AskID: ask.ID, Options: []string{question.Options[0]}, Source: domain.AskSourceCLI}, "operator", approver, clock); err != nil {
				t.Fatal(err)
			}
			records, err := reopened.LoadCoordinatorRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			records.Attempts[0].Progress = domain.ProgressSucceeded
			records.Attempts[0].Control = domain.ControlStopped
			records.WorkflowRuns[0].Sink.Progress = domain.ProgressSucceeded
			if err := reopened.SaveCoordinatorRecords(ctx, records); err != nil {
				t.Fatal(err)
			}
			runner.NodeCoalesceWindow = 0
			clock = clock.Add(time.Minute)
			runner.Tick(ctx, nil, nil)
			runner.Tick(ctx, nil, nil)
			if len(control.sends) != 3 || control.sends[2] != original.DeliveryID {
				t.Fatalf("terminal sends: %v", control.sends)
			}
		})
	}
}

func TestAskNotificationRequiresExplicitRunRecipient(t *testing.T) {
	for _, mode := range []string{"none", "unrelated", "non-ask"} {
		t.Run(mode, func(t *testing.T) {
			store, attempt, now := taskWaitFixture(t)
			if mode == "unrelated" {
				otherRun(t, store, now, domain.ProgressActive)
				askSinkNotify(t, store, "unrelated", "r2", "A", now)
			}
			registration := askRegistration(attempt, "ask-no-route", pickAsk(), time.Hour)
			if mode == "non-ask" {
				askSinkNotify(t, store, "notify", "r", "A", now)
				registration = taskWaitRegistration(attempt, "shell", domain.WakeEach)
			}
			if _, err := store.RegisterTaskWait(context.Background(), registration, now); err != nil {
				t.Fatal(err)
			}
			if err := store.SettleNodeWaits(context.Background(), now.Add(45*time.Minute)); err != nil {
				t.Fatal(err)
			}
			waits, err := store.ListNodeWaits(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range waits {
				if strings.HasPrefix(w.Request.ID, askNotificationPrefix) {
					t.Fatal("guessed recipient", w)
				}
			}
			if loadAttempt(t, store, attempt.ID).Progress != domain.ProgressWaitingExternal {
				t.Fatal("registration semantics changed")
			}
		})
	}
}

func TestAskNotificationRecipientOrderingAndSettledFallback(t *testing.T) {
	for _, live := range []bool{true, false} {
		t.Run(map[bool]string{true: "prefer live", false: "settled fallback"}[live], func(t *testing.T) {
			ctx := context.Background()
			store, attempt, now := taskWaitFixture(t)
			a := askSinkNotify(t, store, "a", "r", "old", now)
			b := askSinkNotify(t, store, "b", "r", "new", now)
			askSinkNotify(t, store, "c", "r", "later", now)
			tx, err := store.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			a.SettledAt = &now
			if err := saveNodeWaitTx(ctx, tx, a); err != nil {
				t.Fatal(err)
			}
			if !live {
				b.SettledAt = &now
				if err := saveNodeWaitTx(ctx, tx, b); err != nil {
					t.Fatal(err)
				}
				var raw []byte
				if err := tx.QueryRowContext(ctx, "SELECT record FROM coordinator_node_waits WHERE id='c'").Scan(&raw); err != nil {
					t.Fatal(err)
				}
				// Settling c through a delivery transition below is unnecessary: remove
				// it from candidate selection to exercise the all-settled fallback.
				if _, err := tx.ExecContext(ctx, "UPDATE coordinator_node_waits SET record=json_set(record,'$.settledAt',?) WHERE id='c'", now.Format(time.RFC3339)); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			ask, err := store.RegisterTaskWait(ctx, askRegistration(attempt, "ordering", pickAsk(), time.Hour), now)
			if err != nil {
				t.Fatal(err)
			}
			expected := b
			if !live {
				expected = a
			}
			event := askEvent(t, store, ask.ID)
			if event.Host != expected.Host || event.Actor != expected.Actor || event.Request.ThreadID != expected.Request.ThreadID {
				t.Fatalf("recipient: %+v", event)
			}
		})
	}
}
