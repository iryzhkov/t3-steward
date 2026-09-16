package backlog

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestEscalationOutboxEntryUsesConfiguredNotifyThread(t *testing.T) {
	now := supervisionTestTime()
	record := supervisionTestRecord()
	entry, ok := EscalationOutboxEntry(record, "thread-notify", "incident-1", "gate review timed out", []string{"event-1"}, now)
	if !ok {
		t.Fatal("a run with a configured notify thread must produce an escalation")
	}
	if err := entry.Validate(); err != nil {
		t.Fatalf("validate entry: %v", err)
	}
	if entry.ThreadID != "thread-notify" || entry.DedupeKey != "incident-1" {
		t.Fatalf("entry = %+v, want the configured thread and the incident as deduplication key", entry)
	}
	if entry.Delivery != SupervisionDeliveryPending {
		t.Fatalf("delivery = %q, want pending", entry.Delivery)
	}
	again, _ := EscalationOutboxEntry(record, "thread-notify", "incident-1", "other wording", nil, now.Add(time.Hour))
	if again.ID != entry.ID {
		t.Fatal("the escalation identity must be derived from the run and incident, not from the moment")
	}

	unconfigured := record
	unconfigured.Config.Escalation = domain.SupervisionEscalation{}
	if _, ok := EscalationOutboxEntry(unconfigured, "", "incident-1", "reason", nil, now); ok {
		t.Fatal("an unconfigured destination sends nothing; it never discovers a recipient")
	}
}

// A manifest that asks for a thread notification without naming a thread is the
// configuration the shipped example uses. It must bind to the thread the
// submitter asked to be woken, and when there is none it must say the
// escalation is undeliverable rather than drop it.
func TestEscalationBindsTheSubmitterThreadOrReportsItUndeliverable(t *testing.T) {
	now := supervisionTestTime()
	record := supervisionTestRecord()
	record.Config.Escalation = domain.SupervisionEscalation{NotifyThread: true}

	thread, undeliverable := SupervisionEscalationThread(record, "thread-submitter")
	if thread != "thread-submitter" || undeliverable != "" {
		t.Fatalf("thread = %q undeliverable = %q, want the submitter's notify thread", thread, undeliverable)
	}
	entry, ok := EscalationOutboxEntry(record, thread, "incident-1", "the overseer route is gone", nil, now)
	if !ok || entry.ThreadID != "thread-submitter" {
		t.Fatalf("entry = %+v ok = %v, want an escalation addressed to the submitter", entry, ok)
	}
	if err := entry.Validate(); err != nil {
		t.Fatalf("validate entry: %v", err)
	}

	// No manifest thread and no submitter thread: undeliverable, with a reason
	// an operator can read, and nothing sent.
	thread, undeliverable = SupervisionEscalationThread(record, "")
	if thread != "" || undeliverable != SupervisionUndeliverableEscalation {
		t.Fatalf("thread = %q undeliverable = %q, want the undeliverable explanation", thread, undeliverable)
	}
	if _, ok := EscalationOutboxEntry(record, thread, "incident-1", "reason", nil, now); ok {
		t.Fatal("an escalation with no destination must send nothing")
	}

	// The manifest's own thread still wins over the submitter's.
	declared := supervisionTestRecord()
	if thread, _ := SupervisionEscalationThread(declared, "thread-submitter"); thread != "thread-notify" {
		t.Fatalf("thread = %q, want the thread the manifest declared", thread)
	}
	// A run that asked for no notification at all is not undeliverable: it asked
	// for nothing.
	silent := supervisionTestRecord()
	silent.Config.Escalation = domain.SupervisionEscalation{}
	if thread, undeliverable := SupervisionEscalationThread(silent, "thread-submitter"); thread != "" || undeliverable != "" {
		t.Fatalf("thread = %q undeliverable = %q, want silence", thread, undeliverable)
	}
}

func TestAppendSupervisionOutboxDeduplicatesOnIncident(t *testing.T) {
	now := supervisionTestTime()
	record := supervisionTestRecord()
	first, _ := EscalationOutboxEntry(record, "thread-notify", "incident-1", "first", nil, now)
	entries, added := AppendSupervisionOutbox(nil, first)
	if !added || len(entries) != 1 {
		t.Fatalf("entries = %+v added = %v, want the first escalation stored", entries, added)
	}
	repeat, _ := EscalationOutboxEntry(record, "thread-notify", "incident-1", "again", nil, now.Add(time.Minute))
	entries, added = AppendSupervisionOutbox(entries, repeat)
	if added || len(entries) != 1 {
		t.Fatalf("entries = %+v added = %v, want no second notification for one incident", entries, added)
	}

	// A delivered escalation still suppresses a repeat of the same incident.
	delivered, changed, err := TransitionSupervisionDelivery(entries[0], SupervisionDeliverySending, now)
	if err != nil || !changed {
		t.Fatalf("send: changed = %v err = %v", changed, err)
	}
	delivered, _, err = TransitionSupervisionDelivery(delivered, SupervisionDeliveryDelivered, now)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	entries = []SupervisionOutboxEntry{delivered}
	if _, added = AppendSupervisionOutbox(entries, repeat); added {
		t.Fatal("a delivered escalation must not be re-sent for the same incident")
	}

	other, _ := EscalationOutboxEntry(record, "thread-notify", "incident-2", "different condition", nil, now)
	if entries, added = AppendSupervisionOutbox(entries, other); !added || len(entries) != 2 {
		t.Fatalf("entries = %+v added = %v, want a new incident to notify", entries, added)
	}
}

func TestSupervisionDeliveryTransitionsAreIdempotent(t *testing.T) {
	now := supervisionTestTime()
	entry, _ := EscalationOutboxEntry(supervisionTestRecord(), "thread-notify", "incident-1", "reason", nil, now)
	sending, changed, err := TransitionSupervisionDelivery(entry, SupervisionDeliverySending, now)
	if err != nil || !changed || sending.Attempts != 1 {
		t.Fatalf("sending = %+v changed = %v err = %v", sending, changed, err)
	}
	delivered, changed, err := TransitionSupervisionDelivery(sending, SupervisionDeliveryDelivered, now)
	if err != nil || !changed || delivered.DeliveredAt == nil {
		t.Fatalf("delivered = %+v changed = %v err = %v", delivered, changed, err)
	}
	repeat, changed, err := TransitionSupervisionDelivery(delivered, SupervisionDeliveryDelivered, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("a repeated acknowledgement must not be an error: %v", err)
	}
	if changed || !repeat.DeliveredAt.Equal(*delivered.DeliveredAt) {
		t.Fatal("a repeated acknowledgement must change nothing")
	}
	if _, _, err := TransitionSupervisionDelivery(delivered, SupervisionDeliverySending, now); err == nil {
		t.Fatal("nothing returns from delivered; absence of a reply never authorizes a second send")
	}
	if _, _, err := TransitionSupervisionDelivery(delivered, SupervisionDeliveryCancelled, now); err == nil {
		t.Fatal("a delivered escalation cannot be cancelled after the fact")
	}
}

func TestSupervisionDeliveryRecoveryRequiresPositiveObservation(t *testing.T) {
	now := supervisionTestTime()
	entry, _ := EscalationOutboxEntry(supervisionTestRecord(), "thread-notify", "incident-1", "reason", nil, now)
	sending, _, err := TransitionSupervisionDelivery(entry, SupervisionDeliverySending, now)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	ambiguous, changed, err := TransitionSupervisionDelivery(sending, SupervisionDeliveryRecoveryRequired, now)
	if err != nil || !changed {
		t.Fatalf("recovery-required: changed = %v err = %v", changed, err)
	}
	if _, _, err := TransitionSupervisionDelivery(ambiguous, SupervisionDeliverySending, now); err == nil {
		t.Fatal("an ambiguous send is resolved by observing the outcome, not by sending again")
	}
	if _, changed, err = TransitionSupervisionDelivery(ambiguous, SupervisionDeliveryDelivered, now); err != nil || !changed {
		t.Fatalf("positive observation must settle it: changed = %v err = %v", changed, err)
	}
}

func TestActivationWakeOutboxEntryIsOnePerDispatchIdentity(t *testing.T) {
	now := supervisionTestTime()
	activation := domain.Activation{
		ID: ActivationID("run-1", 1), RunID: "run-1", Epoch: 1,
		DispatchIdentity: ActivationDispatchIdentity("run-1", 1),
	}
	first, ok := ActivationWakeOutboxEntry(activation, "gate ready", []string{"event-1"}, now)
	if !ok {
		t.Fatal("an activation with a dispatch identity must produce a wake intent")
	}
	entries, added := AppendSupervisionOutbox(nil, first)
	if !added {
		t.Fatal("the first wake must be stored")
	}
	retry, _ := ActivationWakeOutboxEntry(activation, "gate ready", []string{"event-1"}, now.Add(time.Minute))
	if _, added = AppendSupervisionOutbox(entries, retry); added {
		t.Fatal("a retried dispatch of one activation is one wake intent, not two")
	}
}
