package backlog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Supervision outbox: one durable wake or escalation intent per reason to talk
// to somebody, reconciled through a stable external-effect identity.
//
// This mirrors the node-wait delivery machinery (`coordinator_node_waits` plus
// `Store.TransitionNodeWake`) rather than inventing a second one. The node-wait
// ADR already paid for the hard parts: a delivery state that is owned by one
// transition at a time, positive observation before a second send, and a
// terminal state a lost reply cannot undo. Escalation reuses the configured
// notify thread and adds no messaging integration and no recipient discovery.
//
// Deduplication is on the incident ID, so re-escalating the same incident does
// not notify twice, while a genuinely new incident always does.

// SupervisionOutboxKind separates the two things the coordinator sends.
type SupervisionOutboxKind string

const (
	// SupervisionOutboxWake wakes an overseer activation.
	SupervisionOutboxWake SupervisionOutboxKind = "activation-wake"
	// SupervisionOutboxEscalation notifies a human on the configured thread.
	SupervisionOutboxEscalation SupervisionOutboxKind = "escalation"
)

// SupervisionDelivery is the delivery state of one outbox entry. The values and
// the legal moves between them are the node-wait wake states, deliberately
// spelled the same way so an operator reading two tables reads one vocabulary.
type SupervisionDelivery string

const (
	SupervisionDeliveryPending          SupervisionDelivery = "pending"
	SupervisionDeliveryHeld             SupervisionDelivery = "held"
	SupervisionDeliveryOffline          SupervisionDelivery = "offline"
	SupervisionDeliveryBusy             SupervisionDelivery = "busy"
	SupervisionDeliverySending          SupervisionDelivery = "sending"
	SupervisionDeliveryDelivered        SupervisionDelivery = "delivered"
	SupervisionDeliveryRecoveryRequired SupervisionDelivery = "recovery-required"
	SupervisionDeliveryRejected         SupervisionDelivery = "rejected"
	SupervisionDeliveryCancelled        SupervisionDelivery = "cancelled"
)

// SupervisionOutboxEntry is one durable delivery intent.
type SupervisionOutboxEntry struct {
	// ID is deterministic in the thing that caused the entry, so a retry of the
	// same cause writes the same row instead of a second one.
	ID    string                `json:"id"`
	RunID string                `json:"runId"`
	Kind  SupervisionOutboxKind `json:"kind"`
	// DedupeKey is the incident ID for an escalation and the activation dispatch
	// identity for a wake. Two entries with the same key are the same intent.
	DedupeKey string `json:"dedupeKey"`
	// IncidentID, ActivationID and Epoch identify what is being reported.
	IncidentID   string `json:"incidentId,omitempty"`
	ActivationID string `json:"activationId,omitempty"`
	Epoch        int64  `json:"epoch,omitempty"`
	// ThreadID is the configured notify thread for an escalation. A wake has
	// none: it is delivered by dispatching the activation as assigned work.
	ThreadID string `json:"threadId,omitempty"`
	// Reason is the normalized, human-readable cause. EventIDs preserves every
	// evidence reference the coalescer folded together.
	Reason         string              `json:"reason,omitempty"`
	EventIDs       []string            `json:"eventIds,omitempty"`
	Delivery       SupervisionDelivery `json:"delivery"`
	Attempts       int                 `json:"attempts,omitempty"`
	Payload        string              `json:"payload,omitempty"`
	PayloadDigest  string              `json:"payloadDigest,omitempty"`
	LastError      string              `json:"lastError,omitempty"`
	NextAction     string              `json:"nextAction,omitempty"`
	NextEligibleAt *time.Time          `json:"nextEligibleAt,omitempty"`
	CreatedAt      time.Time           `json:"createdAt"`
	DeliveredAt    *time.Time          `json:"deliveredAt,omitempty"`
}

// Validate rejects an entry that cannot be delivered or deduplicated.
func (e SupervisionOutboxEntry) Validate() error {
	switch {
	case strings.TrimSpace(e.ID) == "":
		return errors.New("supervision outbox entry requires an id")
	case strings.TrimSpace(e.RunID) == "":
		return errors.New("supervision outbox entry requires a run id")
	case e.Kind != SupervisionOutboxWake && e.Kind != SupervisionOutboxEscalation:
		return fmt.Errorf("supervision outbox entry has unknown kind %q", e.Kind)
	case strings.TrimSpace(e.DedupeKey) == "":
		return errors.New("supervision outbox entry requires a deduplication key")
	case e.Kind == SupervisionOutboxEscalation && strings.TrimSpace(e.ThreadID) == "":
		return errors.New("supervision escalation requires the configured notify thread")
	case e.Delivery == "":
		return errors.New("supervision outbox entry requires a delivery state")
	case e.CreatedAt.IsZero():
		return errors.New("supervision outbox entry requires a creation time")
	}
	return nil
}

// SupervisionUndeliverableEscalation is the explanation recorded on an incident
// whose escalation asked for a notification that has no destination. It is
// phrased for the operator who reads it in explain or status.
const SupervisionUndeliverableEscalation = "escalation is undeliverable: the manifest asks for a thread notification, " +
	"the supervision block names no thread_id and this run was submitted without --notify-thread"

// SupervisionEscalationThread resolves where an escalation for this run is
// delivered, and explains it when there is nowhere to deliver it.
//
// The manifest's own escalation.thread_id wins, because it is the destination
// the campaign author chose. A manifest that asks for a notification without
// naming a thread binds to the thread the submitter asked to be woken, which is
// the one recipient the coordinator already knows and is allowed to use; the
// plan forbids discovering any other. When neither exists the escalation is
// undeliverable, and the returned reason says so rather than letting it be
// dropped silently.
func SupervisionEscalationThread(record domain.SupervisionRecord, submitterThreadID string) (string, string) {
	escalation := record.Config.Escalation
	if !escalation.NotifyThread {
		return "", ""
	}
	if thread := strings.TrimSpace(escalation.ThreadID); thread != "" {
		return thread, ""
	}
	if thread := strings.TrimSpace(submitterThreadID); thread != "" {
		return thread, ""
	}
	return "", SupervisionUndeliverableEscalation
}

// EscalationOutboxEntry builds the escalation delivery intent for one incident,
// addressed to the thread SupervisionEscalationThread resolved.
//
// It reports false when there is no destination. That is not an error: an
// undeliverable escalation is recorded on its incident and visible in status,
// and nothing is sent. The plan forbids discovering a recipient instead.
func EscalationOutboxEntry(
	record domain.SupervisionRecord,
	threadID string,
	incidentID, reason string,
	eventIDs []string,
	now time.Time,
) (SupervisionOutboxEntry, bool) {
	incidentID = strings.TrimSpace(incidentID)
	threadID = strings.TrimSpace(threadID)
	if incidentID == "" || !record.Config.Escalation.NotifyThread || threadID == "" {
		return SupervisionOutboxEntry{}, false
	}
	return SupervisionOutboxEntry{
		ID:         stableCoordinatorID("supervision-escalation", record.RunID+"\x00"+incidentID),
		RunID:      record.RunID,
		Kind:       SupervisionOutboxEscalation,
		DedupeKey:  incidentID,
		IncidentID: incidentID,
		Epoch:      record.ActivationEpoch,
		ThreadID:   threadID,
		Reason:     reason,
		EventIDs:   append([]string(nil), eventIDs...),
		Delivery:   SupervisionDeliveryPending,
		CreatedAt:  now.UTC(),
	}, true
}

// ActivationWakeOutboxEntry builds the wake intent for one activation dispatch.
// Its deduplication key is the deterministic dispatch identity, so a retried
// dispatch of the same activation is one intent, not two.
func ActivationWakeOutboxEntry(activation domain.Activation, reason string, eventIDs []string, now time.Time) (SupervisionOutboxEntry, bool) {
	if strings.TrimSpace(activation.DispatchIdentity) == "" || strings.TrimSpace(activation.RunID) == "" {
		return SupervisionOutboxEntry{}, false
	}
	return SupervisionOutboxEntry{
		ID:           stableCoordinatorID("supervision-wake", activation.DispatchIdentity),
		RunID:        activation.RunID,
		Kind:         SupervisionOutboxWake,
		DedupeKey:    activation.DispatchIdentity,
		ActivationID: activation.ID,
		Epoch:        activation.Epoch,
		Reason:       reason,
		EventIDs:     append([]string(nil), eventIDs...),
		Delivery:     SupervisionDeliveryPending,
		CreatedAt:    now.UTC(),
	}, true
}

// AppendSupervisionOutbox adds an entry unless an entry with the same
// deduplication key is already live. Pending, held, retryable, ambiguous,
// sending, or delivered rows all suppress a fresh intent; only a terminal
// cancelled or rejected intent permits a separately identified replacement.
//
// It returns the new slice and whether the entry was added.
func AppendSupervisionOutbox(existing []SupervisionOutboxEntry, entry SupervisionOutboxEntry) ([]SupervisionOutboxEntry, bool) {
	for _, current := range existing {
		if current.RunID != entry.RunID || current.Kind != entry.Kind || current.DedupeKey != entry.DedupeKey {
			continue
		}
		switch current.Delivery {
		case SupervisionDeliveryPending, SupervisionDeliveryHeld, SupervisionDeliveryOffline,
			SupervisionDeliveryBusy, SupervisionDeliverySending, SupervisionDeliveryRecoveryRequired,
			SupervisionDeliveryDelivered:
			return existing, false
		}
	}
	return append(append([]SupervisionOutboxEntry(nil), existing...), entry), true
}

// TransitionSupervisionDelivery moves one entry's delivery ownership.
//
// A repeated delivery of an entry already in the target state is idempotent: it
// reports no change and no error, because the same authoritative receipt can
// arrive more than once and the second one must not be read as a second send. An entry in some other state reports no change either, so a
// caller that lost a race retries from what it reads rather than overwriting.
// Only a move the machine has no row for is an error.
func TransitionSupervisionDelivery(
	entry SupervisionOutboxEntry,
	to SupervisionDelivery,
	now time.Time,
) (SupervisionOutboxEntry, bool, error) {
	from := entry.Delivery
	if from == to {
		return entry, false, nil
	}
	if !supervisionDeliveryAllowed(from, to) {
		return entry, false, fmt.Errorf("invalid supervision delivery transition %s to %s", from, to)
	}
	entry.Delivery = to
	if to == SupervisionDeliverySending {
		entry.Attempts++
	}
	if to == SupervisionDeliveryDelivered {
		delivered := now.UTC()
		entry.DeliveredAt = &delivered
	}
	return entry, true, nil
}

// supervisionDeliveryAllowed is the delivery table. Once sending is durable, a
// lost reply requires positive observation: absence never authorizes a second
// send, and nothing returns from delivered.
func supervisionDeliveryAllowed(from, to SupervisionDelivery) bool {
	switch from {
	case SupervisionDeliveryPending:
		return to == SupervisionDeliveryHeld || to == SupervisionDeliveryOffline || to == SupervisionDeliveryBusy || to == SupervisionDeliverySending || to == SupervisionDeliveryRejected || to == SupervisionDeliveryCancelled
	case SupervisionDeliveryHeld, SupervisionDeliveryOffline, SupervisionDeliveryBusy:
		return to == SupervisionDeliveryOffline || to == SupervisionDeliveryBusy || to == SupervisionDeliverySending || to == SupervisionDeliveryRejected || to == SupervisionDeliveryCancelled
	case SupervisionDeliverySending:
		return to == SupervisionDeliveryDelivered || to == SupervisionDeliveryRecoveryRequired || to == SupervisionDeliveryRejected
	case SupervisionDeliveryRecoveryRequired:
		return to == SupervisionDeliveryDelivered || to == SupervisionDeliveryRejected
	}
	return false
}

// SupervisionOutboxStore is the durable surface the outbox needs. Lane B3 owns
// the table; Lane B7 binds this interface to it. Listing is scoped to one run
// because every caller here acts on one run's supervision.
type SupervisionOutboxStore interface {
	// AppendSupervisionOutbox persists entries that are not already live under
	// their deduplication key, and reports how many rows it actually wrote.
	AppendSupervisionOutbox(ctx context.Context, runID string, entries []SupervisionOutboxEntry) (int, error)
	// ListSupervisionOutbox returns this run's entries, oldest first.
	ListSupervisionOutbox(ctx context.Context, runID string) ([]SupervisionOutboxEntry, error)
	// TransitionSupervisionOutbox fences delivery ownership with a compare and
	// set on the current delivery state, exactly as TransitionNodeWake does.
	TransitionSupervisionOutbox(ctx context.Context, id string, from, to SupervisionDelivery, now time.Time) (bool, error)
}
