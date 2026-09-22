package wait

import (
	"context"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// SupervisionEscalationStore is the durable half of escalation delivery.
type SupervisionEscalationStore interface {
	PendingSupervisionEscalations(context.Context) ([]domain.SupervisionEscalationDelivery, error)
	TransitionSupervisionOutboxRow(ctx context.Context, id, from, to string, now time.Time) (bool, error)
}

// SupervisionEscalationClaimer atomically freezes the exact notification body
// and claims its single sender.
type SupervisionEscalationClaimer interface {
	ClaimSupervisionEscalation(ctx context.Context, id, from, payload string, now time.Time) (bool, error)
}

// tickSupervisionEscalations is the one supervisor-owned notification drain.
// It retries only known-no-effect states and reconciles ambiguous sends without
// treating bounded-history absence as proof.
func (r *Runner) tickSupervisionEscalations(ctx context.Context) {
	store, ok := r.store.(SupervisionEscalationStore)
	if !ok { return }
	control, ok := r.control.(NodeControl)
	if !ok { return }
	pending, err := store.PendingSupervisionEscalations(ctx)
	if err != nil {
		logFailure(ctx, r.log, "list supervision escalations", err, "error", err)
		return
	}
	now := r.now()
	for _, escalation := range pending {
		if escalation.Delivery == "sending" || escalation.Delivery == "recovery-required" {
			status, observeErr := reconcileNodeWake(ctx, control, escalation.ThreadID, escalation.DeliveryID)
			if observeErr != nil { continue }
			to := "recovery-required"
			switch status {
			case WakeReceiptDelivered:
				to = "delivered"
			case WakeReceiptRejected:
				to = "rejected"
			case WakeReceiptKnownNoEffect:
				to = "offline"
			}
			if to != escalation.Delivery {
				_, _ = store.TransitionSupervisionOutboxRow(ctx, escalation.ID, escalation.Delivery, to, now)
			}
			continue
		}
		if r.NodeDryRun {
			if escalation.Delivery == "pending" {
				_, _ = store.TransitionSupervisionOutboxRow(ctx, escalation.ID, "pending", "held", now)
			}
			continue
		}
		if escalation.DeliveryNextAttemptAt != nil && now.Before(*escalation.DeliveryNextAttemptAt) { continue }
		thread, getErr := r.control.GetThread(ctx, escalation.ThreadID)
		if getErr != nil || thread == nil {
			if escalation.Delivery != "offline" {
				_, _ = store.TransitionSupervisionOutboxRow(ctx, escalation.ID, escalation.Delivery, "offline", now)
			}
			continue
		}
		if thread.ArchivedAt != nil {
			_, _ = store.TransitionSupervisionOutboxRow(ctx, escalation.ID, escalation.Delivery, "rejected", now)
			continue
		}
		if healthy, _ := r.healthy(*thread); !healthy {
			if escalation.Delivery != "busy" {
				_, _ = store.TransitionSupervisionOutboxRow(ctx, escalation.ID, escalation.Delivery, "busy", now)
			}
			continue
		}
		text := escalation.Payload
		if text == "" {
			text = fmt.Sprintf(
				"Campaign supervision escalation (T3 steward): run %s, incident %s. Reason: %s.\n"+
					"Nothing the incident holds will proceed until it is resolved. "+
					"Inspect it with \"t3-steward campaign supervision show %s\" and resolve it with "+
					"\"t3-steward campaign supervision resolve %s --incident %s\".",
				escalation.RunID, escalation.IncidentID, escalation.Reason,
				escalation.RunID, escalation.RunID, escalation.IncidentID)
		}
		var claimed bool
		if freezer, ok := store.(SupervisionEscalationClaimer); ok {
			claimed, err = freezer.ClaimSupervisionEscalation(ctx, escalation.ID, escalation.Delivery, text, now)
		} else {
			claimed, err = store.TransitionSupervisionOutboxRow(ctx, escalation.ID, escalation.Delivery, "sending", now)
		}
		if err != nil || !claimed { continue }
		if err := control.SendNodeWake(ctx, *thread, escalation.DeliveryID, text); err != nil {
			_, _ = store.TransitionSupervisionOutboxRow(ctx, escalation.ID, "sending", "recovery-required", now)
		}
	}
}
