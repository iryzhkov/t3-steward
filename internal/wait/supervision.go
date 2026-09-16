package wait

import (
	"context"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// SupervisionEscalationStore is the durable half of escalation delivery. It is
// the same shape as NodeStore on purpose: an escalation is delivered by the
// mechanism that already delivers a node wake, so the plan's "add no new
// messaging integration and no recipient discovery" is satisfied by reusing this
// loop rather than by writing a second one.
type SupervisionEscalationStore interface {
	PendingSupervisionEscalations(context.Context) ([]domain.SupervisionEscalationDelivery, error)
	TransitionSupervisionOutboxRow(ctx context.Context, id, from, to string, now time.Time) (bool, error)
}

// tickSupervisionEscalations delivers every escalation whose incident is waiting
// for a human to the run's configured notify thread.
//
// It never repeats an uncertain external send. A claim is a compare-and-set on
// the delivery state, exactly as tickNodes does, and an interrupted send is
// resolved by positive message evidence rather than by its absence.
func (r *Runner) tickSupervisionEscalations(ctx context.Context) {
	store, ok := r.store.(SupervisionEscalationStore)
	if !ok {
		return
	}
	control, ok := r.control.(NodeControl)
	if !ok {
		return
	}
	pending, err := store.PendingSupervisionEscalations(ctx)
	if err != nil {
		r.log.Error("list supervision escalations", "error", err)
		return
	}
	for _, escalation := range pending {
		if escalation.Delivery == "sending" || escalation.Delivery == "recovery-required" {
			found, err := control.ObserveNodeWake(ctx, escalation.ThreadID, escalation.DeliveryID)
			if err != nil {
				continue
			}
			to := "recovery-required"
			if found {
				to = "delivered"
			}
			if to != escalation.Delivery {
				_, _ = store.TransitionSupervisionOutboxRow(ctx, escalation.ID, escalation.Delivery, to, r.now())
			}
			continue
		}
		if r.NodeDryRun {
			if escalation.Delivery == "pending" {
				_, _ = store.TransitionSupervisionOutboxRow(ctx, escalation.ID, "pending", "held", r.now())
			}
			continue
		}
		thread, err := r.control.GetThread(ctx, escalation.ThreadID)
		if err != nil || thread == nil || thread.ArchivedAt != nil {
			continue
		}
		if healthy, _ := r.healthy(*thread); !healthy {
			continue
		}
		claimed, err := store.TransitionSupervisionOutboxRow(ctx, escalation.ID, escalation.Delivery, "sending", r.now())
		if err != nil || !claimed {
			continue
		}
		text := fmt.Sprintf(
			"Campaign supervision escalation (T3 steward): run %s, incident %s. Reason: %s.\n"+
				"Nothing the incident holds will proceed until it is resolved. "+
				"Inspect it with \"t3-steward campaign supervision show %s\" and resolve it with "+
				"\"t3-steward campaign supervision resolve %s --incident %s\".",
			escalation.RunID, escalation.IncidentID, escalation.Reason,
			escalation.RunID, escalation.RunID, escalation.IncidentID)
		if err := control.SendNodeWake(ctx, *thread, escalation.DeliveryID, text); err != nil {
			_, _ = store.TransitionSupervisionOutboxRow(ctx, escalation.ID, "sending", "recovery-required", r.now())
		}
	}
}
