package wait

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type NodeStore interface {
	SettleNodeWaits(context.Context, time.Time) error
	ListNodeWaits(context.Context) ([]domain.NodeWait, error)
	TransitionNodeWake(context.Context, string, string, string, time.Time) (bool, error)
}
type NodeControl interface {
	ObserveNodeWake(context.Context, string, string) (bool, error)
	SendNodeWake(context.Context, domain.Thread, string, string) error
}

// tickNodes never repeats an uncertain external send. The durable sending state
// survives a crash before or after Dispatch. Only positive message evidence
// resolves it; absence in a bounded read is not proof of non-delivery.
func (r *Runner) tickNodes(ctx context.Context) {
	store, ok := r.store.(NodeStore)
	if !ok {
		return
	}
	if err := store.SettleNodeWaits(ctx, r.now()); err != nil {
		r.log.Error("settle node waits", "error", err)
		return
	}
	waits, err := store.ListNodeWaits(ctx)
	if err != nil {
		r.log.Error("list node waits", "error", err)
		return
	}
	control, ok := r.control.(NodeControl)
	if !ok {
		return
	}
	host := r.NodeHost
	if host == "" {
		host, _ = os.Hostname()
	}
	for _, w := range waits {
		if w.Host != host {
			continue
		}
		if w.SettledAt == nil || w.Delivery == "delivered" || w.Delivery == "cancelled" {
			continue
		}
		if w.Delivery == "sending" || w.Delivery == "recovery-required" {
			found, err := control.ObserveNodeWake(ctx, w.Request.ThreadID, w.DeliveryID)
			if err != nil {
				continue
			}
			to := "recovery-required"
			if found {
				to = "delivered"
			}
			if to != w.Delivery {
				_, _ = store.TransitionNodeWake(ctx, w.Request.ID, w.Delivery, to, r.now())
			}
			continue
		}
		if r.NodeDryRun {
			if w.Delivery == "pending" {
				_, _ = store.TransitionNodeWake(ctx, w.Request.ID, "pending", "held", r.now())
			}
			continue
		}
		thread, err := r.control.GetThread(ctx, w.Request.ThreadID)
		if err != nil || thread == nil || thread.ArchivedAt != nil {
			continue
		}
		if healthy, _ := r.healthy(*thread); !healthy {
			continue
		}
		claimed, err := store.TransitionNodeWake(ctx, w.Request.ID, w.Delivery, "sending", r.now())
		if err != nil || !claimed {
			continue
		}
		text := fmt.Sprintf("Wait finished (T3 steward): %q. Node %s: %s (exit %d). Observed attempt %s, run revision %d.\nContinue the work that was waiting on this.", w.Request.Name, w.Request.Target.String(), w.Observation.Reason, w.Observation.ExitCode, w.Observation.AttemptID, w.Observation.RunRevision)
		if err := control.SendNodeWake(ctx, *thread, w.DeliveryID, text); err != nil {
			_, _ = store.TransitionNodeWake(ctx, w.Request.ID, "sending", "recovery-required", r.now())
		}
	}
}
