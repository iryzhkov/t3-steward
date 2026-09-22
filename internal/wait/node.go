package wait

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type NodeStore interface {
	SettleNodeWaits(context.Context, time.Time) error
	ListNodeWaits(context.Context) ([]domain.NodeWait, error)
	TransitionNodeWake(context.Context, string, string, string, time.Time) (bool, error)
}

// HostNodeStore is a node-wait store that can answer for one host alone. A
// store that implements it is asked only for the waits this runner could act
// on, which is the same set tickNodes keeps from a full list.
//
// It is optional because the local SQLite store has nothing to gain from it:
// the narrowing happens in the same process against the same rows. A store that
// answers over the admin transport has everything to gain, because the
// coordinator's node-wait table is append-only and the answer travels once per
// tick.
type HostNodeStore interface {
	ListNodeWaitsForHost(ctx context.Context, host string) ([]domain.NodeWait, error)
}

type NodeControl interface {
	ObserveNodeWake(context.Context, string, string) (bool, error)
	SendNodeWake(context.Context, domain.Thread, string, string) error
}

// WakeReceiptStatus distinguishes positive evidence from an unknown effect.
// Unknown never authorizes another send.
type WakeReceiptStatus string

const (
	WakeReceiptDelivered WakeReceiptStatus = "delivered"
	WakeReceiptRejected  WakeReceiptStatus = "rejected"
	WakeReceiptUnknown   WakeReceiptStatus = "unknown"
)

// NodeWakeReceiptControl explicitly advertises tri-state reconciliation.
type NodeWakeReceiptControl interface {
	ReconcileNodeWake(context.Context, string, string) (WakeReceiptStatus, error)
}

func reconcileNodeWake(ctx context.Context, control NodeControl, threadID, deliveryID string) (WakeReceiptStatus, error) {
	if receipts, ok := control.(NodeWakeReceiptControl); ok {
		return receipts.ReconcileNodeWake(ctx, threadID, deliveryID)
	}
	found, err := control.ObserveNodeWake(ctx, threadID, deliveryID)
	if found {
		return WakeReceiptDelivered, err
	}
	return WakeReceiptUnknown, err
}

// NodeGroupClaimer atomically freezes a wake's exact payload and, for wake-all,
// every member that rides in the same external effect.
type NodeGroupClaimer interface {
	ClaimNodeWakeGroup(ctx context.Context, leaderID, from, deliveryID, payload string, memberIDs []string, now time.Time) (bool, error)
}

// nodeWakeProse is the human part of a node or quota wake, after the trailer.
func nodeWakeProse(w domain.NodeWait) string {
	if w.Request.Quota != nil {
		return fmt.Sprintf("Wait finished (T3 steward): %q. Quota pool %s: %s.\nContinue the work that was waiting on this.",
			w.Request.Name, w.Request.Quota.Pool, w.Observation.Reason)
	}
	return fmt.Sprintf("Wait finished (T3 steward): %q. Node %s: %s (exit %d).%s\nContinue the work that was waiting on this.",
		w.Request.Name, w.Request.Target.String(), w.Observation.Reason, w.Observation.ExitCode, nodeWakeObservation(w))
}

// nodeWakeObservation is the evidence clause, and is empty when there is no
// evidence to state.
//
// A run sink has no attempt of its own: it is the run's join point, and it is
// exactly the target "task run" and "campaign submit --notify-thread" register
// on. Printing "Observed attempt , run revision 0" for it said nothing twice.
func nodeWakeObservation(w domain.NodeWait) string {
	switch {
	case w.Observation == nil:
		return ""
	case w.Observation.AttemptID != "":
		return fmt.Sprintf(" Observed attempt %s, run revision %d.", w.Observation.AttemptID, w.Observation.RunRevision)
	case w.Observation.RunRevision != 0:
		return fmt.Sprintf(" Observed at run revision %d.", w.Observation.RunRevision)
	default:
		return ""
	}
}

// tickNodes never repeats an uncertain external send. The durable sending state
// survives a crash before or after Dispatch. Only positive message evidence
// resolves it; absence in a bounded read is not proof of non-delivery.
func (r *Runner) tickNodes(ctx context.Context) {
	// A configured transport wins over the local store. On a host that runs no
	// coordinator the local store answers this interface and holds no node
	// waits at all, so preferring it would deliver nothing and say nothing.
	store := r.NodeStore
	if store == nil {
		local, ok := r.store.(NodeStore)
		if !ok {
			return
		}
		store = local
	}
	if err := store.SettleNodeWaits(ctx, r.now()); err != nil {
		logFailure(ctx, r.log, "settle node waits", err, "error", err)
		return
	}
	host := r.NodeHost
	if host == "" {
		host, _ = os.Hostname()
	}
	waits, err := r.listNodeWaits(ctx, store, host)
	if err != nil {
		logFailure(ctx, r.log, "list node waits", err, "error", err)
		return
	}
	control, ok := r.control.(NodeControl)
	if !ok {
		return
	}
	if r.NodeDelivery != nil && !r.NodeDryRun {
		r.NodeDelivery(ctx, host)
	}
	groups := nodeWaitGroups(waits, host)
	for _, w := range waits {
		if w.Host != host {
			continue
		}
		if w.SettledAt == nil || w.Delivery == "delivered" || w.Delivery == "cancelled" {
			continue
		}
		members, grouped := groups[nodeGroupKey(w)]
		if grouped {
			// A --wake all group wakes once, with one message, when every member
			// has settled; the earliest member carries the send and the others
			// are marked delivered with it.
			if !nodeGroupSettled(members) || members[0].Request.ID != w.Request.ID {
				continue
			}
		}
		if w.Delivery == "sending" || w.Delivery == "recovery-required" {
			status := WakeReceiptUnknown
			var err error
			if receipts, ok := control.(NodeWakeReceiptControl); ok {
				status, err = receipts.ReconcileNodeWake(ctx, w.Request.ThreadID, w.DeliveryID)
			} else {
				var found bool
				found, err = control.ObserveNodeWake(ctx, w.Request.ThreadID, w.DeliveryID)
				if found {
					status = WakeReceiptDelivered
				}
			}
			if err != nil {
				continue
			}
			to := "recovery-required"
			if status == WakeReceiptDelivered {
				to = "delivered"
			}
			if to != w.Delivery {
				if _, err := store.TransitionNodeWake(ctx, w.Request.ID, w.Delivery, to, r.now()); err != nil {
					logNodeWakeTransition(ctx, r, w.Request.ID, w.Delivery, to, host, err)
				}
			}
			continue
		}
		if r.NodeDryRun {
			if w.Delivery == "pending" {
				if _, err := store.TransitionNodeWake(ctx, w.Request.ID, "pending", "held", r.now()); err != nil {
					logNodeWakeTransition(ctx, r, w.Request.ID, "pending", "held", host, err)
				}
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
		text := nodeTrailer(w) + "\n\n" + nodeWakeProse(w)
		memberIDs := []string{w.Request.ID}
		if grouped {
			text = nodeGroupMessage(members)
			memberIDs = memberIDs[:0]
			for _, member := range members {
				memberIDs = append(memberIDs, member.Request.ID)
			}
		}
		var claimed bool
		if freezer, ok := store.(NodeGroupClaimer); ok {
			claimed, err = freezer.ClaimNodeWakeGroup(ctx, w.Request.ID, w.Delivery, w.DeliveryID, text, memberIDs, r.now())
		} else {
			claimed, err = store.TransitionNodeWake(ctx, w.Request.ID, w.Delivery, "sending", r.now())
		}
		if err != nil {
			logNodeWakeTransition(ctx, r, w.Request.ID, w.Delivery, "sending", host, err)
			continue
		}
		if !claimed {
			continue
		}
		if err := control.SendNodeWake(ctx, *thread, w.DeliveryID, text); err != nil {
			if _, err := store.TransitionNodeWake(ctx, w.Request.ID, "sending", "recovery-required", r.now()); err != nil {
				logNodeWakeTransition(ctx, r, w.Request.ID, "sending", "recovery-required", host, err)
			}
			continue
		}
		if grouped {
			_, frozen := store.(NodeGroupClaimer)
			for _, member := range members[1:] {
				if !frozen {
					claimed, err := store.TransitionNodeWake(ctx, member.Request.ID, member.Delivery, "sending", r.now())
					if err != nil {
						logNodeWakeTransition(ctx, r, member.Request.ID, member.Delivery, "sending", host, err)
						continue
					}
					if !claimed {
						continue
					}
				}
				if _, err := store.TransitionNodeWake(ctx, member.Request.ID, "sending", "delivered", r.now()); err != nil {
					logNodeWakeTransition(ctx, r, member.Request.ID, "sending", "delivered", host, err)
				}
			}
		}
	}
}

// listNodeWaits reads the waits this runner could act on, asking the store to
// narrow the answer when it can do so itself.
//
// The narrowing is exactly the filter the delivery loop below applies anyway:
// this host's waits, and only while their delivery has not ended. A store that
// cannot narrow answers with everything and the loop discards the rest, which
// is what a store behind an older coordinator does.
func (r *Runner) listNodeWaits(ctx context.Context, store NodeStore, host string) ([]domain.NodeWait, error) {
	if scoped, ok := store.(HostNodeStore); ok && host != "" {
		return scoped.ListNodeWaitsForHost(ctx, host)
	}
	return store.ListNodeWaits(ctx)
}

// logNodeWakeTransition reports a delivery transition the store refused.
//
// On a coordinator this store is local SQLite and an error here means a corrupt
// database. On every other host it is the coordinator over the admin transport,
// where an error means a coordinator that was rolled back to a release without
// the transition, an expired credential or a transport fault -- and the
// consequence is a wake that silently never arrives, which is indistinguishable
// from the defect this delivery path exists to fix. It names the wait, the
// transition it was refused and the host that tried, which is what turns
// "delivery=pending forever" into one log line with a cause.
func logNodeWakeTransition(ctx context.Context, r *Runner, id, from, to, host string, err error) {
	logFailure(ctx, r.log, "move a node wake through its delivery states", err,
		"wait", id, "from", from, "to", to, "host", host, "error", err)
}

// nodeGroupKey identifies a --wake all group: one thread, one group name.
func nodeGroupKey(w domain.NodeWait) string {
	return w.Request.ThreadID + "\x00" + w.Request.Group
}

// nodeWaitGroups collects this host's --wake all groups, each sorted by
// creation so the earliest member carries the send. A member that is already
// delivered or cancelled is left out: it has nothing more to say, and keeping
// it would make it the earliest member of a group it can no longer send for.
func nodeWaitGroups(waits []domain.NodeWait, host string) map[string][]domain.NodeWait {
	groups := map[string][]domain.NodeWait{}
	for _, w := range waits {
		if w.Host != host || w.Request.Group == "" || w.Request.Wake != domain.WakeAll || w.Delivery == "delivered" || w.Delivery == "cancelled" {
			continue
		}
		key := nodeGroupKey(w)
		groups[key] = append(groups[key], w)
	}
	for key := range groups {
		sort.Slice(groups[key], func(i, j int) bool { return groups[key][i].CreatedAt.Before(groups[key][j].CreatedAt) })
	}
	return groups
}

func nodeGroupSettled(members []domain.NodeWait) bool {
	for _, member := range members {
		if member.SettledAt == nil {
			return false
		}
	}
	return true
}

// nodeGroupMessage is the one wake of a settled group: the earliest member's
// trailer with the member count, then each member's prose.
func nodeGroupMessage(members []domain.NodeWait) string {
	var b strings.Builder
	b.WriteString(nodeTrailer(members[0]))
	fmt.Fprintf(&b, " count=%d\n\nWaits finished (T3 steward): %d conditions of group %q settled.\n", len(members), len(members), members[0].Request.Group)
	for _, member := range members {
		b.WriteString("\n## ")
		b.WriteString(member.Request.ID)
		b.WriteString("\n")
		b.WriteString(nodeTrailer(member))
		b.WriteString("\n")
		b.WriteString(nodeWakeProse(member))
		b.WriteString("\n")
	}
	return b.String()
}
