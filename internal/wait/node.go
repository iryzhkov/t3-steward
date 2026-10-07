package wait

import (
	"context"
	"errors"
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

// WakeReceiptStatus distinguishes authoritative evidence from a bounded,
// inconclusive observation. Unknown never authorizes another send.
type WakeReceiptStatus string

const (
	WakeReceiptDelivered     WakeReceiptStatus = "delivered"
	WakeReceiptRejected      WakeReceiptStatus = "rejected"
	WakeReceiptKnownNoEffect WakeReceiptStatus = "known-no-effect"
	WakeReceiptUnknown       WakeReceiptStatus = "unknown"
)

// NodeWakeReceiptControl is implemented only by transports that can state the
// strength of their receipt evidence.
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

// NodeGroupClaimer atomically freezes an exact payload and every wake-all
// member represented by that one external effect.
type NodeGroupClaimer interface {
	ClaimNodeWakeGroup(ctx context.Context, leaderID, from, deliveryID, payload string, memberIDs []string, now time.Time) (bool, error)
}

// nodeWakeProse is the human part of a node or quota wake, after the trailer.
func nodeWakeProse(w domain.NodeWait) string {
	rows := summaryTableRows
	return nodeWakeProseWith(w, nil, &rows)
}

// nodeWakeProseWith is nodeWakeProse with the wake's summary, when one was
// built. A summary replaces the generic sentence with the answer; one that
// could not be built leaves the generic prose and says so in one fixed line.
func nodeWakeProseWith(w domain.NodeWait, summary *WakeSummary, rows *int) string {
	if w.Request.Quota != nil {
		return fmt.Sprintf("Wait finished (T3 steward): %q. Quota pool %s: %s. Recheck authentic fresh quota eligibility before provider work; a reset deadline alone does not confirm recovery.\n%s",
			w.Request.Name, w.Request.Quota.Pool, w.Observation.Reason, wakeGuidance)
	}
	if summary != nil && summary.Unavailable == "" {
		return summary.nodeProse(rows) + wakeGuidance
	}
	text := fmt.Sprintf("Wait finished (T3 steward): %q. Node %s: %s (exit %d).%s%s\n",
		w.Request.Name, w.Request.Target.String(), w.Observation.Reason, w.Observation.ExitCode, nodeWakeObservation(w), nodeWakeResult(w))
	if summary != nil {
		text += summary.unavailableLine() + "\n"
	}
	return text + wakeGuidance
}

// nodeWakeMessage is the whole wake of one node or quota wait.
func nodeWakeMessage(w domain.NodeWait, summary *WakeSummary) string {
	rows := summaryTableRows
	return nodeTrailerWith(w, summary) + "\n\n" + nodeWakeProseWith(w, summary, &rows)
}

// nodeWakeResult renders only a bounded, command-safe terminal run identity.
// It changes prose only; machine trailer fields keep their original bytes.
func nodeWakeResult(w domain.NodeWait) string {
	if w.Observation == nil || !w.Observation.Progress.Terminal() {
		return ""
	}
	run := w.Observation.Target.RunID
	if !commandSafeRunID(run) {
		return ""
	}
	return "\nCollect and inspect the actual run result with `t3-steward task result " + run + "` before deciding what to do next."
}

// commandSafeRunID reports whether a run ID can be printed inside a command
// line: bounded, alphanumeric first, then only - _ and dots.
func commandSafeRunID(run string) bool {
	if run == "" || len(run) > 128 || !(run[0] >= 'a' && run[0] <= 'z' || run[0] >= 'A' && run[0] <= 'Z' || run[0] >= '0' && run[0] <= '9') {
		return false
	}
	for _, c := range run {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
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

// tickNodes owns durable node-wake delivery. It freezes the exact external
// effect before sending, retries only outcomes known to have had no effect, and
// never treats absence from a bounded observation window as non-delivery.
func (r *Runner) tickNodes(ctx context.Context) {
	store := r.nodeStore()
	if store == nil {
		return
	}
	now := r.now()
	if err := store.SettleNodeWaits(ctx, now); err != nil {
		logFailure(ctx, r.log, "settle node waits", err, "error", err)
		return
	}
	host := r.nodeHost()
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
		if w.Host != host || w.SettledAt == nil || w.Delivery == "delivered" || w.Delivery == "rejected" || w.Delivery == "cancelled" {
			continue
		}
		members, grouped := groups[nodeGroupKey(w)]
		if grouped && w.Delivery != "sending" && w.Delivery != "recovery-required" &&
			(!nodeGroupSettled(members) || members[0].Request.ID != w.Request.ID) {
			continue
		}
		if w.Delivery == "sending" || w.Delivery == "recovery-required" {
			status, observeErr := reconcileNodeWake(ctx, control, w.Request.ThreadID, w.DeliveryID)
			if observeErr != nil || status == WakeReceiptUnknown {
				// No receipt either way. A thread that is gone has none to
				// find and nothing to resend into, so reconciling it again is
				// a loop with no exit; the wake is ended instead.
				if _, presence := r.lookupThread(ctx, w.Request.ThreadID); presence == threadGone {
					r.rejectGoneWakes(ctx, store, []domain.NodeWait{w}, host, now)
					continue
				}
			}
			if observeErr != nil {
				continue
			}
			to := "recovery-required"
			switch status {
			case WakeReceiptDelivered:
				to = "delivered"
			case WakeReceiptRejected:
				to = "rejected"
			case WakeReceiptKnownNoEffect:
				to = "offline"
			}
			if to != w.Delivery {
				if _, err := store.TransitionNodeWake(ctx, w.Request.ID, w.Delivery, to, now); err != nil {
					logNodeWakeTransition(ctx, r, w.Request.ID, w.Delivery, to, host, err)
				}
			}
			continue
		}
		if r.NodeDryRun {
			if w.Delivery == "pending" {
				if _, err := store.TransitionNodeWake(ctx, w.Request.ID, "pending", "held", now); err != nil {
					logNodeWakeTransition(ctx, r, w.Request.ID, "pending", "held", host, err)
				}
			}
			continue
		}
		if w.DeliveryNextAttemptAt != nil && now.Before(*w.DeliveryNextAttemptAt) {
			continue
		}
		thread, presence := r.lookupThread(ctx, w.Request.ThreadID)
		switch presence {
		case threadUnanswered, threadMissing:
			if w.Delivery != "offline" {
				if _, err := store.TransitionNodeWake(ctx, w.Request.ID, w.Delivery, "offline", now); err != nil {
					logNodeWakeTransition(ctx, r, w.Request.ID, w.Delivery, "offline", host, err)
				}
			}
			continue
		case threadGone:
			// The members of a --wake all group ride on this one send, so they
			// end with it: left behind, each would wait for a leader that will
			// never send, which is how two waits stayed pending for a week.
			ended := []domain.NodeWait{w}
			if grouped {
				ended = members
			}
			r.rejectGoneWakes(ctx, store, ended, host, now)
			continue
		}
		if healthy, _ := r.healthy(*thread); !healthy {
			if w.Delivery != "busy" {
				if _, err := store.TransitionNodeWake(ctx, w.Request.ID, w.Delivery, "busy", now); err != nil {
					logNodeWakeTransition(ctx, r, w.Request.ID, w.Delivery, "busy", host, err)
				}
			}
			continue
		}
		var text string
		memberIDs := []string{w.Request.ID}
		if grouped {
			memberIDs = memberIDs[:0]
			for _, member := range members {
				memberIDs = append(memberIDs, member.Request.ID)
			}
		}
		if w.DeliveryPayload != "" {
			// An earlier attempt froze this wake and is known to have had no
			// effect (offline, busy). The retry sends the same bytes: the
			// summary reads live coordinator state, so rebuilding it could
			// differ, and the store refuses a claim that differs from the
			// frozen payload, which would leave the wake unsendable.
			text = w.DeliveryPayload
			if len(w.DeliveryGroupMembers) != 0 {
				memberIDs = append([]string(nil), w.DeliveryGroupMembers...)
			}
		} else {
			// The summary is built here, once, before the payload is frozen
			// below: a wake already sending or awaiting recovery was handled
			// above with its frozen bytes and never reaches this point.
			wake := []domain.NodeWait{w}
			if grouped {
				wake = members
			}
			summaries := buildNodeSummaries(ctx, r.NodeSummary, wake)
			text = nodeWakeMessage(w, summaries[0])
			if grouped {
				text = nodeGroupMessageWith(members, summaries)
			}
		}
		var claimed bool
		if freezer, ok := store.(NodeGroupClaimer); ok {
			claimed, err = freezer.ClaimNodeWakeGroup(ctx, w.Request.ID, w.Delivery, w.DeliveryID, text, memberIDs, now)
		} else {
			claimed, err = store.TransitionNodeWake(ctx, w.Request.ID, w.Delivery, "sending", now)
		}
		if err != nil {
			logNodeWakeTransition(ctx, r, w.Request.ID, w.Delivery, "sending", host, err)
			continue
		}
		if !claimed {
			continue
		}
		if err := control.SendNodeWake(ctx, *thread, w.DeliveryID, text); err != nil {
			if _, err := store.TransitionNodeWake(ctx, w.Request.ID, "sending", "recovery-required", now); err != nil {
				logNodeWakeTransition(ctx, r, w.Request.ID, "sending", "recovery-required", host, err)
			}
			continue
		}
		if grouped {
			if _, frozen := store.(NodeGroupClaimer); frozen {
				// Every member names the same frozen effect and remains sending
				// until the shared remote message is positively observed.
				continue
			}
			for _, member := range members[1:] {
				claimed, err := store.TransitionNodeWake(ctx, member.Request.ID, member.Delivery, "sending", now)
				if err != nil {
					logNodeWakeTransition(ctx, r, member.Request.ID, member.Delivery, "sending", host, err)
					continue
				}
				if !claimed {
					continue
				}
				if _, err := store.TransitionNodeWake(ctx, member.Request.ID, "sending", "delivered", now); err != nil {
					logNodeWakeTransition(ctx, r, member.Request.ID, "sending", "delivered", host, err)
				}
			}
		}
	}
}

// nodeStore is where this runner reads and moves node waits: the transport to
// the coordinator when one is configured, and otherwise the local store when it
// holds node waits itself (a coordinator host). Nil means this host has none.
func (r *Runner) nodeStore() NodeStore {
	if r.NodeStore != nil {
		return r.NodeStore
	}
	if local, ok := r.store.(NodeStore); ok {
		return local
	}
	return nil
}

// nodeHost is the host whose node wakes this runner delivers.
func (r *Runner) nodeHost() string {
	if r.NodeHost != "" {
		return r.NodeHost
	}
	host, _ := os.Hostname()
	return host
}

// threadGoneConfirm and threadGoneAnswers are what it takes for a thread that
// T3 answers without -- in neither its shell snapshot nor its full index -- to
// count as gone: at least threadGoneAnswers consecutive answers without it,
// spanning at least threadGoneConfirm. Any lookup T3 does not answer, and any
// answer that holds the thread, starts the count again.
//
// One answer is not enough, because ending a wake cannot be undone and absence
// from one answer is evidence rather than proof. A thread T3 reports deleted,
// or archived, is authoritative and needs no confirmation. Before this, a wake
// for a deleted thread was retried every tick for as long as the steward ran.
const (
	threadGoneConfirm = time.Minute
	threadGoneAnswers = 3
)

// ThreadLookupControl is a control that can say why a thread is not live:
// deleted, archived, or absent from an answer that held other threads. The T3
// control implements it; a control that does not is read through GetThread,
// whose nil thread is taken as absent.
type ThreadLookupControl interface {
	LookupThread(ctx context.Context, threadID string) (*domain.Thread, domain.ThreadPresence, error)
}

// threadAbsence is the running confirmation that a thread is absent.
type threadAbsence struct {
	first, last time.Time
	answers     int
}

// threadPresence is what T3's answer says about a wake's thread.
type threadPresence int

const (
	// threadLive: T3 holds the thread and it is not archived.
	threadLive threadPresence = iota
	// threadUnanswered: T3 did not answer (transport, credential or server
	// error), which says nothing about the thread. Always retryable.
	threadUnanswered
	// threadMissing: T3 answered without the thread, not yet confirmed (see
	// threadGoneConfirm). Retryable.
	threadMissing
	// threadGone: T3 reports the thread deleted or archived, or the absence
	// is confirmed. Terminal for every wake addressed to it.
	threadGone
)

// lookupThread asks T3 for a wake's thread and classifies the answer. The
// confirmation of an absence is kept in memory: a restart only restarts it,
// which errs towards retrying.
func (r *Runner) lookupThread(ctx context.Context, id string) (*domain.Thread, threadPresence) {
	thread, presence, err := r.lookupPresence(ctx, id)
	if err != nil {
		// No answer says nothing about the thread, and breaks the run of
		// consecutive answers an absence needs.
		delete(r.threadAbsentSince, id)
		return nil, threadUnanswered
	}
	switch presence {
	case domain.ThreadLive:
		delete(r.threadAbsentSince, id)
		return thread, threadLive
	case domain.ThreadDeleted, domain.ThreadArchived:
		delete(r.threadAbsentSince, id)
		return thread, threadGone
	}
	now := r.now()
	if r.threadAbsentSince == nil {
		r.threadAbsentSince = map[string]threadAbsence{}
	}
	absence := r.threadAbsentSince[id]
	switch {
	case absence.answers == 0:
		absence = threadAbsence{first: now, last: now, answers: 1}
	case now.After(absence.last):
		// Several wakes of one thread looked up at one instant are one answer.
		absence.last = now
		absence.answers++
	}
	r.threadAbsentSince[id] = absence
	if absence.answers >= threadGoneAnswers && now.Sub(absence.first) >= threadGoneConfirm {
		return nil, threadGone
	}
	return nil, threadMissing
}

// lookupPresence reads the thread through LookupThread when the control has
// it, and through GetThread otherwise.
func (r *Runner) lookupPresence(ctx context.Context, id string) (*domain.Thread, domain.ThreadPresence, error) {
	if lookup, ok := r.control.(ThreadLookupControl); ok {
		return lookup.LookupThread(ctx, id)
	}
	thread, err := r.control.GetThread(ctx, id)
	switch {
	case err != nil:
		return nil, "", err
	case thread == nil:
		return nil, domain.ThreadAbsent, nil
	case thread.ArchivedAt != nil:
		return thread, domain.ThreadArchived, nil
	default:
		return thread, domain.ThreadLive, nil
	}
}

// rejectGoneWakes ends the given wakes, whose thread is gone, and logs once
// that it did so with the reason, so "delivery=rejected" has a cause in the
// journal of the host that decided it.
func (r *Runner) rejectGoneWakes(ctx context.Context, store NodeStore, wakes []domain.NodeWait, host string, now time.Time) {
	var ended []string
	for _, w := range wakes {
		if nodeWakeEnded(w.Delivery) {
			continue
		}
		changed, err := store.TransitionNodeWake(ctx, w.Request.ID, w.Delivery, "rejected", now)
		if err != nil {
			logNodeWakeTransition(ctx, r, w.Request.ID, w.Delivery, "rejected", host, err)
			continue
		}
		if changed {
			ended = append(ended, w.Request.ID)
		}
	}
	if len(ended) != 0 {
		r.log.Warn("node wakes rejected: their thread is archived or deleted in T3, so nothing can deliver them",
			"waits", strings.Join(ended, ","), "thread", wakes[0].Request.ThreadID, "host", host)
	}
}

// nodeWakeEnded reports a delivery state nothing moves out of.
func nodeWakeEnded(delivery string) bool {
	return delivery == "delivered" || delivery == "rejected" || delivery == "cancelled"
}

// RejectThreadWakes ends every node wake this host still owes a thread it has
// just deleted from T3, and returns how many it ended.
//
// The archive calls it after a delete. Without it the wakes of a deleted
// thread waited for the delivery tick to notice, and before the tick could
// notice they were retried forever: waits nw-f2bf0274 and nw-23326045 stayed
// pending for a week for a thread the steward's own archive had deleted.
//
// A wake still unsettled is ended too: its thread is gone, so its outcome has
// nowhere to go. A delivery tick may claim a wake between this read and its
// transition, so a transition that finds the state moved is retried against a
// fresh read, a bounded number of times.
func (r *Runner) RejectThreadWakes(ctx context.Context, threadID string) (int, error) {
	store := r.nodeStore()
	if store == nil || threadID == "" {
		return 0, nil
	}
	host := r.nodeHost()
	var rejected int
	var failures []error
	var moved []string
	for round := 0; round < 3; round++ {
		waits, err := r.listNodeWaits(ctx, store, host)
		if err != nil {
			return rejected, fmt.Errorf("read the node wakes of thread %s: %w", threadID, err)
		}
		moved = moved[:0]
		failures = failures[:0]
		for _, w := range waits {
			if w.Host != host || w.Request.ThreadID != threadID || nodeWakeEnded(w.Delivery) {
				continue
			}
			changed, err := store.TransitionNodeWake(ctx, w.Request.ID, w.Delivery, "rejected", r.now())
			switch {
			case err != nil:
				failures = append(failures, fmt.Errorf("reject node wake %s (%s): %w", w.Request.ID, w.Delivery, err))
			case changed:
				rejected++
			default:
				moved = append(moved, w.Request.ID)
			}
		}
		if len(moved) == 0 {
			break
		}
	}
	delete(r.threadAbsentSince, threadID)
	if len(moved) != 0 {
		// The bound stays: a wake that keeps moving is being worked on by a
		// delivery tick, which ends it itself once T3 confirms the thread is
		// gone. The caller is told the cleanup is incomplete and which wakes.
		failures = append(failures, fmt.Errorf(
			"cleanup of thread %s is incomplete: node wakes %s changed state on each of 3 reads; the delivery loop ends them once T3 confirms the thread is gone, and t3-steward triage lists any it does not",
			threadID, strings.Join(moved, ", ")))
	}
	return rejected, errors.Join(failures...)
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
		if w.Host != host || w.Request.Group == "" || w.Request.Wake != domain.WakeAll || w.Delivery == "delivered" || w.Delivery == "rejected" || w.Delivery == "cancelled" {
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
	return nodeGroupMessageWith(members, make([]*WakeSummary, len(members)))
}

// nodeGroupMessageWith is nodeGroupMessage with each member's summary, in
// member order. The members' tables share one row allowance.
func nodeGroupMessageWith(members []domain.NodeWait, summaries []*WakeSummary) string {
	var b strings.Builder
	b.WriteString(nodeTrailerWith(members[0], summaries[0]))
	fmt.Fprintf(&b, " count=%d\n\nWaits finished (T3 steward): %d conditions of group %q settled.\n", len(members), len(members), members[0].Request.Group)
	rows := summaryGroupRows
	for i, member := range members {
		b.WriteString("\n## ")
		b.WriteString(member.Request.ID)
		b.WriteString("\n")
		b.WriteString(nodeTrailerWith(member, summaries[i]))
		b.WriteString("\n")
		b.WriteString(nodeWakeProseWith(member, summaries[i], &rows))
		b.WriteString("\n")
	}
	return b.String()
}
