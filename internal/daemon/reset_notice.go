package daemon

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// deliverResetNotices rolls the quota reset out to each thread, exactly once,
// that was warned, drained or stopped for a window whose reported reset time
// has now passed.
//
// A warning can make a session pause of its own accord, and a voluntary pause
// leaves no resume intent behind, so nothing was watching for it at all.
// Because T3 delivers a message only as thread.turn.start, one delivery is
// both the notice and the restart for such a thread; treating that as
// incidental was how the gap stayed open. It is deliberate here, and bounded
// by the three rules the rollout rests on:
//
//   - Pace. One turn is started per provider instance per
//     reset_notice.interval_between_threads, counted against the resume
//     path's dispatches too, so a window is never hit by a broadcast.
//   - Probe. The first turn started for a window no reading has confirmed is
//     registered as that provider's probe, because the turn produces exactly
//     the reading the probe exists to obtain, and one probe is enough.
//   - Evidence. The moment a reading from the new window contradicts the
//     reset, by phase or by usage at the warn threshold, the rollout stops
//     and the remaining advisories stay owed. The rollout generates its own
//     evidence and obeys it.
//
// What this never does is manufacture recovery. It does not write
// RecoveredAt, does not rearm a bucket and does not make a resume intent
// eligible: the clock starts a turn, and only a reading decides anything.
func (d *Daemon) deliverResetNotices(ctx context.Context, threads []domain.Thread, states []domain.BucketState) {
	if !d.cfg.ResetNotice.Enabled {
		return
	}
	now := d.now()
	pending, err := d.store.PendingQuotaResetNotices(ctx, now)
	if err != nil {
		d.log.Error("list pending quota reset notices", "err", err)
		return
	}
	if len(pending) == 0 {
		return
	}
	dispatched, err := d.store.DispatchedThreads(ctx)
	if err != nil {
		d.log.Error("list dispatched threads", "err", err)
		return
	}
	intents, err := d.resumeIntentStatus(ctx)
	if err != nil {
		d.log.Error("list resume intents", "err", err)
		return
	}
	byID := map[string]domain.Thread{}
	for _, t := range threads {
		byID[t.ID] = t
	}
	byKey := map[domain.BucketKey]domain.BucketState{}
	for _, st := range states {
		byKey[st.Key] = st
	}
	halted := map[string]bool{}
	for _, n := range pending {
		log := d.log.With("thread", n.ThreadID, "bucket", n.Key.String())
		window := n.Key.String() + "#" + n.Epoch
		if halted[window] {
			continue
		}
		st, haveState := byKey[n.Key]
		if why := d.rolloutContradicted(n, st, haveState); why != "" {
			// A reading has come back from the new window and it does not
			// support the reset. Everything still owed for this window stays
			// owed; the ladder acts on that reading through its own path.
			halted[window] = true
			log.Warn("quota reset rollout halted by a reading", "reason", why)
			d.record(ctx, domain.ActionRecord{Kind: domain.ActionResetNotice, Bucket: n.Key.String(),
				Detail: "rollout halted, remaining advisories still owed: " + why})
			continue
		}
		thread, listed := byID[n.ThreadID]
		// A thread that is gone, archived or settled is not messaged, the
		// same way the wake path cancels a wait rather than waking a thread
		// that is no longer there.
		switch {
		case !listed:
			d.skipResetNotice(ctx, n, now, "thread gone or no longer listed")
			continue
		case thread.Settled():
			d.skipResetNotice(ctx, n, now, "thread archived or settled")
			continue
		case !d.ControlAllowed:
			d.skipResetNotice(ctx, n, now, "control disabled: "+d.ControlReason)
			continue
		case dispatched[n.ThreadID] != "":
			// Coordinator-owned work. Nobody reads its messages, and a turn
			// started from outside would run with no coordinator ownership
			// of what it then did, which is why the node-wait path refuses
			// to wake a task-bound thread itself.
			//
			// The registry holds the threads this host's backlog runner and
			// scheduled jobs started. Backlog v2 work executes on workers
			// and is not registered here; it is also not in this watchdog's
			// local thread list, so it never reaches this loop. Should that
			// change, such threads would fall through to the resume test
			// below instead of being recognised as coordinator-owned, and
			// this check is where they would have to be added.
			d.skipResetNotice(ctx, n, now, "coordinator-owned task thread "+dispatched[n.ThreadID])
			continue
		}
		if why := d.alreadyRestarted(n.ThreadID, intents); why != "" {
			// Already awake by another route; there is nothing left to start.
			d.skipResetNotice(ctx, n, now, why)
			continue
		}
		// A message to a running thread joins the turn it is already taking
		// and costs no new capacity. A message to an idle thread starts one,
		// and that is the paced, probing part of the rollout.
		startsTurn := !thread.Running
		if startsTurn && !d.noticeSlotFree(n.Key.ProviderInstanceID, now) {
			log.Debug("quota reset delivery held for the pacing interval")
			continue
		}
		// Claim the advisory before sending it. A crash between the claim and
		// the send loses one message; the opposite order would repeat it, and
		// a repeated message is the failure this feature must not introduce.
		claimed, err := d.store.SettleQuotaResetNotice(ctx, n.ThreadID, n.Key, n.Epoch, now, "sent")
		if err != nil {
			log.Error("claim quota reset notice", "err", err)
			continue
		}
		if !claimed {
			log.Debug("quota reset notice already delivered")
			continue
		}
		unconfirmed := !haveState || !st.ObservedAt.After(n.ResetsAt)
		if startsTurn {
			d.takeNoticeSlot(n.Key.ProviderInstanceID, n.ResetsAt, unconfirmed, now)
		}
		delivery := "joined the running turn"
		if startsTurn {
			delivery = "started a turn"
			if unconfirmed {
				delivery = "started a turn, this provider's probe for the window"
			}
		}
		rec := domain.ActionRecord{
			Kind: domain.ActionResetNotice, Bucket: n.Key.String(), ThreadID: n.ThreadID,
			Detail: fmt.Sprintf("%s: the %s window this thread was %sed for reset at %s per provider metadata; %s",
				thread.Title, n.LimitName, n.Kind, n.ResetsAt.In(now.Location()).Format(time.RFC3339), delivery),
		}
		if err := d.control.WarnThread(ctx, thread, domain.Warning{
			Kind: domain.ActionResetNotice, Text: resetNoticeText(n, st, haveState, startsTurn, now),
		}); err != nil {
			log.Error("send quota reset notice", "err", err)
			rec.Err = err.Error()
		}
		d.record(ctx, rec)
		log.Info("quota reset delivered", "kind", string(n.Kind), "delivery", delivery,
			"resets_at", n.ResetsAt.UTC().Format(time.RFC3339))
	}
}

// rolloutContradicted reports why the rollout for this window must stop, or
// an empty string when nothing contradicts it.
//
// Only a reading taken after the window's reset can contradict it, and the
// rollout is what produces such readings: the first turn it starts is the
// probe. A window that did not really reset therefore stops the rollout
// after one thread rather than after all of them, which is the difference
// between pacing a broadcast and not making one.
func (d *Daemon) rolloutContradicted(n domain.QuotaResetNotice, st domain.BucketState, haveState bool) string {
	if !haveState || !st.ObservedAt.After(n.ResetsAt) {
		return ""
	}
	observed := st.ObservedAt.UTC().Format(time.RFC3339)
	if st.Phase != domain.PhaseNormal {
		return fmt.Sprintf("the reading at %s puts %s in phase %s at %.0f%%", observed, st.Key, st.Phase, st.UsedPercent)
	}
	if warn := d.engineFor(st.Key, st.LimitName).Thresholds().WarnPercent; st.UsedPercent >= warn {
		return fmt.Sprintf("the reading at %s puts %s at %.0f%%, at or above the %.0f%% warn threshold",
			observed, st.Key, st.UsedPercent, warn)
	}
	return ""
}

// resumeIntentStatus maps every thread with a resume intent to its status,
// including the terminal ones: an intent that died without resuming is the
// signal that the advisory is due after all.
func (d *Daemon) resumeIntentStatus(ctx context.Context) (map[string]domain.ResumeStatus, error) {
	intents, err := d.store.ListResumeIntents(ctx)
	if err != nil {
		return nil, err
	}
	status := make(map[string]domain.ResumeStatus, len(intents))
	for _, intent := range intents {
		status[intent.ThreadID] = intent.Status
	}
	return status, nil
}

// alreadyRestarted reports why this thread needs nothing further, or an
// empty string when it is still owed its delivery.
//
// Only a thread automatic resume has already restarted, or is restarting
// right now, is excluded: starting a second turn for it would be the one
// duplicate this path must never produce. A thread whose intent is merely
// pending or eligible is not excluded. Waiting for the resume path to act
// was what left a voluntarily paused session with nothing watching it, and
// the delivery below is itself the restart, made on the same pacing and
// registered as the same probe the resume path would have used.
//
// resume.coordinator_threads_only takes no part in this. The resume path
// ignores it (it is kept for configuration compatibility, because T3 has no
// child threads and every thread is a coordinator thread in that sense), so
// reading it here would claim an exclusion the resume path does not honour.
// The interactive and unattended distinction the flag is sometimes read as
// making comes from the dispatched-thread registry instead, above.
func (d *Daemon) alreadyRestarted(threadID string, intents map[string]domain.ResumeStatus) string {
	if !d.cfg.Resume.Enabled {
		return ""
	}
	switch intents[threadID] {
	case domain.ResumeResuming, domain.ResumeResumed:
		return "automatic resume already restarted this thread"
	default:
		return ""
	}
}

// skipResetNotice settles an advisory that must not be sent, so that it is
// not reconsidered on every later pass.
func (d *Daemon) skipResetNotice(ctx context.Context, n domain.QuotaResetNotice, now time.Time, reason string) {
	settled, err := d.store.SettleQuotaResetNotice(ctx, n.ThreadID, n.Key, n.Epoch, now, "skipped: "+reason)
	if err != nil {
		d.log.Error("settle quota reset notice", "thread", n.ThreadID, "err", err)
		return
	}
	if !settled {
		return
	}
	d.log.Info("quota reset notice not sent", "thread", n.ThreadID, "bucket", n.Key.String(), "reason", reason)
	d.record(ctx, domain.ActionRecord{Kind: domain.ActionResetNotice, Bucket: n.Key.String(), ThreadID: n.ThreadID,
		Detail: "not sent: " + reason})
}

// noticeSlotFree reports whether the rollout may start another turn on this
// provider instance now. It observes the resume path's own dispatches as
// well: both mechanisms start turns against the same quota, and the point of
// the pacing is that one provider gets one new turn at a time.
func (d *Daemon) noticeSlotFree(provider string, now time.Time) bool {
	gap := d.cfg.ResetNotice.IntervalBetweenThreads.D()
	if gap <= 0 {
		return true
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, last := range []time.Time{d.lastNotice[provider], d.lastResume[provider]} {
		if !last.IsZero() && now.Before(last.Add(gap)) {
			return false
		}
	}
	return true
}

// takeNoticeSlot records a delivery that started a turn. It counts as a
// dispatch for the resume path's stagger too, so a resume does not start a
// second turn on the same provider inside the same interval. When no reading
// has confirmed the window yet it is also registered as that provider's
// probe for this reset: the turn just started produces exactly the reading
// the probe exists to obtain, and the resume path must not start another
// one for the same purpose.
func (d *Daemon) takeNoticeSlot(provider string, resetsAt time.Time, probe bool, now time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastNotice[provider] = now
	d.lastResume[provider] = now
	if probe {
		d.probed[provider] = resetsAt
	}
}

// resetNoticeRetention is how long notice rows are kept. An advisory held
// while automatic resume owns its thread has to outlive that intent, or the
// deferral would itself become the silence this feature ends, so the window
// covers resume.max_intent_age.
func (d *Daemon) resetNoticeRetention() time.Duration {
	keep := 7 * 24 * time.Hour
	if d.cfg.Resume.Enabled {
		if outlive := d.cfg.Resume.MaxIntentAge.D() + 24*time.Hour; outlive > keep {
			keep = outlive
		}
	}
	return keep
}

// recordNoticeDelivery remembers that a thread was told about a bucket, so
// that the advisory owed to it when that window resets survives a restart.
func (d *Daemon) recordNoticeDelivery(ctx context.Context, t domain.Thread, kind domain.ActionKind, state domain.BucketState, now time.Time) {
	if !d.cfg.ResetNotice.Enabled || state.ResetsAt == nil || state.Epoch == "" {
		// Without a reported reset time there is no window whose end the
		// clock could recognise, and nothing honest to say later.
		return
	}
	limit := state.LimitName
	if limit == "" {
		limit = state.Key.String()
	}
	if err := d.store.RecordQuotaNoticeDelivery(ctx, domain.QuotaResetNotice{
		ThreadID:          t.ID,
		Key:               state.Key,
		Epoch:             state.Epoch,
		Kind:              kind,
		LimitName:         limit,
		Threshold:         d.noticeThreshold(state, kind),
		UsedPercent:       state.UsedPercent,
		WarnedAt:          now,
		ResetsAt:          *state.ResetsAt,
		StoppedByWatchdog: kind == domain.ActionStop,
	}); err != nil {
		d.log.Error("record quota notice delivery", "thread", t.ID, "err", err)
	}
}

// noticeThreshold is the configured percentage the notice fired at.
func (d *Daemon) noticeThreshold(state domain.BucketState, kind domain.ActionKind) float64 {
	t := d.engineFor(state.Key, state.LimitName).Thresholds()
	switch kind {
	case domain.ActionDrain:
		return t.DrainPercent
	case domain.ActionStop:
		return t.StopPercent
	default:
		return t.WarnPercent
	}
}

// resetNoticeText renders the delivery. It states what the steward knows,
// how it knows it and what it does not know, because the difference is
// exactly what the recipient's next move rests on. When the delivery is
// itself the turn the recipient is now taking, it says so plainly: an agent
// should not have to work out whether it has been invited back or merely
// informed.
func resetNoticeText(n domain.QuotaResetNotice, st domain.BucketState, haveState, startsTurn bool, now time.Time) string {
	var b strings.Builder
	if startsTurn {
		b.WriteString("Quota window reset (T3 steward). This message starts a turn: you may carry on.\n\n")
	} else {
		b.WriteString("Quota window reset (T3 steward, advisory).\n\n")
	}
	fmt.Fprintf(&b, "%s at %s because %q (%s) was at %.0f%%, against the %.0f%% %s threshold. "+
		"The provider reported that window as resetting at %s, and that time has now passed (%s ago).\n\n",
		noticeOpening(n.Kind), n.WarnedAt.In(now.Location()).Format("2006-01-02 15:04 MST"),
		n.LimitName, n.Key, n.UsedPercent, n.Threshold, n.Kind,
		n.ResetsAt.In(now.Location()).Format("2006-01-02 15:04 MST"),
		humanDuration(now.Sub(n.ResetsAt).Round(time.Minute)))
	b.WriteString(resetNoticeEvidence(n, st, haveState, now))
	b.WriteString("\n\n")
	if startsTurn {
		b.WriteString("This turn is yours: if you paused or were stopped with work left, you can pick it up now, " +
			"starting by checking the current state rather than assuming your earlier subagents are still alive. " +
			"Nothing has been confirmed on your behalf and no capacity has been granted; the first reading this " +
			"turn produces is the first real evidence about the new window. If the window has not in fact reset, " +
			"that reading will show it and the watchdog will warn you, ask you to drain, or stop this thread " +
			"again on it, exactly as before.")
	} else {
		b.WriteString("This message joins the turn you are already taking and is advisory. It grants no capacity, " +
			"and nothing has been resumed on your behalf. The watchdog keeps its thresholds and will warn, ask " +
			"you to drain, or stop this thread again from the new window's readings, so if the window has not in " +
			"fact reset, the next reading will show it and the watchdog will act on that.")
	}
	return b.String()
}

func noticeOpening(kind domain.ActionKind) string {
	switch kind {
	case domain.ActionDrain:
		return "You were asked to drain"
	case domain.ActionStop:
		return "This thread was stopped by the quota watchdog"
	default:
		return "You were warned"
	}
}

// resetNoticeEvidence describes the reading situation without overstating
// it: the reset above is provider metadata, not a measurement.
func resetNoticeEvidence(n domain.QuotaResetNotice, st domain.BucketState, haveState bool, now time.Time) string {
	if haveState && st.ObservedAt.After(n.ResetsAt) {
		return fmt.Sprintf("A reading taken %s ago, after that reset, puts the window at %.0f%%.",
			humanDuration(now.Sub(st.ObservedAt).Round(time.Minute)), st.UsedPercent)
	}
	age := "older still"
	if haveState && !st.ObservedAt.IsZero() {
		age = humanDuration(now.Sub(st.ObservedAt).Round(time.Minute)) + " old"
	}
	return fmt.Sprintf("No fresh reading has confirmed the new window. The steward only sees usage while a turn "+
		"runs, and the newest reading for this bucket is %s, from the window that ended. The reset above is the "+
		"provider's own metadata, not a measurement of the new window.", age)
}
