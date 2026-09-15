package daemon

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// deliverResetNotices tells each thread, exactly once, that a quota window it
// was warned or stopped for has passed the reset time the provider itself
// reported.
//
// The pass is driven by the clock on purpose. Readings arrive only from
// running turns, so a thread that stopped after a warning is never told
// anything again unless something else speaks first. The clock is enough to
// justify saying so and nothing more: this path never writes RecoveredAt,
// never touches a resume intent and never makes one eligible. The resume
// decision keeps its evidence requirement and its probe.
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
	byID := map[string]domain.Thread{}
	for _, t := range threads {
		byID[t.ID] = t
	}
	byKey := map[domain.BucketKey]domain.BucketState{}
	for _, st := range states {
		byKey[st.Key] = st
	}
	for _, n := range pending {
		log := d.log.With("thread", n.ThreadID, "bucket", n.Key.String())
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
		}
		// A message to a running thread joins the turn it is already taking.
		// A message to an idle thread starts one, so those are staggered per
		// provider instance.
		if !thread.Running && !d.noticeSlotFree(n.Key.ProviderInstanceID, now) {
			log.Debug("quota reset notice held for the stagger interval")
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
		if !thread.Running {
			d.takeNoticeSlot(n.Key.ProviderInstanceID, now)
		}
		st, haveState := byKey[n.Key]
		rec := domain.ActionRecord{
			Kind: domain.ActionResetNotice, Bucket: n.Key.String(), ThreadID: n.ThreadID,
			Detail: fmt.Sprintf("%s: the %s window this thread was %sed for reset at %s per provider metadata",
				thread.Title, n.LimitName, n.Kind, n.ResetsAt.In(now.Location()).Format(time.RFC3339)),
		}
		if err := d.control.WarnThread(ctx, thread, domain.Warning{
			Kind: domain.ActionResetNotice, Text: resetNoticeText(n, st, haveState, now),
		}); err != nil {
			log.Error("send quota reset notice", "err", err)
			rec.Err = err.Error()
		}
		d.record(ctx, rec)
		log.Info("quota reset notice delivered", "kind", string(n.Kind), "resets_at", n.ResetsAt.UTC().Format(time.RFC3339))
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

// noticeSlotFree reports whether another advisory may start a turn on this
// provider instance now.
func (d *Daemon) noticeSlotFree(provider string, now time.Time) bool {
	gap := d.cfg.ResetNotice.IntervalBetweenThreads.D()
	if gap <= 0 {
		return true
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	last, ok := d.lastNotice[provider]
	return !ok || !now.Before(last.Add(gap))
}

func (d *Daemon) takeNoticeSlot(provider string, now time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastNotice[provider] = now
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

// resetNoticeText renders the advisory. It states what the steward knows,
// how it knows it and what it does not know, because the difference is
// exactly what the recipient's decision rests on.
func resetNoticeText(n domain.QuotaResetNotice, st domain.BucketState, haveState bool, now time.Time) string {
	var b strings.Builder
	b.WriteString("Quota window reset (T3 steward, advisory).\n\n")
	fmt.Fprintf(&b, "%s at %s because %q (%s) was at %.0f%%, against the %.0f%% %s threshold. "+
		"The provider reported that window as resetting at %s, and that time has now passed (%s ago).\n\n",
		noticeOpening(n.Kind), n.WarnedAt.In(now.Location()).Format("2006-01-02 15:04 MST"),
		n.LimitName, n.Key, n.UsedPercent, n.Threshold, n.Kind,
		n.ResetsAt.In(now.Location()).Format("2006-01-02 15:04 MST"),
		humanDuration(now.Sub(n.ResetsAt).Round(time.Minute)))
	b.WriteString(resetNoticeEvidence(n, st, haveState, now))
	b.WriteString("\n\nThis message is advisory. It grants no capacity and resumes nothing, and nothing has been " +
		"resumed on your behalf. Decide for yourself whether to continue; the watchdog keeps its thresholds " +
		"and will warn, ask you to drain, or stop this thread again from the new window's readings.")
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
