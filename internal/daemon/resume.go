package daemon

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/iryzhkov/t3-quota-watchdog/internal/domain"
)

// advanceResumes cancels stale intents, marks eligible ones, and resumes
// them one provider instance at a time with staggering.
func (d *Daemon) advanceResumes(ctx context.Context, threads []domain.Thread, states []domain.BucketState) {
	intents, err := d.store.ListResumeIntents(ctx, domain.ResumePending, domain.ResumeEligible)
	if err != nil {
		d.log.Error("list resume intents", "err", err)
		return
	}
	if len(intents) == 0 {
		return
	}
	now := d.now()
	dry := d.cfg.Policy.DryRun
	byID := map[string]domain.Thread{}
	for _, t := range threads {
		byID[t.ID] = t
	}
	byKey := map[domain.BucketKey]domain.BucketState{}
	for _, st := range states {
		byKey[st.Key] = st
	}

	var candidates []domain.ResumeIntent
	for _, intent := range intents {
		log := d.log.With("thread", intent.ThreadID)
		thread, ok := byID[intent.ThreadID]
		cancel := func(reason string) {
			intent.Status = domain.ResumeCancelled
			intent.Reason = reason
			intent.UpdatedAt = now
			log.Info("resume intent cancelled", "reason", reason)
			if err := d.store.SaveResumeIntent(ctx, intent); err != nil {
				log.Error("save resume intent", "err", err)
			}
			d.record(ctx, domain.ActionRecord{Kind: domain.ActionResume, ThreadID: intent.ThreadID, Detail: "cancelled: " + reason})
		}
		switch {
		case !ok:
			cancel("thread deleted or no longer listed")
			continue
		case thread.ArchivedAt != nil:
			cancel("thread archived")
			continue
		case now.Sub(intent.StoppedAt) > d.cfg.Resume.MaxIntentAge.D():
			cancel("intent older than max_intent_age")
			continue
		// In dry-run mode nothing was stopped, so the thread is expected to
		// keep running; only real stops watch for manual interaction.
		case !dry && thread.TurnID != "" && intent.StoppedTurnID != "" && thread.TurnID != intent.StoppedTurnID:
			cancel("a new turn started after the watchdog stop")
			continue
		// The watchdog's own warn and drain messages are user messages too,
		// hence the tolerance around the stop time.
		case !dry && thread.LatestUserMessageAt != nil && thread.LatestUserMessageAt.After(intent.StoppedAt.Add(10*time.Second)):
			cancel("a user message arrived after the watchdog stop")
			continue
		case !dry && thread.Running:
			// Same turn, still running: a thread finishing its checkpoint
			// after the drain request. Wait for it to stop.
			log.Debug("resume deferred: thread still running its drained turn")
			continue
		}
		if !d.cfg.Resume.Enabled {
			continue
		}
		if thread.HasPendingApprovals || thread.HasPendingUserInput {
			log.Debug("resume deferred: thread waits for user input")
			continue
		}
		eligible, why := d.resumeEligible(intent, thread, byKey, now)
		if !eligible {
			log.Debug("resume not eligible yet", "reason", why)
			if intent.Status == domain.ResumeEligible {
				intent.Status = domain.ResumePending
				intent.UpdatedAt = now
				_ = d.store.SaveResumeIntent(ctx, intent)
			}
			continue
		}
		if intent.Status != domain.ResumeEligible {
			intent.Status = domain.ResumeEligible
			intent.UpdatedAt = now
			if err := d.store.SaveResumeIntent(ctx, intent); err != nil {
				log.Error("save resume intent", "err", err)
				continue
			}
			log.Info("resume intent eligible")
		}
		candidates = append(candidates, intent)
	}
	if len(candidates) == 0 {
		return
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].StoppedAt.Before(candidates[j].StoppedAt) })

	d.mu.Lock()
	defer d.mu.Unlock()
	launched := map[string]int{}
	for _, intent := range candidates {
		provider := intent.ProviderInstanceID
		if launched[provider] >= d.cfg.Resume.MaxConcurrentPerProvider {
			continue
		}
		if last, ok := d.lastResume[provider]; ok && now.Sub(last) < d.cfg.Resume.IntervalBetweenThreads.D() {
			continue
		}
		// Recheck every applicable bucket immediately before resuming;
		// abort the batch when any of them reached the warning level.
		thread := byID[intent.ThreadID]
		if ok, why := d.applicableHealthy(thread, byKey); !ok {
			d.log.Warn("resume batch stopped: quota rising again", "reason", why)
			return
		}
		d.resumeOne(ctx, intent, thread, now)
		launched[provider]++
		d.lastResume[provider] = now
	}
}

// resumeEligible checks every bucket that caused the stop and every other
// applicable bucket.
func (d *Daemon) resumeEligible(intent domain.ResumeIntent, thread domain.Thread, byKey map[domain.BucketKey]domain.BucketState, now time.Time) (bool, string) {
	for _, key := range intent.Buckets {
		st, ok := byKey[key]
		if !ok {
			return false, fmt.Sprintf("no state for %s", key)
		}
		if st.RecoveredAt == nil || !st.RecoveredAt.After(intent.StoppedAt) {
			return false, fmt.Sprintf("%s has not recovered since the stop", key)
		}
		if st.Phase != domain.PhaseNormal {
			return false, fmt.Sprintf("%s is in phase %s", key, st.Phase)
		}
		if st.UsedPercent >= d.cfg.Resume.BelowPercent {
			return false, fmt.Sprintf("%s is at %.0f%%, above below_percent %.0f%%", key, st.UsedPercent, d.cfg.Resume.BelowPercent)
		}
		// RecoveredAt is only ever set while evaluating a fresh provider
		// snapshot, so reset_confirmation_required holds by construction:
		// the wall clock passing resetsAt never sets it.
		if now.Before(st.RecoveredAt.Add(d.cfg.Resume.ResetSettleDelay.D())) {
			return false, fmt.Sprintf("%s recovered at %s; waiting for the settle delay", key, st.RecoveredAt.Format(time.RFC3339))
		}
	}
	return d.applicableHealthy(thread, byKey)
}

// applicableHealthy requires every bucket that applies to the thread to be
// in the normal phase below the warning threshold.
func (d *Daemon) applicableHealthy(thread domain.Thread, byKey map[domain.BucketKey]domain.BucketState) (bool, string) {
	for key, st := range byKey {
		if d.ignoredWindow(key.Window) {
			continue
		}
		if !thread.MatchesBucket(key, st.ModelSelector) {
			continue
		}
		if st.ResetsAt != nil && !st.ResetsAt.After(d.now()) && st.Phase == domain.PhaseNormal {
			// Window expired with no fresh data: not a blocker.
			continue
		}
		if !st.Healthy {
			return false, fmt.Sprintf("%s is %s at %.0f%%", key, st.Phase, st.UsedPercent)
		}
	}
	return true, ""
}

func (d *Daemon) resumeOne(ctx context.Context, intent domain.ResumeIntent, thread domain.Thread, now time.Time) {
	log := d.log.With("thread", intent.ThreadID, "title", thread.Title)
	rec := domain.ActionRecord{Kind: domain.ActionResume, ThreadID: intent.ThreadID, Detail: "resume: " + thread.Title}
	if !d.ControlAllowed {
		log.Info("resume skipped: control disabled", "reason", d.ControlReason)
		return
	}
	intent.Status = domain.ResumeResuming
	intent.UpdatedAt = now
	if err := d.store.SaveResumeIntent(ctx, intent); err != nil {
		log.Error("save resume intent", "err", err)
		return
	}
	err := d.control.ResumeThread(ctx, thread, d.cfg.Resume.Prompt)
	if err != nil {
		log.Error("resume thread", "err", err)
		rec.Err = err.Error()
		intent.Status = domain.ResumeFailed
		intent.Reason = err.Error()
	} else {
		log.Info("thread resumed")
		intent.Status = domain.ResumeResumed
		intent.ResumedAt = &now
		if !d.cfg.Policy.DryRun {
			d.notifier.Send(ctx, "T3 quota watchdog: thread resumed", thread.Title)
		}
	}
	intent.UpdatedAt = now
	if err := d.store.SaveResumeIntent(ctx, intent); err != nil {
		log.Error("save resume intent", "err", err)
	}
	d.record(ctx, rec)
}
