package daemon

import (
	"context"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// advanceResumes cancels stale intents, marks eligible ones, and resumes
// them one provider instance at a time with staggering. An intent for a
// thread a live attempt owns is cancelled: that thread belongs to a steward
// attempt and the worker runtime decides when it resumes. An intent for a
// thread whose attempt has settled is cancelled too: nothing would use the
// resumed turn.
func (d *Daemon) advanceResumes(ctx context.Context, threads []domain.Thread, states []domain.BucketState, owners ThreadOwners) {
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
		if attemptID, ownedThread := owners.Live[intent.ThreadID]; ownedThread {
			// Evaluated on every tick, so an intent recorded before ownership
			// was consulted, or before the attempt claimed the thread, is
			// cancelled on the first tick that sees the ownership.
			cancel(OwnedThreadReason(attemptID))
			continue
		}
		if attemptID, settled := owners.Settled[intent.ThreadID]; settled {
			// The intent predates this rule (the watchdog no longer stops
			// owned threads) and its attempt has since failed, completed or
			// been cancelled: a one-time upgrade hazard, closed here.
			cancel(SettledThreadReason(attemptID))
			continue
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
		//
		// Only a user message counts as the user taking the thread over. A
		// turn that starts without one is the harness continuing on its own:
		// a background task or subagent finishing wakes the thread for a
		// turn that lasts milliseconds, and an interrupted turn is often
		// followed by such a turn within one poll. Cancelling on the turn id
		// alone made every stop of a thread with background work cancel its
		// own resume before anyone had acted.
		//
		// The watchdog's own warn and drain messages are user messages too,
		// hence the tolerance around the stop time.
		case !dry && thread.LatestUserMessageAt != nil && thread.LatestUserMessageAt.After(intent.StoppedAt.Add(userMessageTolerance)):
			cancel("a user message arrived after the watchdog stop")
			continue
		case !dry && thread.Running:
			// Still running, in the drained turn or in a turn the harness
			// started on its own: wait for it to stop. A turn started while
			// the bucket is still stopped is stopped again by pollThreads.
			log.Debug("resume deferred: thread still running")
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
		// Remember a probe so only one thread per provider tests a reset
		// that no reading has confirmed yet.
		for _, key := range intent.Buckets {
			if st, ok := byKey[key]; ok && st.ResetsAt != nil && !st.ObservedAt.After(*st.ResetsAt) && now.After(*st.ResetsAt) {
				d.probed[key.ProviderInstanceID] = *st.ResetsAt
			}
		}
	}
}

// resumeEligible checks every bucket that caused the stop and every other
// applicable bucket.
func (d *Daemon) resumeEligible(intent domain.ResumeIntent, thread domain.Thread, byKey map[domain.BucketKey]domain.BucketState, now time.Time) (bool, string) {
	return BucketsRecovered(d.cfg, intent.Buckets, intent.StoppedAt, thread, byKey, now, func(st domain.BucketState) bool {
		return d.probeAllowed(st, now)
	})
}

// probeAllowed reports whether a bucket whose reset time has passed without
// a fresh reading may be probed by resuming one thread.
func (d *Daemon) probeAllowed(st domain.BucketState, now time.Time) bool {
	if !ProbeWindowOpen(d.cfg, st, now) {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	// One probe per provider per reset.
	if last, ok := d.probed[st.Key.ProviderInstanceID]; ok && last.Equal(*st.ResetsAt) {
		return false
	}
	return true
}

// applicableHealthy requires every bucket that applies to the thread to be
// in the normal phase below the warning threshold.
func (d *Daemon) applicableHealthy(thread domain.Thread, byKey map[domain.BucketKey]domain.BucketState) (bool, string) {
	return ApplicableHealthy(d.cfg, thread, byKey, d.now())
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
			d.notifier.Send(ctx, "T3 steward: thread resumed", thread.Title)
		}
	}
	intent.UpdatedAt = now
	if err := d.store.SaveResumeIntent(ctx, intent); err != nil {
		log.Error("save resume intent", "err", err)
	}
	d.record(ctx, rec)
}
