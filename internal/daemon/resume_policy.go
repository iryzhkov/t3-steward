package daemon

import (
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The functions in this file are the watchdog's quota rules with the daemon's
// state factored out, so that the worker runtime on the same host can apply
// the same rules to the threads it owns. The daemon calls them too; there is
// one copy of each rule.

// GoverningPause reports the bucket that requires a thread with the given
// provider and model to drain or stop right now, when one does. It is the
// evaluation pollThreads applies to running threads: a bucket in the draining
// or stopped phase whose window has not expired and is not ignored. The most
// severe phase wins when several buckets apply.
func GoverningPause(cfg config.Config, thread domain.Thread, states []domain.BucketState, now time.Time) (domain.BucketState, bool) {
	var governing domain.BucketState
	found := false
	for _, st := range states {
		if st.Phase != domain.PhaseDraining && st.Phase != domain.PhaseStopped {
			continue
		}
		if ignoredWindowIn(cfg.Policy.IgnoreWindows, st.Key.Window) || !thread.MatchesBucket(st.Key, st.ModelSelector) {
			continue
		}
		if st.ResetsAt != nil && !st.ResetsAt.After(now) {
			// The window passed; a fresh snapshot has to rearm or re-stop it.
			continue
		}
		if !found || st.Phase.Rank() > governing.Phase.Rank() {
			governing = st
			found = true
		}
	}
	return governing, found
}

// BucketsRecovered applies the resume eligibility rules: every bucket that
// caused the stop must have rearmed after the stop, be in the normal phase
// below resume.below_percent and past the settle delay, and every other bucket
// that applies to the thread must be healthy. probe, when not nil, may allow a
// bucket whose reset time has passed without a confirming reading to be
// probed by resuming; see ProbeWindowOpen.
func BucketsRecovered(cfg config.Config, stopped []domain.BucketKey, stoppedAt time.Time, thread domain.Thread, byKey map[domain.BucketKey]domain.BucketState, now time.Time, probe func(domain.BucketState) bool) (bool, string) {
	for _, key := range stopped {
		st, ok := byKey[key]
		if !ok {
			return false, fmt.Sprintf("no state for %s", key)
		}
		if st.RecoveredAt == nil || !st.RecoveredAt.After(stoppedAt) {
			// No fresh reading has confirmed the reset. Readings only come
			// from running turns, so after the reset time has passed by
			// probe_after_reset one thread per provider is resumed as a
			// probe; its first call yields the reading that rearms the
			// bucket, or gets it stopped again at once.
			if probe != nil && probe(st) {
				continue
			}
			return false, fmt.Sprintf("%s has not recovered since the stop", key)
		}
		if st.Phase != domain.PhaseNormal {
			return false, fmt.Sprintf("%s is in phase %s", key, st.Phase)
		}
		if st.UsedPercent >= cfg.Resume.BelowPercent {
			return false, fmt.Sprintf("%s is at %.0f%%, above below_percent %.0f%%", key, st.UsedPercent, cfg.Resume.BelowPercent)
		}
		// RecoveredAt is only ever set while evaluating a fresh provider
		// snapshot, so reset_confirmation_required holds by construction:
		// the wall clock passing resetsAt never sets it.
		if now.Before(st.RecoveredAt.Add(cfg.Resume.ResetSettleDelay.D())) {
			return false, fmt.Sprintf("%s recovered at %s; waiting for the settle delay", key, st.RecoveredAt.Format(time.RFC3339))
		}
	}
	return ApplicableHealthy(cfg, thread, byKey, now)
}

// ProbeWindowOpen reports whether a bucket's reset time has passed by
// resume.probe_after_reset plus the settle delay with no reading newer than
// the reset. The daemon additionally limits probes to one per provider and
// reset; a caller without that bookkeeping probes with at most one thread.
func ProbeWindowOpen(cfg config.Config, st domain.BucketState, now time.Time) bool {
	probe := cfg.Resume.ProbeAfterReset.D()
	if probe <= 0 || st.ResetsAt == nil {
		return false
	}
	if now.Before(st.ResetsAt.Add(probe + cfg.Resume.ResetSettleDelay.D())) {
		return false
	}
	// A reading newer than the reset would have rearmed or re-stopped the
	// bucket; if one exists the probe is not needed.
	return !st.ObservedAt.After(*st.ResetsAt)
}

// ApplicableHealthy requires every bucket that applies to the thread to be
// in the normal phase below the warning threshold.
func ApplicableHealthy(cfg config.Config, thread domain.Thread, byKey map[domain.BucketKey]domain.BucketState, now time.Time) (bool, string) {
	for key, st := range byKey {
		if ignoredWindowIn(cfg.Policy.IgnoreWindows, key.Window) {
			continue
		}
		if !thread.MatchesBucket(key, st.ModelSelector) {
			continue
		}
		if st.ResetsAt != nil && !st.ResetsAt.After(now) {
			// Window expired with no fresh data: whatever phase it was in
			// belongs to the old window, so it is not a blocker.
			continue
		}
		if !st.Healthy {
			return false, fmt.Sprintf("%s is %s at %.0f%%", key, st.Phase, st.UsedPercent)
		}
	}
	return true, ""
}
