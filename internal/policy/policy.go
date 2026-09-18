// Package policy is the pure quota state machine. It knows nothing about
// WebSockets, log files or T3 command names: it turns a snapshot plus the
// previous bucket state into a new state and a list of actions.
//
// One state machine exists per bucket and reset epoch:
//
//	normal -> warned -> draining -> stopped -> (reset) -> normal
package policy

import (
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Thresholds configures the engine.
type Thresholds struct {
	WarnPercent  float64
	DrainPercent float64
	StopPercent  float64
	// RearmPercent is the usage below which a bucket may rearm. With a reset
	// time it is checked once the window has passed; without one it must hold
	// for RearmObservations consecutive observations.
	RearmPercent float64
	GracePeriod  time.Duration
	// RearmObservations is the number of consecutive observations below
	// RearmPercent needed to rearm a bucket that reports no reset time.
	RearmObservations int
	// ResetTolerance absorbs the jitter some providers add to resetsAt between
	// two events of the same window. A reset time that moves forward by more
	// than this is treated as a new window.
	ResetTolerance time.Duration
	// RateWindow is how far back readings are used to estimate the burn
	// rate. Zero disables rate-based escalation.
	RateWindow time.Duration
	// WarnETA, DrainETA and StopETA escalate when the projected time to
	// exhaustion at the current rate falls below them, provided the window
	// does not reset first.
	WarnETA, DrainETA, StopETA time.Duration
	// ResetExemption suppresses every escalation when the window resets
	// within this long: stopping then saves nothing.
	ResetExemption time.Duration
	// RunwayMargin is how many times the time to the reset the projected
	// runway must cover before the percentage ladder is ignored. One means
	// an exhaustion projected after the reset is never a reason to act.
	RunwayMargin float64
	// HardStopETA is the only projection that stops a thread whose usage is
	// still below StopPercent: the provider ends the session at 100% by
	// itself, so an interrupt is worth its cost only in the last minutes.
	// Above it the projection asks for a drain at most. Zero uses
	// DefaultHardStopETA.
	HardStopETA time.Duration
}

// DefaultHardStopETA is the projected time to exhaustion under which a hard
// stop fires below the stop threshold.
const DefaultHardStopETA = 2 * time.Minute

// DefaultThresholds are the shipped defaults.
func DefaultThresholds() Thresholds {
	return Thresholds{
		WarnPercent:       85,
		DrainPercent:      90,
		StopPercent:       95,
		RearmPercent:      50,
		GracePeriod:       60 * time.Second,
		RearmObservations: 2,
		ResetTolerance:    5 * time.Minute,
		RateWindow:        10 * time.Minute,
		WarnETA:           30 * time.Minute,
		DrainETA:          15 * time.Minute,
		StopETA:           5 * time.Minute,
		ResetExemption:    10 * time.Minute,
		RunwayMargin:      1,
		HardStopETA:       DefaultHardStopETA,
	}
}

func (e *Engine) hardStopETA() time.Duration {
	if e.t.HardStopETA <= 0 {
		return DefaultHardStopETA
	}
	return e.t.HardStopETA
}

// Validate rejects thresholds that cannot form a monotonic ladder.
func (t Thresholds) Validate() error {
	if !(t.WarnPercent < t.DrainPercent && t.DrainPercent < t.StopPercent && t.StopPercent <= 100) {
		return fmt.Errorf("thresholds must satisfy warn < drain < stop <= 100 (got warn=%v drain=%v stop=%v)",
			t.WarnPercent, t.DrainPercent, t.StopPercent)
	}
	if t.WarnPercent <= 0 {
		return fmt.Errorf("warn_percent must be positive")
	}
	if t.RearmPercent <= 0 || t.RearmPercent >= t.WarnPercent {
		return fmt.Errorf("rearm_percent must be between 0 and warn_percent (got %v)", t.RearmPercent)
	}
	if t.GracePeriod < 0 {
		return fmt.Errorf("grace_period must not be negative")
	}
	if t.RearmObservations < 1 {
		return fmt.Errorf("rearm_observations must be at least 1")
	}
	if t.RateWindow > 0 && !(t.StopETA < t.DrainETA && t.DrainETA < t.WarnETA) {
		return fmt.Errorf("stop_eta < drain_eta < warn_eta is required (got %s, %s, %s)", t.StopETA, t.DrainETA, t.WarnETA)
	}
	if t.ResetExemption < 0 {
		return fmt.Errorf("reset_exemption must not be negative")
	}
	if t.RunwayMargin < 1 {
		return fmt.Errorf("runway_margin must be at least 1 (got %v)", t.RunwayMargin)
	}
	if t.HardStopETA < 0 {
		return fmt.Errorf("hard_stop_eta must not be negative")
	}
	return nil
}

// ExtensionMargin is how many percentage points below the warn threshold a
// reading must land, after dropping since the previous reading, for the
// engine to conclude that the provider extended the quota mid-window and
// rearm the bucket without waiting for the reset time.
const ExtensionMargin = 5

// Engine evaluates snapshots against bucket states.
type Engine struct {
	t Thresholds
}

// New returns an engine for the given thresholds.
func New(t Thresholds) *Engine {
	return &Engine{t: t}
}

// Thresholds returns the engine's configuration.
func (e *Engine) Thresholds() Thresholds { return e.t }

// applied is the percentage ladder recorded on every state the engine
// writes, so a later start can tell which ladder produced the phase.
func (e *Engine) applied() *domain.ThresholdSet {
	return &domain.ThresholdSet{WarnPercent: e.t.WarnPercent, DrainPercent: e.t.DrainPercent, StopPercent: e.t.StopPercent}
}

// level maps a usage percentage to the phase it demands.
func (e *Engine) level(usedPercent float64) domain.Phase {
	switch {
	case usedPercent >= e.t.StopPercent:
		return domain.PhaseStopped
	case usedPercent >= e.t.DrainPercent:
		return domain.PhaseDraining
	case usedPercent >= e.t.WarnPercent:
		return domain.PhaseWarned
	default:
		return domain.PhaseNormal
	}
}

func (e *Engine) rateWindow() time.Duration {
	if e.t.RateWindow <= 0 {
		return 10 * time.Minute
	}
	return e.t.RateWindow
}

// burnRate estimates percent per minute from the readings inside the rate
// window and the time to exhaustion at that rate. Readings are whole
// percent and can wobble, so the rate is the rise from the oldest reading
// in the window to the highest reading, over that span; it needs at least
// two minutes of history and never goes negative.
func (e *Engine) burnRate(recent []domain.Reading, now time.Time) (float64, *time.Duration) {
	if e.t.RateWindow <= 0 || len(recent) < 2 {
		return 0, nil
	}
	cutoff := now.Add(-e.rateWindow())
	var oldest *domain.Reading
	high := recent[len(recent)-1]
	for i := range recent {
		r := recent[i]
		if r.At.Before(cutoff) {
			continue
		}
		if oldest == nil {
			oldest = &recent[i]
		}
		if r.Used > high.Used {
			high = r
		}
	}
	if oldest == nil {
		return 0, nil
	}
	span := high.At.Sub(oldest.At)
	if span < 2*time.Minute {
		// Too little history for a rate: use the whole window span so a
		// burst of readings a few seconds apart does not read as infinite.
		span = now.Sub(oldest.At)
		if span < 2*time.Minute {
			return 0, nil
		}
	}
	rise := high.Used - oldest.Used
	if rise <= 0 {
		return 0, nil
	}
	rate := rise / span.Minutes()
	remaining := 100 - high.Used
	if remaining <= 0 {
		d := time.Duration(0)
		return rate, &d
	}
	eta := time.Duration(remaining / rate * float64(time.Minute))
	return rate, &eta
}

func trimReadings(recent []domain.Reading, cutoff time.Time) []domain.Reading {
	i := 0
	for i < len(recent) && recent[i].At.Before(cutoff) {
		i++
	}
	if i == 0 {
		return recent
	}
	return append([]domain.Reading(nil), recent[i:]...)
}

// wantedLevel combines the percentage ladder with the exhaustion ladder and
// applies the reset exemption. It returns the level and the reason.
func (e *Engine) wantedLevel(snap domain.QuotaSnapshot, state *domain.BucketState, now time.Time) (domain.Phase, string) {
	var untilReset time.Duration
	if snap.ResetsAt != nil {
		untilReset = snap.ResetsAt.Sub(now)
		if e.t.ResetExemption > 0 && untilReset > 0 && untilReset <= e.t.ResetExemption {
			return domain.PhaseNormal, fmt.Sprintf("window resets in %s; nothing to save by stopping", untilReset.Round(time.Second))
		}
	}
	// With a known burn rate the question is runway, not percentage: if
	// the window would last to the reset with margin, nothing is gained by
	// stopping, however high the reading.
	if state.ExhaustsIn != nil && e.t.RateWindow > 0 && untilReset > 0 {
		if ok, why := e.runwayHolds(*state.ExhaustsIn, untilReset, state.RatePerMinute); ok {
			return domain.PhaseNormal, why
		}
	}
	level := e.level(snap.UsedPercent)
	why := fmt.Sprintf("threshold %.0f%%", e.thresholdFor(level))
	// The projection only tightens the ladder once usage is already in
	// warning territory; a burst early in a window is not a reason to
	// wind anything down. And it asks for a drain at most while the
	// percentage is below the stop threshold: a session that checkpoints
	// and stops on request loses nothing, an interrupted one loses its
	// subagents, and the provider ends the session at 100% anyway. Only a
	// projection under the hard-stop floor, when no drain can finish in
	// time, is a stop.
	if snap.UsedPercent >= e.t.WarnPercent && state.ExhaustsIn != nil && e.t.RateWindow > 0 && (snap.ResetsAt == nil || *state.ExhaustsIn < untilReset) {
		eta := *state.ExhaustsIn
		var etaLevel domain.Phase
		switch {
		case eta <= e.hardStopETA():
			etaLevel = domain.PhaseStopped
		case eta <= e.t.DrainETA:
			etaLevel = domain.PhaseDraining
		case eta <= e.t.WarnETA:
			etaLevel = domain.PhaseWarned
		}
		if etaLevel.Rank() > level.Rank() {
			// A single burst of readings can project a short ETA; require
			// the projection on two consecutive readings before acting.
			state.ETAStrikes++
			if state.ETAStrikes < 2 {
				return level, why
			}
			level = etaLevel
			resetNote := "no reset time reported"
			if snap.ResetsAt != nil {
				resetNote = fmt.Sprintf("window resets in %s", untilReset.Round(time.Minute))
			}
			why = fmt.Sprintf("burning %.1f%%/min, exhausted in about %s, %s", state.RatePerMinute, eta.Round(time.Minute), resetNote)
		}
	}
	return level, why
}

// runwayHolds reports whether the projected time to exhaustion covers the
// time to the reset with the configured margin. With the default margin of
// one, an exhaustion projected after the reset is no reason to act: the
// window ends before the quota does.
func (e *Engine) runwayHolds(eta, untilReset time.Duration, rate float64) (bool, string) {
	margin := e.t.RunwayMargin
	if margin <= 0 {
		margin = 1
	}
	if float64(eta) >= margin*float64(untilReset) {
		return true, fmt.Sprintf("burning %.2f%%/min, about %s of runway against %s to the reset; no need to stop",
			rate, eta.Round(time.Minute), untilReset.Round(time.Minute))
	}
	return false, ""
}

// Evaluate applies one snapshot to the previous state of the same bucket.
func (e *Engine) Evaluate(snap domain.QuotaSnapshot, prev domain.BucketState, now time.Time) domain.Decision {
	state := prev
	if state.Key == (domain.BucketKey{}) {
		state = domain.BucketState{Key: snap.Key, Phase: domain.PhaseNormal}
	}
	if snap.Key != state.Key {
		return domain.Decision{State: prev, Ignored: "snapshot is for a different bucket"}
	}
	if snap.SourceEventID != "" && snap.SourceEventID == state.LastEventID {
		return domain.Decision{State: prev, Ignored: "duplicate event"}
	}
	if !state.ObservedAt.IsZero() && snap.ObservedAt.Before(state.ObservedAt) {
		return domain.Decision{State: prev, Ignored: "out-of-order event"}
	}
	if snap.IsExpired(now) {
		return domain.Decision{State: prev, Ignored: "reset time already passed; snapshot describes an old window"}
	}

	var actions []domain.Action
	reset, reason := e.detectReset(snap, state)
	if reset {
		if state.Phase != domain.PhaseNormal {
			actions = append(actions, domain.Action{
				Kind: domain.ActionRearm, Bucket: snap.Key, Snapshot: snap, Reason: reason,
			})
		}
		t := now
		state.RecoveredAt = &t
		state.Phase = domain.PhaseNormal
		state.DrainDeadline = nil
		state.StoppedAt = nil
		state.RearmObservations = 0
	} else if state.Phase != domain.PhaseNormal && state.ResetsAt == nil {
		// No reset time: rearm only after usage stays low for several
		// consecutive observations.
		if snap.UsedPercent < e.t.RearmPercent {
			state.RearmObservations++
			if state.RearmObservations >= e.t.RearmObservations {
				actions = append(actions, domain.Action{
					Kind: domain.ActionRearm, Bucket: snap.Key, Snapshot: snap,
					Reason: fmt.Sprintf("usage stayed below %.0f%% for %d observations", e.t.RearmPercent, state.RearmObservations),
				})
				t := now
				state.RecoveredAt = &t
				state.Phase = domain.PhaseNormal
				state.DrainDeadline = nil
				state.StoppedAt = nil
				state.RearmObservations = 0
			}
		} else {
			state.RearmObservations = 0
		}
	}

	// Burn rate over the recent readings of this window.
	if reset || len(state.Recent) == 0 {
		state.Recent = nil
	}
	state.Recent = append(state.Recent, domain.Reading{At: snap.ObservedAt, Used: snap.UsedPercent})
	state.RatePerMinute, state.ExhaustsIn = e.burnRate(state.Recent, snap.ObservedAt)
	state.Recent = trimReadings(state.Recent, snap.ObservedAt.Add(-2*e.rateWindow()))

	// Escalate to the highest applicable level, from the percentage ladder
	// or from the projected time to exhaustion, unless the window resets
	// so soon that stopping would save nothing. Levels below the current
	// phase never fire again within the same epoch.
	strikesBefore := state.ETAStrikes
	want, why := e.wantedLevel(snap, &state, now)
	if state.ETAStrikes == strikesBefore {
		// This reading did not ask for a projection-based level.
		state.ETAStrikes = 0
	}
	// A warning is advisory; once usage is back below the warn threshold
	// with nothing asking for more, the bucket is normal again. Draining
	// and stopped never de-escalate within a window.
	if state.Phase == domain.PhaseWarned && want == domain.PhaseNormal && snap.UsedPercent < e.t.WarnPercent {
		state.Phase = domain.PhaseNormal
	}
	if want.Rank() > state.Phase.Rank() {
		var kind domain.ActionKind
		switch want {
		case domain.PhaseWarned:
			kind = domain.ActionWarn
		case domain.PhaseDraining:
			kind = domain.ActionDrain
		case domain.PhaseStopped:
			kind = domain.ActionStop
		}
		actions = append(actions, domain.Action{
			Kind: kind, Bucket: snap.Key, Snapshot: snap,
			Reason: fmt.Sprintf("%s at %.0f%%: %s", describe(snap), snap.UsedPercent, why),
		})
		state.Phase = want
		switch want {
		case domain.PhaseDraining:
			deadline := now.Add(e.t.GracePeriod)
			state.DrainDeadline = &deadline
		case domain.PhaseStopped:
			state.DrainDeadline = nil
			stoppedAt := now
			state.StoppedAt = &stoppedAt
		}
	}

	state.WindowDuration = snap.WindowDuration
	state.LimitName = snap.LimitName
	state.ModelSelector = snap.ModelSelector
	state.UsedPercent = snap.UsedPercent
	state.ResetsAt = snap.ResetsAt
	state.Epoch = domain.EpochFor(snap.ResetsAt)
	state.ObservedAt = snap.ObservedAt
	state.LastEventID = snap.SourceEventID
	state.UpdatedAt = now
	state.Healthy = state.Phase == domain.PhaseNormal && snap.UsedPercent < e.t.WarnPercent
	state.AppliedThresholds = e.applied()
	return domain.Decision{State: state, Actions: actions}
}

// Rederive recomputes a stored phase from the stored percentage under this
// engine's ladder, for the daemon to call at start: the thresholds may have
// changed since the phase was written, and on a host where nothing runs no
// reading will ever revisit it. Only the percentage ladder is consulted;
// there is no fresh reading, so no projection. A stored phase above what the
// ladder produces is lowered, with the stop and drain bookkeeping cleared and
// RecoveredAt set when the result is normal, and one rearm action names the
// stored and the new phase and both ladders. A phase is never raised: only a
// reading does that. The returned decision carries no actions when nothing
// changes.
func (e *Engine) Rederive(prev domain.BucketState, now time.Time) domain.Decision {
	if prev.Key == (domain.BucketKey{}) || prev.Phase == domain.PhaseNormal {
		return domain.Decision{State: prev}
	}
	want := e.level(prev.UsedPercent)
	if want.Rank() >= prev.Phase.Rank() {
		return domain.Decision{State: prev}
	}
	state := prev
	state.Phase = want
	state.StoppedAt = nil
	state.DrainDeadline = nil
	state.ETAStrikes = 0
	state.RearmObservations = 0
	if want == domain.PhaseNormal {
		t := now
		state.RecoveredAt = &t
	}
	state.Healthy = want == domain.PhaseNormal && prev.UsedPercent < e.t.WarnPercent
	state.AppliedThresholds = e.applied()
	state.UpdatedAt = now
	before := "unknown"
	if prev.AppliedThresholds != nil {
		before = prev.AppliedThresholds.String()
	}
	snap := domain.QuotaSnapshot{
		Key: prev.Key, LimitName: prev.LimitName, UsedPercent: prev.UsedPercent,
		ResetsAt: prev.ResetsAt, ModelSelector: prev.ModelSelector, ObservedAt: prev.ObservedAt,
		SourceEventID: prev.LastEventID, WindowDuration: prev.WindowDuration,
	}
	return domain.Decision{
		State: state,
		Actions: []domain.Action{{
			Kind: domain.ActionRearm, Bucket: prev.Key, Snapshot: snap,
			Reason: fmt.Sprintf("re-derived at load: %s at %.0f%% is %s under thresholds warn/drain/stop %s, not %s as stored under %s",
				describe(snap), prev.UsedPercent, want, state.AppliedThresholds, prev.Phase, before),
		}},
	}
}

// Tick advances timers without a new snapshot. When a drain grace period
// expires it escalates to the hard stop on the same terms a reading does:
// the last reading at or above the stop threshold, or an exhaustion projected
// under the hard-stop floor. Below both the drain request stands and the next
// reading decides.
func (e *Engine) Tick(prev domain.BucketState, now time.Time) domain.Decision {
	state := prev
	if state.Phase != domain.PhaseDraining || state.DrainDeadline == nil || now.Before(*state.DrainDeadline) {
		return domain.Decision{State: prev}
	}
	if state.ResetsAt != nil && state.ExhaustsIn != nil && e.t.RateWindow > 0 {
		if until := state.ResetsAt.Sub(now); until > 0 {
			if ok, why := e.runwayHolds(*state.ExhaustsIn, until, state.RatePerMinute); ok {
				state.DrainDeadline = nil
				state.UpdatedAt = now
				return domain.Decision{State: state, Ignored: "grace expired but " + why}
			}
		}
	}
	if state.ResetsAt != nil && e.t.ResetExemption > 0 {
		if until := state.ResetsAt.Sub(now); until > 0 && until <= e.t.ResetExemption {
			// The window resets before a stop would save anything; let the
			// drain request stand and wait for the reset.
			state.DrainDeadline = nil
			state.UpdatedAt = now
			return domain.Decision{State: state, Ignored: fmt.Sprintf("grace expired but the window resets in %s; not stopping", until.Round(time.Second))}
		}
	}
	// The grace stop obeys the same rule as the ladder (S-18): below the
	// stop threshold an interrupt is worth its cost only when no drain can
	// finish before the provider ends the session at 100% by itself. The
	// drain notice was the point; a session that ignores it and keeps
	// climbing is stopped by the reading that crosses the threshold.
	if state.UsedPercent < e.t.StopPercent && (state.ExhaustsIn == nil || *state.ExhaustsIn > e.hardStopETA()) {
		state.DrainDeadline = nil
		state.UpdatedAt = now
		eta := "no exhaustion projected"
		if state.ExhaustsIn != nil {
			eta = fmt.Sprintf("exhaustion projected in %s", state.ExhaustsIn.Round(time.Minute))
		}
		return domain.Decision{State: state, Ignored: fmt.Sprintf("grace expired at %.0f%%, below the stop threshold of %.0f%% with %s; the drain request stands", state.UsedPercent, e.t.StopPercent, eta)}
	}
	state.Phase = domain.PhaseStopped
	state.DrainDeadline = nil
	stoppedAt := now
	state.StoppedAt = &stoppedAt
	state.Healthy = false
	state.UpdatedAt = now
	snap := domain.QuotaSnapshot{
		Key: state.Key, LimitName: state.LimitName, UsedPercent: state.UsedPercent,
		ResetsAt: state.ResetsAt, ModelSelector: state.ModelSelector, ObservedAt: state.ObservedAt,
		SourceEventID: state.LastEventID, WindowDuration: state.WindowDuration,
	}
	return domain.Decision{
		State: state,
		Actions: []domain.Action{{
			Kind: domain.ActionStop, Bucket: state.Key, Snapshot: snap,
			Reason: fmt.Sprintf("grace period of %s expired after the drain warning", e.t.GracePeriod),
		}},
	}
}

// detectReset decides whether the snapshot belongs to a new reset window.
func (e *Engine) detectReset(snap domain.QuotaSnapshot, prev domain.BucketState) (bool, string) {
	if prev.ObservedAt.IsZero() {
		return false, ""
	}
	if prev.ResetsAt == nil {
		return false, ""
	}
	windowPassed := snap.ObservedAt.After(*prev.ResetsAt)
	movedForward := snap.ResetsAt != nil && snap.ResetsAt.Sub(*prev.ResetsAt) > e.t.ResetTolerance
	switch {
	case windowPassed && snap.UsedPercent < e.t.RearmPercent:
		return true, fmt.Sprintf("window reset at %s confirmed by a fresh snapshot at %.0f%%", prev.ResetsAt.UTC().Format(time.RFC3339), snap.UsedPercent)
	case movedForward && snap.UsedPercent < e.t.RearmPercent:
		return true, fmt.Sprintf("provider moved the reset time forward to %s and reports %.0f%%", snap.ResetsAt.UTC().Format(time.RFC3339), snap.UsedPercent)
	case windowPassed || movedForward:
		// A new window that is already above the rearm threshold: the old
		// alerts are over, but the bucket is not healthy. Treat it as a fresh
		// epoch so the ladder can fire again.
		return true, fmt.Sprintf("new window (reset %s) starts at %.0f%%", domain.EpochFor(snap.ResetsAt), snap.UsedPercent)
	case prev.Phase != domain.PhaseNormal && snap.UsedPercent < prev.UsedPercent && snap.UsedPercent < e.t.WarnPercent-ExtensionMargin:
		// Usage never falls within a window unless the provider raised the
		// limit. A drop that lands well below the warn threshold means the
		// quota was extended; a drop that stays near the threshold, or a
		// reading that is merely low after a projection-based stop, does not.
		return true, fmt.Sprintf("provider extended the quota: usage fell from %.0f%% to %.0f%%, more than %d points below the warn threshold of %.0f%%",
			prev.UsedPercent, snap.UsedPercent, ExtensionMargin, e.t.WarnPercent)
	}
	return false, ""
}

func (e *Engine) thresholdFor(p domain.Phase) float64 {
	switch p {
	case domain.PhaseWarned:
		return e.t.WarnPercent
	case domain.PhaseDraining:
		return e.t.DrainPercent
	case domain.PhaseStopped:
		return e.t.StopPercent
	}
	return 0
}

func describe(s domain.QuotaSnapshot) string {
	if s.LimitName != "" {
		return fmt.Sprintf("%q (%s)", s.LimitName, s.Key)
	}
	return s.Key.String()
}
