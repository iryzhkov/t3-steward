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

	"github.com/iryzhkov/t3-quota-watchdog/internal/domain"
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
}

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
	}
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
	return nil
}

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
				state.RearmObservations = 0
			}
		} else {
			state.RearmObservations = 0
		}
	}

	// Escalate to the highest applicable level. Levels below the current
	// phase never fire again within the same epoch.
	want := e.level(snap.UsedPercent)
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
			Reason: fmt.Sprintf("%s at %.0f%% (threshold %.0f%%)", describe(snap), snap.UsedPercent, e.thresholdFor(want)),
		})
		state.Phase = want
		switch want {
		case domain.PhaseDraining:
			deadline := now.Add(e.t.GracePeriod)
			state.DrainDeadline = &deadline
		case domain.PhaseStopped:
			state.DrainDeadline = nil
		}
	}

	state.LimitName = snap.LimitName
	state.ModelSelector = snap.ModelSelector
	state.UsedPercent = snap.UsedPercent
	state.ResetsAt = snap.ResetsAt
	state.Epoch = domain.EpochFor(snap.ResetsAt)
	state.ObservedAt = snap.ObservedAt
	state.LastEventID = snap.SourceEventID
	state.UpdatedAt = now
	state.Healthy = state.Phase == domain.PhaseNormal && snap.UsedPercent < e.t.WarnPercent
	return domain.Decision{State: state, Actions: actions}
}

// Tick advances timers without a new snapshot. It fires the hard stop when a
// drain grace period expires.
func (e *Engine) Tick(prev domain.BucketState, now time.Time) domain.Decision {
	state := prev
	if state.Phase != domain.PhaseDraining || state.DrainDeadline == nil || now.Before(*state.DrainDeadline) {
		return domain.Decision{State: prev}
	}
	state.Phase = domain.PhaseStopped
	state.DrainDeadline = nil
	state.Healthy = false
	state.UpdatedAt = now
	snap := domain.QuotaSnapshot{
		Key: state.Key, LimitName: state.LimitName, UsedPercent: state.UsedPercent,
		ResetsAt: state.ResetsAt, ModelSelector: state.ModelSelector, ObservedAt: state.ObservedAt,
		SourceEventID: state.LastEventID,
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
