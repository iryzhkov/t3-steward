package policy

import (
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// The 10:07:55 case (S-18): an interactive session running five subagents
// climbs from 78% to 86% in two minutes, which projects exhaustion in about
// four minutes with 112 minutes to the reset. The projection may ask for a
// drain, never for a hard stop while the percentage is below the stop
// threshold: the provider ends the session itself at 100%, and an interrupt
// discards the subagents' work for nothing.
func TestBurnRateProjectionDrainsButNeverStopsBelowStopPercent(t *testing.T) {
	e := New(DefaultThresholds())
	r := base.Add(112 * time.Minute)
	var st domain.BucketState
	st = e.Evaluate(snap(78.4, base, &r, "s0"), st, base).State
	st = e.Evaluate(snap(82.2, base.Add(time.Minute), &r, "s1"), st, base.Add(time.Minute)).State
	d := e.Evaluate(snap(86, base.Add(2*time.Minute), &r, "s2"), st, base.Add(2*time.Minute))
	only(t, d, domain.ActionWarn) // the percentage ladder; the projection needs a second strike
	st = d.State
	if st.RatePerMinute < 3.5 || st.RatePerMinute > 4.1 || st.ExhaustsIn == nil || *st.ExhaustsIn > 5*time.Minute {
		t.Fatalf("rate = %v eta = %v", st.RatePerMinute, st.ExhaustsIn)
	}
	d = e.Evaluate(snap(86.3, base.Add(2*time.Minute+5*time.Second), &r, "s3"), st, base.Add(2*time.Minute+5*time.Second))
	only(t, d, domain.ActionDrain)
	if d.State.Phase != domain.PhaseDraining || d.State.StoppedAt != nil {
		t.Fatalf("state = %+v", d.State)
	}
	if !strings.Contains(d.Actions[0].Reason, "burning") {
		t.Fatalf("reason = %s", d.Actions[0].Reason)
	}
	// A projection under the hard-stop floor still stops: at 93% burning
	// 5%/min the session has under two minutes left.
	st = domain.BucketState{}
	st = e.Evaluate(snap(80, base, &r, "f0"), st, base).State
	st = e.Evaluate(snap(85, base.Add(time.Minute), &r, "f1"), st, base.Add(time.Minute)).State
	st = e.Evaluate(snap(91, base.Add(2*time.Minute), &r, "f2"), st, base.Add(2*time.Minute)).State
	d = e.Evaluate(snap(93, base.Add(2*time.Minute+30*time.Second), &r, "f3"), st, base.Add(2*time.Minute+30*time.Second))
	if d.State.Phase != domain.PhaseStopped || d.State.StoppedAt == nil {
		t.Fatalf("state under the hard-stop floor = %+v actions=%v", d.State, kinds(d))
	}
}

// With the default runway margin of one, a projected exhaustion later than
// the reset is no reason to act, whatever the percentage.
func TestRunwayMarginOneIgnoresExhaustionAfterReset(t *testing.T) {
	th := DefaultThresholds()
	if th.RunwayMargin != 1 {
		t.Fatalf("default runway margin = %v, want 1", th.RunwayMargin)
	}
	e := New(th)
	r := base.Add(20 * time.Minute)
	// 94% ten minutes ago, 96% now: 0.2%/min, twenty minutes of runway
	// against a reset in twenty minutes. Under the old 1.5 margin the 96%
	// reading fired a stop.
	earlier := base.Add(-10 * time.Minute)
	st := domain.BucketState{
		Key: key, Phase: domain.PhaseNormal, UsedPercent: 94, ResetsAt: &r, ObservedAt: earlier,
		Epoch: domain.EpochFor(&r), Recent: []domain.Reading{{At: earlier, Used: 94}},
	}
	d := e.Evaluate(snap(96, base, &r, "m2"), st, base)
	only(t, d)
	if d.State.Phase != domain.PhaseNormal {
		t.Fatalf("phase = %s with exhaustion projected after the reset", d.State.Phase)
	}
}

// StoppedAt records when the bucket entered the stopped phase and is cleared
// by the reset, so the daemon can tell a turn the user started after the stop
// from one the harness started on its own.
func TestStoppedAtFollowsTheStopAndTheReset(t *testing.T) {
	e := New(DefaultThresholds())
	r := resetAt
	d := e.Evaluate(snap(96, base.Add(time.Minute), &r, "1"), domain.BucketState{}, base.Add(time.Minute))
	only(t, d, domain.ActionStop)
	if d.State.StoppedAt == nil || !d.State.StoppedAt.Equal(base.Add(time.Minute)) {
		t.Fatalf("stoppedAt = %v", d.State.StoppedAt)
	}
	next := resetAt.Add(5 * time.Hour)
	d = e.Evaluate(snap(3, resetAt.Add(time.Minute), &next, "2"), d.State, resetAt.Add(time.Minute))
	only(t, d, domain.ActionRearm)
	if d.State.StoppedAt != nil {
		t.Fatalf("stoppedAt survived the reset: %v", d.State.StoppedAt)
	}
	// The grace-timer stop records it too.
	far := base.Add(time.Hour)
	d = e.Evaluate(snap(91, base, &far, "3"), domain.BucketState{}, base)
	only(t, d, domain.ActionDrain)
	d = e.Tick(d.State, base.Add(2*time.Minute))
	only(t, d, domain.ActionStop)
	if d.State.StoppedAt == nil || !d.State.StoppedAt.Equal(base.Add(2*time.Minute)) {
		t.Fatalf("tick stoppedAt = %v", d.State.StoppedAt)
	}
}
