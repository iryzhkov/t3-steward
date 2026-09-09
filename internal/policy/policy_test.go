package policy

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var (
	base    = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	resetAt = base.Add(5 * time.Hour)
	key     = domain.BucketKey{ProviderInstanceID: "codex", LimitID: "codex", Window: "primary"}
)

func snap(used float64, at time.Time, resets *time.Time, id string) domain.QuotaSnapshot {
	return domain.QuotaSnapshot{Key: key, LimitName: "codex primary", UsedPercent: used, ResetsAt: resets, ObservedAt: at, SourceEventID: id}
}

func kinds(d domain.Decision) []domain.ActionKind {
	var out []domain.ActionKind
	for _, a := range d.Actions {
		out = append(out, a.Kind)
	}
	return out
}

func only(t *testing.T, d domain.Decision, want ...domain.ActionKind) {
	t.Helper()
	got := kinds(d)
	if len(got) != len(want) {
		t.Fatalf("actions = %v, want %v (ignored=%q)", got, want, d.Ignored)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("actions = %v, want %v", got, want)
		}
	}
}

func TestLadder(t *testing.T) {
	e := New(DefaultThresholds())
	r := resetAt
	var st domain.BucketState
	// Readings ten minutes apart keep the burn rate below the exhaustion
	// ladder; this test is about the percentage ladder alone.
	d := e.Evaluate(snap(84, base, &r, "1"), st, base)
	only(t, d)
	st = d.State
	d = e.Evaluate(snap(85, base.Add(10*time.Minute), &r, "2"), st, base.Add(10*time.Minute))
	only(t, d, domain.ActionWarn)
	st = d.State
	if st.Phase != domain.PhaseWarned {
		t.Fatalf("phase = %s", st.Phase)
	}
	d = e.Evaluate(snap(88, base.Add(20*time.Minute), &r, "3"), st, base.Add(20*time.Minute))
	only(t, d)
	st = d.State
	d = e.Evaluate(snap(90, base.Add(30*time.Minute), &r, "4"), st, base.Add(30*time.Minute))
	only(t, d, domain.ActionDrain)
	st = d.State
	if st.DrainDeadline == nil || !st.DrainDeadline.Equal(base.Add(31*time.Minute)) {
		t.Fatalf("drain deadline = %v", st.DrainDeadline)
	}
	d = e.Evaluate(snap(95, base.Add(30*time.Minute+30*time.Second), &r, "5"), st, base.Add(30*time.Minute+30*time.Second))
	only(t, d, domain.ActionStop)
	st = d.State
	if st.Phase != domain.PhaseStopped || st.DrainDeadline != nil || st.Healthy {
		t.Fatalf("state after stop = %+v", st)
	}
	// Usage decreasing without a reset does not rearm.
	d = e.Evaluate(snap(40, base.Add(40*time.Minute), &r, "6"), st, base.Add(40*time.Minute))
	only(t, d)
	if d.State.Phase != domain.PhaseStopped {
		t.Fatalf("decrease without reset rearmed: %s", d.State.Phase)
	}
}

func TestDirectJumpToStop(t *testing.T) {
	e := New(DefaultThresholds())
	r := resetAt
	d := e.Evaluate(snap(97, base, &r, "1"), domain.BucketState{}, base)
	only(t, d, domain.ActionStop)
}

func TestGraceTimerStops(t *testing.T) {
	e := New(DefaultThresholds())
	r := resetAt
	d := e.Evaluate(snap(91, base, &r, "1"), domain.BucketState{}, base)
	only(t, d, domain.ActionDrain)
	st := d.State
	d = e.Tick(st, base.Add(30*time.Second))
	only(t, d)
	d = e.Tick(st, base.Add(61*time.Second))
	only(t, d, domain.ActionStop)
	if d.State.Phase != domain.PhaseStopped {
		t.Fatalf("phase = %s", d.State.Phase)
	}
	// Restart during a grace period: the persisted state still carries the
	// deadline, so the tick still fires.
	restarted := st
	d = e.Tick(restarted, base.Add(2*time.Minute))
	only(t, d, domain.ActionStop)
}

func TestDuplicateAndOutOfOrder(t *testing.T) {
	e := New(DefaultThresholds())
	r := resetAt
	d := e.Evaluate(snap(86, base.Add(time.Minute), &r, "a"), domain.BucketState{}, base.Add(time.Minute))
	only(t, d, domain.ActionWarn)
	st := d.State
	d = e.Evaluate(snap(86, base.Add(time.Minute), &r, "a"), st, base.Add(2*time.Minute))
	if d.Ignored == "" {
		t.Fatal("duplicate accepted")
	}
	d = e.Evaluate(snap(20, base, &r, "b"), st, base.Add(2*time.Minute))
	if d.Ignored == "" {
		t.Fatal("out-of-order accepted")
	}
}

func TestExpiredSnapshotIgnored(t *testing.T) {
	e := New(DefaultThresholds())
	r := resetAt
	d := e.Evaluate(snap(99, base, &r, "old"), domain.BucketState{}, resetAt.Add(time.Hour))
	if d.Ignored == "" || len(d.Actions) != 0 {
		t.Fatalf("expired snapshot acted: %+v", d)
	}
}

func TestResetRearms(t *testing.T) {
	e := New(DefaultThresholds())
	r := resetAt
	d := e.Evaluate(snap(96, base, &r, "1"), domain.BucketState{}, base)
	only(t, d, domain.ActionStop)
	st := d.State
	// Wall clock passing the reset does nothing by itself.
	d = e.Tick(st, resetAt.Add(time.Minute))
	only(t, d)
	// A fresh snapshot after the reset with low usage rearms.
	r2 := resetAt.Add(5 * time.Hour)
	d = e.Evaluate(snap(3, resetAt.Add(2*time.Minute), &r2, "2"), st, resetAt.Add(2*time.Minute))
	only(t, d, domain.ActionRearm)
	st = d.State
	if st.Phase != domain.PhaseNormal || !st.Healthy || st.RecoveredAt == nil || st.Epoch != domain.EpochFor(&r2) {
		t.Fatalf("state after rearm = %+v", st)
	}
	// The ladder fires again in the new epoch.
	d = e.Evaluate(snap(85, resetAt.Add(3*time.Minute), &r2, "3"), st, resetAt.Add(3*time.Minute))
	only(t, d, domain.ActionWarn)
}

func TestResetIntoHighUsageStartsNewEpochWithoutRearm(t *testing.T) {
	e := New(DefaultThresholds())
	r := resetAt
	d := e.Evaluate(snap(96, base, &r, "1"), domain.BucketState{}, base)
	st := d.State
	r2 := resetAt.Add(5 * time.Hour)
	// New window but already at 92%: rearm the ladder and immediately drain.
	d = e.Evaluate(snap(92, resetAt.Add(time.Minute), &r2, "2"), st, resetAt.Add(time.Minute))
	only(t, d, domain.ActionRearm, domain.ActionDrain)
	if d.State.Healthy {
		t.Fatal("bucket healthy at 92%")
	}
}

func TestResetTimeJitterIsNotAReset(t *testing.T) {
	e := New(DefaultThresholds())
	r := resetAt
	d := e.Evaluate(snap(96, base, &r, "1"), domain.BucketState{}, base)
	st := d.State
	jitter := resetAt.Add(-time.Second)
	d = e.Evaluate(snap(30, base.Add(time.Minute), &jitter, "2"), st, base.Add(time.Minute))
	only(t, d)
	if d.State.Phase != domain.PhaseStopped {
		t.Fatalf("jitter rearmed: %s", d.State.Phase)
	}
}

func TestResetTimeMovedForwardWithLowUsageRearms(t *testing.T) {
	e := New(DefaultThresholds())
	r := resetAt
	d := e.Evaluate(snap(96, base, &r, "1"), domain.BucketState{}, base)
	st := d.State
	moved := resetAt.Add(time.Hour)
	d = e.Evaluate(snap(10, base.Add(time.Minute), &moved, "2"), st, base.Add(time.Minute))
	only(t, d, domain.ActionRearm)
}

func TestNoResetTimeNeedsConsecutiveLowObservations(t *testing.T) {
	e := New(DefaultThresholds())
	d := e.Evaluate(snap(96, base, nil, "1"), domain.BucketState{}, base)
	only(t, d, domain.ActionStop)
	st := d.State
	d = e.Evaluate(snap(10, base.Add(time.Minute), nil, "2"), st, base.Add(time.Minute))
	only(t, d)
	st = d.State
	d = e.Evaluate(snap(70, base.Add(2*time.Minute), nil, "3"), st, base.Add(2*time.Minute))
	only(t, d)
	st = d.State
	if st.RearmObservations != 0 {
		t.Fatalf("counter not reset: %d", st.RearmObservations)
	}
	d = e.Evaluate(snap(10, base.Add(3*time.Minute), nil, "4"), st, base.Add(3*time.Minute))
	st = d.State
	d = e.Evaluate(snap(12, base.Add(4*time.Minute), nil, "5"), st, base.Add(4*time.Minute))
	only(t, d, domain.ActionRearm)
}

func TestIndependentWindows(t *testing.T) {
	e := New(DefaultThresholds())
	r := resetAt
	primary := e.Evaluate(snap(96, base, &r, "1"), domain.BucketState{}, base)
	secondaryKey := key
	secondaryKey.Window = "secondary"
	s := snap(20, base, &r, "2")
	s.Key = secondaryKey
	secondary := e.Evaluate(s, domain.BucketState{}, base)
	if primary.State.Phase != domain.PhaseStopped || secondary.State.Phase != domain.PhaseNormal {
		t.Fatalf("windows not independent: %s / %s", primary.State.Phase, secondary.State.Phase)
	}
	if !secondary.State.Healthy {
		t.Fatal("secondary should be healthy")
	}
}

func TestValidate(t *testing.T) {
	bad := DefaultThresholds()
	bad.DrainPercent = 80
	if err := bad.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
	if err := DefaultThresholds().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestBurnRateDrainsBeforeThresholds(t *testing.T) {
	e := New(DefaultThresholds())
	r := base.Add(3 * time.Hour) // window resets in 3 hours
	var st domain.BucketState
	// Above the warn threshold and burning 1.5%/min: the projection says
	// under ten minutes to exhaustion, so the drain request goes out at
	// 89.5% instead of waiting for 90%.
	d := e.Evaluate(snap(85, base, &r, "r0"), st, base)
	only(t, d, domain.ActionWarn)
	st = d.State
	d = e.Evaluate(snap(86.5, base.Add(time.Minute), &r, "r1"), st, base.Add(time.Minute))
	only(t, d)
	st = d.State
	d = e.Evaluate(snap(88, base.Add(2*time.Minute), &r, "r2"), st, base.Add(2*time.Minute))
	only(t, d) // first strike
	st = d.State
	if st.RatePerMinute < 1.4 || st.RatePerMinute > 1.6 || st.ETAStrikes != 1 {
		t.Fatalf("rate = %v strikes = %d", st.RatePerMinute, st.ETAStrikes)
	}
	d = e.Evaluate(snap(89.5, base.Add(3*time.Minute), &r, "r3"), st, base.Add(3*time.Minute))
	only(t, d, domain.ActionDrain)
	if !strings.Contains(d.Actions[0].Reason, "burning") {
		t.Fatalf("reason = %s", d.Actions[0].Reason)
	}
}

func TestProjectionNeverFiresBelowWarnThreshold(t *testing.T) {
	e := New(DefaultThresholds())
	r := base.Add(3 * time.Hour)
	var st domain.BucketState
	// A burst early in the window: 29% to 41% in six minutes projects 29
	// minutes to exhaustion, but usage is far below the warn threshold.
	st = e.Evaluate(snap(29, base, &r, "b1"), st, base).State
	st = e.Evaluate(snap(30, base.Add(4*time.Minute), &r, "b2"), st, base.Add(4*time.Minute)).State
	st = e.Evaluate(snap(41, base.Add(6*time.Minute), &r, "b3"), st, base.Add(6*time.Minute)).State
	d := e.Evaluate(snap(43, base.Add(7*time.Minute), &r, "b4"), st, base.Add(7*time.Minute))
	only(t, d)
	if d.State.Phase != domain.PhaseNormal || d.State.ETAStrikes != 0 {
		t.Fatalf("state = %+v", d.State)
	}
	// A bucket left warned by an earlier version drops back to normal
	// when usage is below the threshold.
	warned := d.State
	warned.Phase = domain.PhaseWarned
	d = e.Evaluate(snap(44, base.Add(8*time.Minute), &r, "b5"), warned, base.Add(8*time.Minute))
	if d.State.Phase != domain.PhaseNormal {
		t.Fatalf("warned did not de-escalate: %s", d.State.Phase)
	}
}

func TestResetExemptionSkipsStopNearReset(t *testing.T) {
	e := New(DefaultThresholds())
	r := base.Add(2 * time.Minute)
	d := e.Evaluate(snap(96, base, &r, "1"), domain.BucketState{}, base)
	only(t, d)
	if d.State.Phase != domain.PhaseNormal {
		t.Fatalf("phase = %s", d.State.Phase)
	}
	// And the grace timer does not fire a stop when the reset is that close.
	far := base.Add(time.Hour)
	d = e.Evaluate(snap(91, base, &far, "2"), domain.BucketState{}, base)
	only(t, d, domain.ActionDrain)
	st := d.State
	st.ResetsAt = ptrTime(base.Add(3 * time.Minute))
	d = e.Tick(st, base.Add(2*time.Minute))
	only(t, d)
	if d.Ignored == "" || d.State.DrainDeadline != nil {
		t.Fatalf("tick = %+v", d)
	}
}

func TestExhaustionAfterResetIsIgnored(t *testing.T) {
	e := New(DefaultThresholds())
	r := base.Add(12 * time.Minute) // resets before the projected exhaustion
	var st domain.BucketState
	used := 40.0
	for i := 0; i <= 10; i++ {
		at := base.Add(time.Duration(i) * time.Minute)
		st = e.Evaluate(snap(used, at, &r, fmt.Sprintf("x%d", i)), st, at).State
		used += 2.3
	}
	if st.Phase != domain.PhaseNormal {
		t.Fatalf("phase = %s although the window resets before exhaustion", st.Phase)
	}
}

func ptrTime(t time.Time) *time.Time { return &t }

func TestRunwayHoldsSkipsStopAtHighPercent(t *testing.T) {
	e := New(DefaultThresholds())
	r := base.Add(60 * time.Minute) // resets in an hour
	var st domain.BucketState
	// A slow, steady climb from 84%: 0.2%/min. At 93% with 15 minutes to
	// the reset the runway is about 35 minutes, more than 1.5x the wait,
	// so nothing fires although the percentage ladder passed 85 and 90.
	used := 84.0
	for i := 0; i <= 45; i++ {
		at := base.Add(time.Duration(i) * time.Minute)
		st = e.Evaluate(snap(used, at, &r, fmt.Sprintf("w%d", i)), st, at).State
		used += 0.2
	}
	// The 85% warning went out early on, when the runway (75 minutes)
	// did not yet cover 1.5x the 55 minutes to the reset; the drain at 90%
	// and the stop at 95% did not, because by then it did.
	if st.Phase.Rank() > domain.PhaseWarned.Rank() || st.ExhaustsIn == nil {
		t.Fatalf("phase = %s with runway to spare (eta %v)", st.Phase, st.ExhaustsIn)
	}
	// A burst changes the rate: 99% half a minute later projects
	// exhaustion within minutes, well before the reset.
	at := base.Add(45*time.Minute + 30*time.Second)
	d := e.Evaluate(snap(99, at, &r, "burst"), st, at)
	only(t, d, domain.ActionStop)
}
