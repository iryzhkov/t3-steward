package policy

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-quota-watchdog/internal/domain"
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
	d := e.Evaluate(snap(84, base, &r, "1"), st, base)
	only(t, d)
	st = d.State
	d = e.Evaluate(snap(85, base.Add(time.Minute), &r, "2"), st, base.Add(time.Minute))
	only(t, d, domain.ActionWarn)
	st = d.State
	if st.Phase != domain.PhaseWarned {
		t.Fatalf("phase = %s", st.Phase)
	}
	d = e.Evaluate(snap(88, base.Add(2*time.Minute), &r, "3"), st, base.Add(2*time.Minute))
	only(t, d)
	st = d.State
	d = e.Evaluate(snap(90, base.Add(3*time.Minute), &r, "4"), st, base.Add(3*time.Minute))
	only(t, d, domain.ActionDrain)
	st = d.State
	if st.DrainDeadline == nil || !st.DrainDeadline.Equal(base.Add(4*time.Minute)) {
		t.Fatalf("drain deadline = %v", st.DrainDeadline)
	}
	d = e.Evaluate(snap(95, base.Add(3*time.Minute+30*time.Second), &r, "5"), st, base.Add(3*time.Minute+30*time.Second))
	only(t, d, domain.ActionStop)
	st = d.State
	if st.Phase != domain.PhaseStopped || st.DrainDeadline != nil || st.Healthy {
		t.Fatalf("state after stop = %+v", st)
	}
	// Usage decreasing without a reset does not rearm.
	d = e.Evaluate(snap(40, base.Add(10*time.Minute), &r, "6"), st, base.Add(10*time.Minute))
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
