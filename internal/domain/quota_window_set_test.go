package domain

import (
	"strings"
	"testing"
	"time"
)

// A reading is fresh up to and including the maximum age and stale one
// nanosecond past it; a reading taken before its own reset is stale once that
// reset has passed, however young it is; a zero time is never fresh.
func TestQuotaReadingFreshAtTheAgeEdges(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	key := BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "seven_day"}
	later := now.Add(48 * time.Hour)
	for _, tc := range []struct {
		name   string
		state  BucketState
		maxAge time.Duration
		fresh  bool
	}{
		{name: "exactly the maximum age", state: BucketState{Key: key, ObservedAt: now.Add(-10 * time.Minute)}, maxAge: 10 * time.Minute, fresh: true},
		{name: "one nanosecond past it", state: BucketState{Key: key, ObservedAt: now.Add(-10*time.Minute - time.Nanosecond)}, maxAge: 10 * time.Minute},
		{name: "default threshold, 59m", state: BucketState{Key: key, ObservedAt: now.Add(-59 * time.Minute)}, fresh: true},
		{name: "default threshold, 61m", state: BucketState{Key: key, ObservedAt: now.Add(-61 * time.Minute)}},
		{name: "zero observation time", state: BucketState{Key: key}, maxAge: time.Hour},
		{name: "young, own reset passed", state: BucketState{Key: key, ObservedAt: now.Add(-time.Minute), ResetsAt: ptrTime(now.Add(-time.Second))}, maxAge: time.Hour},
		{name: "young, own reset exactly now", state: BucketState{Key: key, ObservedAt: now.Add(-time.Minute), ResetsAt: ptrTime(now)}, maxAge: time.Hour},
		{name: "young, reset ahead", state: BucketState{Key: key, ObservedAt: now.Add(-time.Minute), ResetsAt: &later}, maxAge: time.Hour, fresh: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := QuotaReadingFresh(tc.state, now, tc.maxAge); got != tc.fresh {
				t.Fatalf("fresh = %v, want %v", got, tc.fresh)
			}
		})
	}
}

// Exhaustion is at 100% or in the stopped phase; 99.9% normal is not.
func TestQuotaReadingExhaustedAtThePercentEdge(t *testing.T) {
	for _, tc := range []struct {
		state     BucketState
		exhausted bool
	}{
		{state: BucketState{UsedPercent: 99.9, Phase: PhaseDraining}},
		{state: BucketState{UsedPercent: 100, Phase: PhaseDraining}, exhausted: true},
		{state: BucketState{UsedPercent: 40, Phase: PhaseStopped}, exhausted: true},
	} {
		if got := QuotaReadingExhausted(tc.state); got != tc.exhausted {
			t.Fatalf("%+v: exhausted = %v, want %v", tc.state, got, tc.exhausted)
		}
	}
}

func ptrTime(at time.Time) *time.Time { return &at }

// The declared windows are the provider's, not the ones that happen to have
// been observed: Claude declares five_hour and seven_day, Codex primary and
// secondary only when it reports one, and any other provider declares
// nothing beyond what it was observed reporting.
func TestDeclaredQuotaWindows(t *testing.T) {
	secondary := []BucketState{{Key: BucketKey{ProviderInstanceID: "codex", LimitID: "codex", Window: WindowSecondary}}}
	for _, tc := range []struct {
		provider string
		observed []BucketState
		want     string
	}{
		{provider: "claude", want: "five_hour,seven_day"},
		{provider: "claudeAgent", want: "five_hour,seven_day"},
		{provider: "codex", want: "primary"},
		{provider: "codex", observed: secondary, want: "primary,secondary"},
		{provider: "ollama", want: ""},
	} {
		if got := strings.Join(DeclaredQuotaWindows(tc.provider, tc.observed), ","); got != tc.want {
			t.Fatalf("%s with %d observed: %q, want %q", tc.provider, len(tc.observed), got, tc.want)
		}
	}
}

// A window set is complete only when every declared window has a reading and
// fresh only when every reading in it is fresh; the first problem names the
// window.
func TestReadQuotaWindowsCompletenessAndFreshness(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	reset := now.Add(72 * time.Hour)
	five := BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"}
	week := BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "seven_day"}
	pool := QuotaPool{ID: "pool-claude", Provider: "claude", ProviderInstanceIDs: []string{"claudeAgent"}, BucketSelection: BucketSelectionResolved, Buckets: []BucketKey{five, week}}
	freshFive := BucketState{Key: five, Phase: PhaseNormal, UsedPercent: 10, ObservedAt: now.Add(-time.Minute)}
	freshWeek := BucketState{Key: week, Phase: PhaseNormal, UsedPercent: 20, ResetsAt: &reset, ObservedAt: now.Add(-time.Minute)}
	for _, tc := range []struct {
		name     string
		pool     QuotaPool
		states   []BucketState
		complete bool
		fresh    bool
		problem  string
		code     string
	}{
		{name: "complete and fresh", pool: pool, states: []BucketState{freshFive, freshWeek}, complete: true, fresh: true},
		{name: "declared weekly window missing", pool: QuotaPool{ID: "pool-claude", Provider: "claude", BucketSelection: BucketSelectionResolved, Buckets: []BucketKey{five}},
			states: []BucketState{freshFive}, problem: "missing seven_day", code: QuotaWindowMissing},
		{name: "named bucket with no reading", pool: pool, states: []BucketState{freshFive}, problem: "missing seven_day", code: QuotaWindowMissing},
		{name: "weekly window present and stale", pool: pool, states: []BucketState{freshFive, {Key: week, UsedPercent: 20, ResetsAt: &reset, ObservedAt: now.Add(-61 * time.Minute)}},
			complete: true, problem: "stale seven_day", code: QuotaWindowStale},
		{name: "fresh and exhausted", pool: pool, states: []BucketState{freshFive, {Key: week, Phase: PhaseStopped, UsedPercent: 100, ResetsAt: &reset, ObservedAt: now}},
			complete: true, fresh: true, problem: "exhausted seven_day until 2026-10-07T12:00:00Z", code: QuotaWindowExhausted},
		{name: "unknown buckets", pool: QuotaPool{ID: "pool-claude", Provider: "claude", BucketSelection: BucketSelectionUnknown}, states: []BucketState{freshFive, freshWeek},
			problem: "quota unknown", code: QuotaWindowUnknown},
		{name: "undeclared provider with no reading", pool: QuotaPool{ID: "pool-local", Provider: "ollama", BucketSelection: BucketSelectionResolved},
			problem: "missing quota reading", code: QuotaWindowMissing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set := ReadQuotaWindows(tc.pool, tc.states, now, time.Hour)
			if set.Complete() != tc.complete || set.Fresh() != tc.fresh {
				t.Fatalf("complete=%v fresh=%v, want %v and %v: %+v", set.Complete(), set.Fresh(), tc.complete, tc.fresh, set)
			}
			problem, found := set.Problem()
			if found != (tc.problem != "") || problem.String() != tc.problem || problem.Code != tc.code {
				t.Fatalf("problem = %q (%s, found %v), want %q (%s)", problem.String(), problem.Code, found, tc.problem, tc.code)
			}
		})
	}
}

// F4: the aggregate took the highest percent from one window, the earliest
// reset from another and the newest observation time from a third, a reading
// no single observation supports. The aggregate now carries all three from
// the one window it reports, and each window is listed on its own.
func TestObserveQuotaPoolDoesNotMixWindows(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	soon, later := now.Add(time.Hour), now.Add(72*time.Hour)
	five := BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"}
	week := BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "seven_day"}
	pool := QuotaPool{ID: "pool-claude", Provider: "claude", BucketSelection: BucketSelectionResolved, Buckets: []BucketKey{five, week}}
	states := []BucketState{
		{Key: five, Phase: PhaseNormal, UsedPercent: 12, ResetsAt: &soon, ObservedAt: now},
		{Key: week, Phase: PhaseDraining, UsedPercent: 97, ResetsAt: &later, ObservedAt: now.Add(-3 * time.Hour)},
	}
	obs := ObserveQuotaPool(pool, states)
	if obs.Percent != 97 || obs.ResetsAt == nil || !obs.ResetsAt.Equal(later) || !obs.ObservedAt.Equal(now.Add(-3*time.Hour)) || obs.Window != week.String() {
		t.Fatalf("aggregate mixes windows: %+v", obs)
	}
	if len(obs.Windows) != 2 {
		t.Fatalf("windows = %+v, want one entry per window", obs.Windows)
	}
	for _, window := range obs.Windows {
		var want BucketState
		for _, state := range states {
			if state.Key == window.Key {
				want = state
			}
		}
		if window.UsedPercent != want.UsedPercent || !window.ResetsAt.Equal(*want.ResetsAt) || !window.ObservedAt.Equal(want.ObservedAt) {
			t.Fatalf("window %s = %+v, want the values of its own observation %+v", window.Window, window, want)
		}
	}
	fields := QuotaTrailerFields(obs)
	if fields["percent"] != "97" || fields["resetsAt"] != later.Format(time.RFC3339) || fields["window"] != week.String() {
		t.Fatalf("trailer fields mix windows: %v", fields)
	}
}

// F3: a quota wait was met by a stale window that was present, or by a
// declared weekly window that was missing. Only a complete, fresh window set
// below the threshold meets it, and the pending reason says what is wrong.
func TestQuotaWaitNeedsACompleteFreshWindowSet(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	reset := now.Add(72 * time.Hour)
	five := BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"}
	week := BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "seven_day"}
	pools := []QuotaPool{{ID: "pool-claude", Provider: "claude", BucketSelection: BucketSelectionResolved, Buckets: []BucketKey{five, week}}}
	freshFive := BucketState{Key: five, Phase: PhaseNormal, UsedPercent: 10, ObservedAt: now.Add(-time.Minute)}
	below := QuotaWaitCondition{Pool: "pool-claude", Below: percent(50)}
	normal := QuotaWaitCondition{Pool: "pool-claude", Phase: PhaseNormal}
	for _, tc := range []struct {
		name      string
		condition QuotaWaitCondition
		states    []BucketState
		met       bool
		reason    string
	}{
		{name: "stale weekly window present", condition: below, states: []BucketState{freshFive, {Key: week, Phase: PhaseNormal, UsedPercent: 5, ResetsAt: &reset, ObservedAt: now.Add(-2 * time.Hour)}},
			reason: "stale seven_day"},
		{name: "declared weekly window missing", condition: below, states: []BucketState{freshFive}, reason: "missing seven_day"},
		{name: "phase wait with stale weekly window", condition: normal, states: []BucketState{freshFive, {Key: week, Phase: PhaseNormal, UsedPercent: 5, ResetsAt: &reset, ObservedAt: now.Add(-2 * time.Hour)}},
			reason: "stale seven_day"},
		{name: "fresh and complete below", condition: below, states: []BucketState{freshFive, {Key: week, Phase: PhaseNormal, UsedPercent: 49.9, ResetsAt: &reset, ObservedAt: now}},
			met: true, reason: "below 50%"},
		{name: "fresh and complete at the threshold", condition: below, states: []BucketState{freshFive, {Key: week, Phase: PhaseNormal, UsedPercent: 50, ResetsAt: &reset, ObservedAt: now}},
			reason: "not below 50%"},
		{name: "fresh and complete normal", condition: normal, states: []BucketState{freshFive, {Key: week, Phase: PhaseNormal, UsedPercent: 70, ResetsAt: &reset, ObservedAt: now}},
			met: true, reason: "is normal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, outcome, reason, err := EvaluateQuotaWait(tc.condition, pools, tc.states, now, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			if (outcome == TaskWaitMet) != tc.met || !strings.Contains(reason, tc.reason) {
				t.Fatalf("outcome=%q reason=%q, want met=%v and %q", outcome, reason, tc.met, tc.reason)
			}
		})
	}
}

// The 2026-10-04 incident: a Claude seven-day reading of 97% went stale while
// the window was still open, and was read as current. A manual mid-window
// refresh then reported the real usage with the same reset time. Before the
// refresh nothing meets a wait; after it the wait is met.
func TestStaleNinetySevenThenManualRefreshMeetsTheWaitOnlyAfterTheRefresh(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	reset := time.Date(2026, 10, 8, 16, 0, 0, 0, time.UTC)
	five := BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"}
	week := BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "seven_day"}
	pools := []QuotaPool{{ID: "pool-claude", Provider: "claude", BucketSelection: BucketSelectionResolved, Buckets: []BucketKey{five, week}}}
	local := []BucketState{
		{Key: five, Phase: PhaseNormal, UsedPercent: 4, ObservedAt: now.Add(-time.Minute)},
		{Key: week, Phase: PhaseDraining, UsedPercent: 97, ResetsAt: &reset, ObservedAt: now.Add(-5 * time.Hour)},
	}
	wait := QuotaWaitCondition{Pool: "pool-claude", Below: percent(90)}
	_, outcome, reason, err := EvaluateQuotaWait(wait, pools, local, now, time.Hour)
	if err != nil || outcome != "" || !strings.Contains(reason, "stale seven_day") {
		t.Fatalf("before the refresh: outcome=%q reason=%q err=%v", outcome, reason, err)
	}
	refresh := WorkerSnapshot{WorkerID: "omarchy-pc", QuotaObservations: []WorkerQuotaObservation{{
		Key: week, Phase: PhaseNormal, UsedPercent: 12, ResetsAt: &reset, ObservedAt: now, Healthy: true,
	}}}
	merged := MergeQuotaObservations(local, []WorkerSnapshot{refresh})
	_, outcome, reason, err = EvaluateQuotaWait(wait, pools, merged, now, time.Hour)
	if err != nil || outcome != TaskWaitMet {
		t.Fatalf("after the refresh: outcome=%q reason=%q err=%v", outcome, reason, err)
	}
}
