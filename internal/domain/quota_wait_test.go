package domain

import (
	"strings"
	"testing"
	"time"
)

func percent(v float64) *float64 { return &v }

// Exactly one of --below, --phase normal and --reset names the condition.
func TestQuotaWaitConditionNeedsExactlyOneCondition(t *testing.T) {
	for _, bad := range []QuotaWaitCondition{
		{Pool: "p"},
		{Pool: "p", Below: percent(50), Reset: true},
		{Pool: "p", Below: percent(50), Phase: PhaseNormal},
		{Pool: "p", Phase: PhaseStopped},
		{Pool: "p", Below: percent(0)},
		{Pool: "p", Below: percent(101)},
		{Pool: "", Below: percent(50)},
	} {
		if err := bad.Validate(); err == nil {
			t.Fatalf("%+v was accepted", bad)
		}
	}
	for _, good := range []QuotaWaitCondition{
		{Pool: "p", Below: percent(50)},
		{Pool: "p", Phase: PhaseNormal},
		{Pool: "p", Reset: true},
	} {
		if err := good.Validate(); err != nil {
			t.Fatalf("%+v refused: %v", good, err)
		}
		if !strings.HasPrefix(good.String(), "quota p ") {
			t.Fatalf("condition text = %q", good.String())
		}
	}
}

// The pool observation is the worst of its buckets: the highest phase and
// the highest percent, with the earliest reset.
func TestObserveQuotaPoolAndEvaluate(t *testing.T) {
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	soon, later := now.Add(time.Hour), now.Add(5*time.Hour)
	five := BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"}
	week := BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "seven_day"}
	other := BucketKey{ProviderInstanceID: "codex", LimitID: "codex", Window: "primary"}
	pool := QuotaPool{ID: "claude", ProviderInstanceIDs: []string{"claudeAgent"}, Buckets: []BucketKey{five, week}}
	states := []BucketState{
		{Key: five, Phase: PhaseNormal, UsedPercent: 30, ResetsAt: &soon, ObservedAt: now},
		{Key: week, Phase: PhaseWarned, UsedPercent: 82, ResetsAt: &later, ObservedAt: now},
		{Key: other, Phase: PhaseStopped, UsedPercent: 99, ObservedAt: now},
	}
	obs := ObserveQuotaPool(pool, states)
	if obs.Pool != "claude" || obs.Phase != PhaseWarned || obs.Percent != 82 || obs.Buckets != 2 || obs.ResetsAt == nil || !obs.ResetsAt.Equal(soon) {
		t.Fatalf("observation = %+v", obs)
	}
	fields := QuotaTrailerFields(obs)
	if fields["pool"] != "claude" || fields["phase"] != "warned" || fields["percent"] != "82" {
		t.Fatalf("fields = %v", fields)
	}
	if outcome, _ := (QuotaWaitCondition{Pool: "claude", Below: percent(90)}).Evaluate(obs, now); outcome != TaskWaitMet {
		t.Fatalf("below 90 at 82 = %q", outcome)
	}
	if outcome, _ := (QuotaWaitCondition{Pool: "claude", Below: percent(50)}).Evaluate(obs, now); outcome != "" {
		t.Fatalf("below 50 at 82 = %q", outcome)
	}
	if outcome, _ := (QuotaWaitCondition{Pool: "claude", Phase: PhaseNormal}).Evaluate(obs, now); outcome != "" {
		t.Fatalf("phase normal while warned = %q", outcome)
	}
	states[1].Phase = PhaseNormal
	if outcome, _ := (QuotaWaitCondition{Pool: "claude", Phase: PhaseNormal}).Evaluate(ObserveQuotaPool(pool, states), now); outcome != TaskWaitMet {
		t.Fatalf("phase normal = %q", outcome)
	}
	reset := QuotaWaitCondition{Pool: "claude", Reset: true, ResetAt: &soon}
	if outcome, _ := reset.Evaluate(obs, now); outcome != "" {
		t.Fatalf("reset before the window = %q", outcome)
	}
	if outcome, _ := reset.Evaluate(obs, soon); outcome != TaskWaitMet {
		t.Fatalf("reset at the window = %q", outcome)
	}
	// A pool nobody observes stays pending.
	if outcome, reason := (QuotaWaitCondition{Pool: "claude", Below: percent(90)}).Evaluate(ObserveQuotaPool(pool, nil), now); outcome != "" || !strings.Contains(reason, "no observation") {
		t.Fatalf("unobserved pool = %q (%s)", outcome, reason)
	}
	// A pool with no bucket list falls back to its provider instances.
	bare := QuotaPool{ID: "claude", ProviderInstanceIDs: []string{"claudeAgent"}}
	if got := ObserveQuotaPool(bare, states); got.Buckets != 2 {
		t.Fatalf("fallback observation = %+v", got)
	}
}

// The freshest reading per bucket wins whichever host observed it; the
// function moved here from the quota bridge so the store can use it.
func TestMergeQuotaObservationsFreshestWins(t *testing.T) {
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	key := BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "seven_day"}
	local := []BucketState{{Key: key, Phase: PhaseNormal, UsedPercent: 40, ObservedAt: now.Add(-2 * time.Hour)}}
	worker := WorkerSnapshot{WorkerID: "w", QuotaObservations: []WorkerQuotaObservation{{Key: key, Phase: PhaseStopped, UsedPercent: 97, ObservedAt: now}}}
	merged := MergeQuotaObservations(local, []WorkerSnapshot{worker})
	if len(merged) != 1 || merged[0].UsedPercent != 97 || merged[0].Phase != PhaseStopped {
		t.Fatalf("merged = %+v", merged)
	}
	local[0].ObservedAt = now.Add(time.Minute)
	if merged := MergeQuotaObservations(local, []WorkerSnapshot{worker}); len(merged) != 1 || merged[0].UsedPercent != 40 {
		t.Fatalf("an older worker reading replaced a fresher local one: %+v", merged)
	}
}
