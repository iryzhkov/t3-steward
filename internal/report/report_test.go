package report

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-quota-watchdog/internal/domain"
)

func TestParseSchedule(t *testing.T) {
	s, err := ParseSchedule("Mon-Fri 09:00-17:00")
	if err != nil {
		t.Fatal(err)
	}
	if !s.Days[time.Monday] || !s.Days[time.Friday] || s.Days[time.Saturday] || s.Start != 9 || s.End != 17 {
		t.Fatalf("schedule = %+v", s)
	}
	loc := time.UTC
	if !s.Contains(time.Date(2030, 1, 2, 9, 0, 0, 0, loc)) { // Wednesday
		t.Fatal("09:00 Wednesday should be peak")
	}
	if s.Contains(time.Date(2030, 1, 2, 17, 0, 0, 0, loc)) {
		t.Fatal("17:00 should be off-peak")
	}
	if s.Contains(time.Date(2030, 1, 5, 12, 0, 0, 0, loc)) { // Saturday
		t.Fatal("Saturday should be off-peak")
	}
	if _, err := ParseSchedule("Fri-Mon 22:00-06:00"); err == nil {
		t.Fatal("end before start accepted")
	}
	wrap, err := ParseSchedule("Sat-Mon 00:00-24:00")
	if err != nil || !wrap.Days[time.Sunday] || wrap.Days[time.Tuesday] {
		t.Fatalf("wrap-around days = %+v err=%v", wrap, err)
	}
}

var fiveHour = domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"}

func call(thread, model string, at time.Time, id string, cacheWrite, output int64) domain.UsageSample {
	return domain.UsageSample{ProviderInstanceID: "claudeAgent", ThreadID: thread, Model: model, ObservedAt: at,
		SourceEventID: id, Kind: domain.UsageKindCall, CacheWriteTokens: cacheWrite, OutputTokens: output}
}

func TestBuildSplitsRisesByTokensAndIgnoresJitter(t *testing.T) {
	loc := time.UTC
	wed := time.Date(2030, 1, 2, 10, 0, 0, 0, loc) // Wednesday 10:00, peak
	reset := wed.Add(4 * time.Hour)
	obs := []domain.Observation{
		{Key: fiveHour, ObservedAt: wed, UsedPercent: 10, ResetsAt: &reset, EventID: "1", ThreadID: "a", Model: "opus"},
		// Both threads ran in this interval with equal cost: split 5/5.
		{Key: fiveHour, ObservedAt: wed.Add(10 * time.Minute), UsedPercent: 20, ResetsAt: &reset, EventID: "2", ThreadID: "a", Model: "opus"},
		{Key: fiveHour, ObservedAt: wed.Add(20 * time.Minute), UsedPercent: 19, ResetsAt: &reset, EventID: "3", ThreadID: "b", Model: "sonnet"}, // jitter down
		{Key: fiveHour, ObservedAt: wed.Add(30 * time.Minute), UsedPercent: 20, ResetsAt: &reset, EventID: "4", ThreadID: "b", Model: "sonnet"}, // back up: not a rise
		// Only b ran here.
		{Key: fiveHour, ObservedAt: wed.Add(40 * time.Minute), UsedPercent: 25, ResetsAt: &reset, EventID: "5", ThreadID: "b", Model: "sonnet"},
		// Evening, off-peak, same window, nobody ran: outside T3.
		{Key: fiveHour, ObservedAt: wed.Add(9 * time.Hour), UsedPercent: 30, ResetsAt: &reset, EventID: "6", ThreadID: "a", Model: "opus"},
		// New window: the drop is a reset, not consumption.
		{Key: fiveHour, ObservedAt: wed.Add(10 * time.Hour), UsedPercent: 3, ResetsAt: ptr(reset.Add(5 * time.Hour)), EventID: "7", ThreadID: "a", Model: "opus"},
		{Key: fiveHour, ObservedAt: wed.Add(11 * time.Hour), UsedPercent: 8, ResetsAt: ptr(reset.Add(5 * time.Hour)), EventID: "8", ThreadID: "a", Model: "opus"},
	}
	usage := []domain.UsageSample{
		call("a", "opus", wed.Add(5*time.Minute), "u1", 1000, 0),
		call("b", "sonnet", wed.Add(6*time.Minute), "u2", 1000, 0),
		call("b", "sonnet", wed.Add(35*time.Minute), "u3", 500, 0),
		call("a", "opus", wed.Add(10*time.Hour+30*time.Minute), "u4", 700, 0),
		{ProviderInstanceID: "codex", ThreadID: "c", Model: "gpt", ObservedAt: wed, SourceEventID: "u5", Kind: domain.UsageKindCall, OutputTokens: 999}, // other provider
	}
	peak, _ := ParseSchedule("Mon-Fri 09:00-17:00")
	rep := Build(Input{Observations: obs, Usage: usage, Location: loc, Peak: peak, From: wed.Add(-time.Hour), To: wed.Add(24 * time.Hour)})
	if len(rep.Buckets) != 1 {
		t.Fatalf("buckets = %d", len(rep.Buckets))
	}
	b := rep.Buckets[0]
	if b.Total != 25 { // 10 + 5 + 5 + 5
		t.Fatalf("total = %v", b.Total)
	}
	if b.Peak.Percent != 15 || b.OffPeak.Percent != 10 {
		t.Fatalf("peak=%v offpeak=%v", b.Peak.Percent, b.OffPeak.Percent)
	}
	if b.Unattributed != 5 {
		t.Fatalf("unattributed = %v", b.Unattributed)
	}
	if b.Fit.Fitted {
		t.Fatal("fit should not be trusted with 3 rows")
	}
	want := map[string]float64{"opus": 10, "sonnet": 10, "outside T3 or no token data": 5}
	for _, s := range b.ByModel {
		if want[s.Name] != s.Percent {
			t.Fatalf("model %s = %v, want %v (all: %+v)", s.Name, s.Percent, want[s.Name], b.ByModel)
		}
		delete(want, s.Name)
	}
	if len(want) != 0 {
		t.Fatalf("missing models: %v", want)
	}
	if b.Peak.FreshTokens != 2500 || b.OffPeak.FreshTokens != 700 {
		t.Fatalf("tokens peak=%d offpeak=%d", b.Peak.FreshTokens, b.OffPeak.FreshTokens)
	}
	cw := b.CurrentWindow
	if cw == nil || cw.StartPercent != 3 || cw.Percent != 8 || len(cw.ByModel) != 1 || cw.ByModel[0].Name != "opus" || cw.ByModel[0].Percent != 5 {
		t.Fatalf("current window = %+v", cw)
	}
}

func TestSpreadTurnsCoversSubagentWork(t *testing.T) {
	loc := time.UTC
	start := time.Date(2030, 1, 2, 10, 0, 0, 0, loc)
	reset := start.Add(4 * time.Hour)
	// Thread a's parent makes one call at 10:00, then only subagents run
	// until the turn result at 10:30. Readings every 10 minutes rise 3%
	// each; without spreading, the 10:10-10:30 rises would be unattributed.
	obs := []domain.Observation{
		{Key: fiveHour, ObservedAt: start, UsedPercent: 10, ResetsAt: &reset, EventID: "1", ThreadID: "a"},
		{Key: fiveHour, ObservedAt: start.Add(10 * time.Minute), UsedPercent: 13, ResetsAt: &reset, EventID: "2", ThreadID: "a"},
		{Key: fiveHour, ObservedAt: start.Add(20 * time.Minute), UsedPercent: 16, ResetsAt: &reset, EventID: "3", ThreadID: "a"},
		{Key: fiveHour, ObservedAt: start.Add(30 * time.Minute), UsedPercent: 19, ResetsAt: &reset, EventID: "4", ThreadID: "a"},
	}
	usage := []domain.UsageSample{
		call("a", "", start.Add(time.Minute), "c1", 100, 10),
		{ProviderInstanceID: "claudeAgent", ThreadID: "a", Model: "fable", ObservedAt: start.Add(30 * time.Minute), SourceEventID: "r1#fable",
			Kind: domain.UsageKindTurn, CacheWriteTokens: 3100, OutputTokens: 310},
	}
	peak, _ := ParseSchedule("Mon-Fri 09:00-17:00")
	rep := Build(Input{Observations: obs, Usage: usage, Location: loc, Peak: peak, From: start.Add(-time.Hour), To: start.Add(24 * time.Hour)})
	b := rep.Buckets[0]
	if b.Total != 9 || b.Unattributed != 0 {
		t.Fatalf("total=%v unattributed=%v", b.Total, b.Unattributed)
	}
	if len(b.ByModel) != 1 || b.ByModel[0].Name != "fable" || b.ByModel[0].Percent != 9 {
		t.Fatalf("by model = %+v", b.ByModel)
	}
	// The turn total (3100/310) covers the parent's call (100/10) plus the
	// spread remainder (3000/300): no double counting.
	if b.Peak.FreshTokens != 3410 {
		t.Fatalf("fresh tokens = %d", b.Peak.FreshTokens)
	}
}

func TestFitPrefersScaledRatiosWhenEquivalent(t *testing.T) {
	// Rises exactly proportional to list-price cost: the scaled model is
	// as good as the free fit and must be preferred for stability.
	var rows []interval
	for i := 1; i <= 40; i++ {
		r := interval{bin: string(rune('a'+i%26)) + string(rune('a'+i/26))}
		r.tokens = [4]float64{float64(i) * 0.1, float64(i) * 0.05, float64(i) * 2, float64(i) * 0.02}
		r.rise = 2 * (DefaultWeights.Input*r.tokens[0] + DefaultWeights.CacheWrite*r.tokens[1] + DefaultWeights.CacheRead*r.tokens[2] + DefaultWeights.Output*r.tokens[3])
		rows = append(rows, r)
	}
	fit := fitWeights(rows)
	if !fit.Fitted || fit.R2 < 0.99 {
		t.Fatalf("fit = %+v", fit)
	}
	if d := fit.Weights.Output/fit.Weights.Input - 5; d > 0.01 || d < -0.01 {
		t.Fatalf("ratios not preserved: %+v", fit.Weights)
	}
}

func ptr(t time.Time) *time.Time { return &t }
