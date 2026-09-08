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

func TestBuildAttributesRisesAndIgnoresJitter(t *testing.T) {
	key := domain.BucketKey{ProviderInstanceID: "claudeAgent", LimitID: "claude", Window: "five_hour"}
	loc := time.UTC
	wed := time.Date(2030, 1, 2, 10, 0, 0, 0, loc) // Wednesday 10:00, peak
	reset := wed.Add(4 * time.Hour)
	obs := []domain.Observation{
		{Key: key, ObservedAt: wed, UsedPercent: 10, ResetsAt: &reset, EventID: "1", ThreadID: "a", Model: "opus"},
		{Key: key, ObservedAt: wed.Add(10 * time.Minute), UsedPercent: 20, ResetsAt: &reset, EventID: "2", ThreadID: "a", Model: "opus"},
		{Key: key, ObservedAt: wed.Add(20 * time.Minute), UsedPercent: 19, ResetsAt: &reset, EventID: "3", ThreadID: "b", Model: "sonnet"}, // jitter down
		{Key: key, ObservedAt: wed.Add(30 * time.Minute), UsedPercent: 20, ResetsAt: &reset, EventID: "4", ThreadID: "b", Model: "sonnet"}, // back up: not a rise
		{Key: key, ObservedAt: wed.Add(40 * time.Minute), UsedPercent: 25, ResetsAt: &reset, EventID: "5", ThreadID: "b", Model: "sonnet"},
		// Evening, off-peak, same window.
		{Key: key, ObservedAt: wed.Add(9 * time.Hour), UsedPercent: 30, ResetsAt: &reset, EventID: "6", ThreadID: "a", Model: "opus"},
		// New window: the drop is a reset, not consumption.
		{Key: key, ObservedAt: wed.Add(10 * time.Hour), UsedPercent: 3, ResetsAt: ptr(reset.Add(5 * time.Hour)), EventID: "7", ThreadID: "a", Model: "opus"},
		{Key: key, ObservedAt: wed.Add(11 * time.Hour), UsedPercent: 8, ResetsAt: ptr(reset.Add(5 * time.Hour)), EventID: "8", ThreadID: "a", Model: "opus"},
	}
	usage := []domain.UsageSample{
		{ProviderInstanceID: "claudeAgent", ThreadID: "a", Model: "opus", ObservedAt: wed.Add(5 * time.Minute), SourceEventID: "u1", InputTokens: 1000, OutputTokens: 500},
		{ProviderInstanceID: "claudeAgent", ThreadID: "a", Model: "opus", ObservedAt: wed.Add(9 * time.Hour), SourceEventID: "u2", OutputTokens: 200},
		{ProviderInstanceID: "codex", ThreadID: "c", Model: "gpt", ObservedAt: wed, SourceEventID: "u3", OutputTokens: 999}, // other provider
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
	if b.Peak.FreshTokens != 1500 || b.OffPeak.FreshTokens != 200 {
		t.Fatalf("tokens peak=%d offpeak=%d", b.Peak.FreshTokens, b.OffPeak.FreshTokens)
	}
	if b.Peak.ActiveHours != 1 || b.OffPeak.ActiveHours != 2 {
		t.Fatalf("active hours peak=%d offpeak=%d", b.Peak.ActiveHours, b.OffPeak.ActiveHours)
	}
	if b.WeekdayHours[10].Percent != 15 || b.WeekendHours[10].Percent != 0 {
		t.Fatalf("hour 10 = %+v", b.WeekdayHours[10])
	}
	if len(b.ByModel) != 2 || b.ByModel[0].Name != "opus" || b.ByModel[0].Percent != 20 || b.ByModel[1].Percent != 5 {
		t.Fatalf("by model = %+v", b.ByModel)
	}
	cw := b.CurrentWindow
	if cw == nil || cw.StartPercent != 3 || cw.Percent != 8 || len(cw.ByModel) != 1 || cw.ByModel[0].Percent != 5 {
		t.Fatalf("current window = %+v", cw)
	}
}

func ptr(t time.Time) *time.Time { return &t }
