package providerlog

import (
	"errors"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-quota-watchdog/internal/domain"
)

func testdata(name string) string {
	return filepath.Join("..", "..", "..", "testdata", name)
}

func find(snaps []domain.QuotaSnapshot, window string) *domain.QuotaSnapshot {
	for i := range snaps {
		if snaps[i].Key.Window == window {
			return &snaps[i]
		}
	}
	return nil
}

func TestParseCodexReal(t *testing.T) {
	snaps, err := ReadFile(testdata("samples/codex-real.log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 4 {
		t.Fatalf("got %d snapshots, want 4 (2 records x 2 windows)", len(snaps))
	}
	p := find(snaps[:2], domain.WindowPrimary)
	if p == nil {
		t.Fatal("no primary window")
	}
	if p.Key.ProviderInstanceID != "codex" || p.Key.LimitID != "codex" {
		t.Fatalf("key = %+v", p.Key)
	}
	if p.UsedPercent != 47 {
		t.Fatalf("primary used = %v", p.UsedPercent)
	}
	if p.WindowDuration != 300*time.Minute {
		t.Fatalf("primary duration = %v", p.WindowDuration)
	}
	if p.ResetsAt == nil || p.ResetsAt.Unix() != 1788898977 {
		t.Fatalf("primary resetsAt = %v", p.ResetsAt)
	}
	if p.ModelSelector != "" {
		t.Fatalf("codex limits are account-wide, got selector %q", p.ModelSelector)
	}
	s := find(snaps[:2], domain.WindowSecondary)
	if s == nil || s.UsedPercent != 7 || s.WindowDuration != 10080*time.Minute {
		t.Fatalf("secondary = %+v", s)
	}
	if p.SourceEventID == "" || p.ObservedAt.IsZero() {
		t.Fatalf("missing identity: %+v", p)
	}
}

func TestParseClaudeReal(t *testing.T) {
	snaps, err := ReadFile(testdata("samples/claude-real.log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) < 4 {
		t.Fatalf("got %d snapshots", len(snaps))
	}
	five := find(snaps, "five_hour")
	if five == nil {
		t.Fatal("no five_hour window")
	}
	if five.Key.ProviderInstanceID != "claudeAgent" || five.Key.LimitID != "claude" {
		t.Fatalf("key = %+v", five.Key)
	}
	if five.UsedPercent <= 0 || five.UsedPercent > 100 {
		t.Fatalf("five_hour used = %v", five.UsedPercent)
	}
	if five.WindowDuration != 5*time.Hour || five.ResetsAt == nil {
		t.Fatalf("five_hour = %+v", five)
	}
	week := find(snaps, "seven_day")
	if week == nil || week.WindowDuration != 7*24*time.Hour {
		t.Fatalf("seven_day = %+v", week)
	}
}

func TestClaudeFractionScale(t *testing.T) {
	body := `{"type":"account.rate-limits.updated","eventId":"e1","provider":"claudeAgent","createdAt":"2030-01-01T00:00:00Z","payload":{"rateLimits":{"type":"rate_limit_event","rate_limit_info":{"status":"allowed","rateLimitType":"five_hour","unifiedWindows":{"five_hour":{"utilization":0.88,"resetsAt":1893474000},"seven_day_opus":{"utilization":0.5,"resetsAt":1893906000}}}}}}`
	snaps, err := ParseJSON([]byte(body), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	five := find(snaps, "five_hour")
	if five == nil || math.Abs(five.UsedPercent-88) > 0.001 {
		t.Fatalf("five_hour = %+v", five)
	}
	opus := find(snaps, "seven_day_opus")
	if opus == nil || opus.UsedPercent != 50 || opus.ModelSelector != "opus" {
		t.Fatalf("seven_day_opus = %+v", opus)
	}
}

func TestClaudePercentScaleWhenAboveOne(t *testing.T) {
	body := `{"type":"account.rate-limits.updated","eventId":"e2","provider":"claudeAgent","createdAt":"2030-01-01T00:00:00Z","payload":{"rateLimits":{"type":"rate_limit_event","rate_limit_info":{"status":"allowed","rateLimitType":"five_hour","unifiedWindows":{"five_hour":{"utilization":88,"resetsAt":1893474000},"seven_day":{"utilization":15,"resetsAt":1893906000}}}}}}`
	snaps, err := ParseJSON([]byte(body), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if five := find(snaps, "five_hour"); five == nil || five.UsedPercent != 88 {
		t.Fatalf("five_hour = %+v", five)
	}
}

func TestClaudeFlatFallbackAndRejected(t *testing.T) {
	body := `{"type":"account.rate-limits.updated","eventId":"e3","provider":"claudeAgent","createdAt":"2030-01-01T00:00:00Z","payload":{"rateLimits":{"type":"rate_limit_event","rate_limit_info":{"status":"rejected","rateLimitType":"seven_day","utilization":97,"resetsAt":1893906000}}}}`
	snaps, err := ParseJSON([]byte(body), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 1 || snaps[0].Key.Window != "seven_day" || snaps[0].UsedPercent != 100 {
		t.Fatalf("snaps = %+v", snaps)
	}
}

func TestCodexSparseUpdateAndSpending(t *testing.T) {
	body := `{"type":"account.rate-limits.updated","eventId":"e4","provider":"codex","providerInstanceId":"codex","createdAt":"2030-01-01T00:00:00Z","payload":{"rateLimits":{"rateLimits":{"limitId":"codex","secondary":{"resetsAt":1893906000,"usedPercent":91,"windowDurationMins":10080},"individualLimit":{"limit":"100","remainingPercent":12.5,"resetsAt":1893906000,"used":"87.5"}}}}}`
	snaps, err := ParseJSON([]byte(body), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if find(snaps, domain.WindowPrimary) != nil {
		t.Fatal("absent primary window must not produce a snapshot")
	}
	if s := find(snaps, domain.WindowSecondary); s == nil || s.UsedPercent != 91 {
		t.Fatalf("secondary = %+v", s)
	}
	if s := find(snaps, domain.WindowSpending); s == nil || s.UsedPercent != 87.5 {
		t.Fatalf("spending = %+v", s)
	}
}

func TestCodexLimitReached(t *testing.T) {
	body := `{"type":"account.rate-limits.updated","eventId":"e5","provider":"codex","createdAt":"2030-01-01T00:00:00Z","payload":{"rateLimits":{"rateLimits":{"limitId":"codex","primary":{"resetsAt":1893474000,"usedPercent":99,"windowDurationMins":300},"rateLimitReachedType":"rate_limit_reached"}}}}`
	snaps, err := ParseJSON([]byte(body), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if s := find(snaps, domain.WindowPrimary); s == nil || s.UsedPercent != 100 {
		t.Fatalf("primary = %+v", s)
	}
}

func TestNonRateLimitLinesAreCheap(t *testing.T) {
	_, err := ParseLine(`[2030-01-01T00:00:00Z] NTIVE: {"type":"content.delta"}`)
	if !errors.Is(err, ErrNotRateLimit) {
		t.Fatalf("err = %v", err)
	}
	_, err = ParseLine(`[2030-01-01T00:00:00Z] CANON: {"type":"turn.completed"}`)
	if !errors.Is(err, ErrNotRateLimit) {
		t.Fatalf("err = %v", err)
	}
}

func TestMalformedRecord(t *testing.T) {
	_, err := ParseLine(`[2030-01-01T00:00:00Z] CANON: {"type":"account.rate-limits.updated","eventId":`)
	if err == nil || errors.Is(err, ErrNotRateLimit) {
		t.Fatalf("err = %v", err)
	}
}

func TestClaudeModelSelector(t *testing.T) {
	cases := map[string]string{
		"five_hour":                  "",
		"seven_day":                  "",
		"seven_day_opus":             "opus",
		"seven_day_sonnet":           "sonnet",
		"seven_day_overage_included": "",
		"seven_day_oauth_apps":       "",
		"overage":                    "",
	}
	for in, want := range cases {
		if got := claudeModelSelector(in); got != want {
			t.Errorf("%s: got %q want %q", in, got, want)
		}
	}
}
