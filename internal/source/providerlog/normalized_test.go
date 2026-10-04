package providerlog

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func normalizedEvent(provider, limits string) string {
	return fmt.Sprintf(`{"type":"account.rate-limits.updated","eventId":"normalized-test","provider":%q,"providerInstanceId":"isolated-instance","threadId":"disposable-thread","createdAt":"2030-01-01T00:00:00Z","payload":{"limits":%s}}`, provider, limits)
}

func TestNormalizedObservedCodex(t *testing.T) {
	snaps, err := ReadFile(testdata("samples/codex-0045.log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 1 {
		t.Fatalf("snapshots = %+v", snaps)
	}
	s := snaps[0]
	if s.Key.ProviderInstanceID != "codex" || s.Key.LimitID != "codex" || s.Key.Window != "primary" || s.UsedPercent != 5 || s.WindowDuration != 168*time.Hour || s.ResetsAt == nil || s.ResetsAt.Format(time.RFC3339) != "2026-10-09T21:30:00Z" {
		t.Fatalf("snapshot = %+v", s)
	}
}

func TestNormalizedLiveProviderFixtures(t *testing.T) {
	for _, c := range []struct {
		file, provider, window string
		used                   float64
	}{
		{"codex-0045-live.log", "codex", "primary", 5},
		{"claude-0045.log", "claudeAgent", "seven_day", 96},
	} {
		t.Run(c.provider, func(t *testing.T) {
			snaps, err := ReadFile(testdata("samples/" + c.file))
			if err != nil {
				t.Fatal(err)
			}
			if len(snaps) != 1 {
				t.Fatalf("snapshots = %+v", snaps)
			}
			s := snaps[0]
			if s.Key.ProviderInstanceID != c.provider || s.Key.Window != c.window || s.UsedPercent != c.used || s.WindowDuration != 168*time.Hour || s.ResetsAt == nil {
				t.Fatalf("snapshot = %+v", s)
			}
		})
	}
}

func TestNormalizedWindows(t *testing.T) {
	for _, provider := range []string{"codex", "claudeAgent"} {
		t.Run(provider, func(t *testing.T) {
			snaps, err := ParseJSON([]byte(normalizedEvent(provider, `{"windows":[{"id":"primary","kind":"weekly","label":"Weekly","usedPercent":0.5,"windowDurationMins":10080,"resetsAt":"2030-01-09T21:30:00.123+02:00"},{"id":"seven_day_opus","kind":"weekly","label":"Weekly · Opus","usedPercent":96},{"id":"custom","kind":"other","label":"Custom","usedPercent":100}]}`)), time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if len(snaps) != 3 {
				t.Fatalf("snapshots = %+v", snaps)
			}
			p := find(snaps, "primary")
			if p.Key.ProviderInstanceID != "isolated-instance" || p.UsedPercent != 0.5 || p.WindowDuration != 168*time.Hour || p.SourceEventID != "normalized-test" || p.ThreadID != "disposable-thread" || p.ResetsAt == nil || p.ResetsAt.UTC().Format(time.RFC3339Nano) != "2030-01-09T19:30:00.123Z" {
				t.Fatalf("primary = %+v", p)
			}
			opus := find(snaps, "seven_day_opus")
			want := ""
			if provider == "claudeAgent" {
				want = "opus"
			}
			if opus.ModelSelector != want {
				t.Fatalf("selector = %q, want %q", opus.ModelSelector, want)
			}
			if opus.ResetsAt != nil || opus.WindowDuration != 0 {
				t.Fatalf("optional fields invented: %+v", opus)
			}
		})
	}
}

func TestNormalizedInstancesAndAmbiguousPayload(t *testing.T) {
	limits := `{"windows":[{"id":"primary","kind":"weekly","label":"Weekly","usedPercent":96}]}`
	body := normalizedEvent("codex", limits)
	a, err := ParseJSON([]byte(body), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := ParseJSON([]byte(strings.Replace(body, "isolated-instance", "other-instance", 1)), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if a[0].Key == b[0].Key {
		t.Fatal("distinct instances collapsed")
	}
	_, err = ParseJSON([]byte(strings.Replace(body, `"limits":`, `"rateLimits":{"rateLimits":{"limitId":"spark","primary":{"usedPercent":96}}},"limits":`, 1)), time.Time{})
	if err == nil {
		t.Fatal("mixed populated limits must not silently drop a bucket")
	}
}

func TestNormalizedSparseAndMalformed(t *testing.T) {
	cases := []struct {
		name, limits string
		fail         bool
	}{
		{"empty", `{"windows":[]}`, false},
		{"single", `{"windows":[{"id":"secondary","kind":"weekly","label":"Weekly","usedPercent":91}]}`, false},
		{"missing windows", `{}`, true},
		{"null limits", `null`, true},
		{"null windows", `{"windows":null}`, true},
		{"null window", `{"windows":[null]}`, true},
		{"missing usage", `{"windows":[{"id":"primary","kind":"weekly","label":"Weekly"}]}`, true},
		{"null usage", `{"windows":[{"id":"primary","kind":"weekly","label":"Weekly","usedPercent":null}]}`, true},
		{"missing id", `{"windows":[{"kind":"weekly","label":"Weekly","usedPercent":96}]}`, true},
		{"bad kind", `{"windows":[{"id":"primary","kind":"future","label":"Weekly","usedPercent":96}]}`, true},
		{"missing label", `{"windows":[{"id":"primary","kind":"weekly","usedPercent":96}]}`, true},
		{"bad percent", `{"windows":[{"id":"primary","kind":"weekly","label":"Weekly","usedPercent":101}]}`, true},
		{"negative percent", `{"windows":[{"id":"primary","kind":"weekly","label":"Weekly","usedPercent":-1}]}`, true},
		{"string percent", `{"windows":[{"id":"primary","kind":"weekly","label":"Weekly","usedPercent":"96"}]}`, true},
		{"bad reset", `{"windows":[{"id":"primary","kind":"weekly","label":"Weekly","usedPercent":96,"resetsAt":"invalid"}]}`, true},
		{"numeric reset", `{"windows":[{"id":"primary","kind":"weekly","label":"Weekly","usedPercent":96,"resetsAt":1893906000}]}`, true},
		{"null reset", `{"windows":[{"id":"primary","kind":"weekly","label":"Weekly","usedPercent":96,"resetsAt":null}]}`, true},
		{"negative duration", `{"windows":[{"id":"primary","kind":"weekly","label":"Weekly","usedPercent":96,"windowDurationMins":-1}]}`, true},
		{"fractional duration", `{"windows":[{"id":"primary","kind":"weekly","label":"Weekly","usedPercent":96,"windowDurationMins":1.5}]}`, true},
		{"overflow duration", `{"windows":[{"id":"primary","kind":"weekly","label":"Weekly","usedPercent":96,"windowDurationMins":999999999999}]}`, true},
		{"unsupported populated limit", `{"windows":[],"individualLimit":{"remainingPercent":0}}`, true},
		{"unsupported model identity", `{"windows":[],"limitId":"spark"}`, true},
		{"duplicate", `{"windows":[{"id":"primary","kind":"weekly","label":"Weekly","usedPercent":96},{"id":"primary","kind":"weekly","label":"Weekly","usedPercent":0}]}`, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			snaps, err := ParseJSON([]byte(normalizedEvent("codex", c.limits)), time.Time{})
			if (err != nil) != c.fail {
				t.Fatalf("snapshots=%+v error=%v want failure=%v", snaps, err, c.fail)
			}
			if c.fail && len(snaps) != 0 {
				t.Fatal("malformed event must not partially update buckets")
			}
		})
	}
}
