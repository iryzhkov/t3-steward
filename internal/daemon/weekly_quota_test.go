package daemon

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/source/providerlog"
	"gopkg.in/yaml.v3"
)

// Exercise the actual provider payload, shipped override and persisted daemon
// state: a weekly primary must behave just like a weekly secondary.
func TestWeeklyCodexQuotaLadder(t *testing.T) {
	for _, window := range []string{"primary", "secondary"} {
		t.Run(window, func(t *testing.T) {
			h := newHarness(t, func(c *config.Config) {
				if err := yaml.Unmarshal([]byte(config.Sample()), c); err != nil {
					t.Fatal(err)
				}
				c.Policy.DryRun = false
				c.Policy.RateWindow = 0
			})
			h.fake.add("a", "codex", "gpt", true)
			reset := h.clock.Add(4 * 24 * time.Hour)
			for i, step := range []struct {
				used            int
				warnings, stops int
			}{{86, 0, 0}, {94, 0, 0}, {95, 1, 0}, {96, 1, 0}, {97, 2, 0}, {99, 2, 1}} {
				body := fmt.Sprintf(`{"type":"account.rate-limits.updated","provider":"codex","eventId":"%d","payload":{"rateLimits":{"rateLimits":{"planType":"pro","%s":{"usedPercent":%d,"windowDurationMins":10080,"resetsAt":%d}}}}}`, i, window, step.used, reset.Unix())
				snaps, err := providerlog.ParseJSON([]byte(body), h.clock)
				if err != nil || len(snaps) != 1 {
					t.Fatalf("parse: %v %v", snaps, err)
				}
				if !strings.Contains(snaps[0].LimitName, "weekly quota") {
					t.Fatal(snaps[0].LimitName)
				}
				h.d.HandleSnapshot(context.Background(), snaps[0])
				if len(h.fake.warnings) != step.warnings || len(h.fake.stops) != step.stops {
					t.Fatalf("at %d%% warnings=%v stops=%v", step.used, h.fake.warnings, h.fake.stops)
				}
				h.clock = h.clock.Add(time.Second)
			}
			state, err := h.store.LoadBucket(context.Background(), domain.BucketKey{ProviderInstanceID: "codex", LimitID: "codex", Window: window})
			if err != nil {
				t.Fatal(err)
			}
			if state.WindowDuration != 7*24*time.Hour {
				t.Fatalf("lost duration: %+v", state)
			}
		})
	}
}

func TestDurationOverrideRefreshesCachedEngine(t *testing.T) {
	h := newHarness(t, func(c *config.Config) {
		if err := yaml.Unmarshal([]byte(config.Sample()), c); err != nil {
			t.Fatal(err)
		}
	})
	for _, duration := range []time.Duration{0, 5 * time.Hour, 7 * 24 * time.Hour, 5 * time.Hour} {
		want := 85.0
		if duration == 7*24*time.Hour {
			want = 95
		}
		got := h.d.engineFor(codexPrimary, "codex", duration).Thresholds().WarnPercent
		if got != want {
			t.Fatalf("duration %s: warn %v, want %v", duration, got, want)
		}
	}
}

func TestDurationOverrideMatchAndPrecedence(t *testing.T) {
	var weekly config.Override
	weekly.Match.MinWindowDuration = config.Duration(168 * time.Hour)
	for _, tc := range []struct {
		duration time.Duration
		want     bool
	}{{0, false}, {5 * time.Hour, false}, {168*time.Hour - time.Second, false}, {168 * time.Hour, true}, {200 * time.Hour, true}} {
		if got := overrideMatches(weekly, codexPrimary, "codex", tc.duration); got != tc.want {
			t.Fatalf("%s: %v", tc.duration, got)
		}
	}
	weekly.Match.Provider = "claude*"
	if overrideMatches(weekly, codexPrimary, "codex", 168*time.Hour) {
		t.Fatal("ignored provider filter")
	}
	h := newHarness(t, func(c *config.Config) {
		if err := yaml.Unmarshal([]byte(config.Sample()), c); err != nil {
			t.Fatal(err)
		}
		var first config.Override
		first.Match.Window = "primary"
		warn, drain, stop := 90.0, 92.0, 94.0
		first.WarnPercent, first.DrainPercent, first.StopPercent = &warn, &drain, &stop
		c.Overrides = append([]config.Override{first}, c.Overrides...)
	})
	if got := h.d.engineFor(codexPrimary, "codex", 168*time.Hour).Thresholds().WarnPercent; got != 90 {
		t.Fatalf("first match lost: %v", got)
	}
}

func TestQuotaWarningIsAdvisoryAndDrainStops(t *testing.T) {
	snap := domain.QuotaSnapshot{Key: codexPrimary, LimitName: "codex weekly quota (primary window)", UsedPercent: 95}
	for _, sample := range []bool{false, true} {
		cfg := config.Default()
		if sample {
			if err := yaml.Unmarshal([]byte(config.Sample()), &cfg); err != nil {
				t.Fatal(err)
			}
		}
		warn, err := renderMessage(cfg.Messages.Warn, snap, time.Minute, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(warn, "Continue the current user task") || strings.Contains(warn, "then stop at a clean point") {
			t.Fatal(warn)
		}
		drain, err := renderMessage(cfg.Messages.Drain, snap, time.Minute, time.Now())
		if err != nil || !strings.Contains(drain, "Then end your turn.") || !strings.Contains(drain, "interrupt any still-running turn in 1m0s") {
			t.Fatalf("%s: %v", drain, err)
		}
	}
}
