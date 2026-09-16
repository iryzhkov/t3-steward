package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadMigratesOnlyShippedQuotaDefaults(t *testing.T) {
	legacy := strings.Replace(Sample(), "min_window_duration: 168h # weekly, whether primary or secondary", "window: secondary", 1)
	legacy = strings.Replace(legacy, DefaultWarnMessage, legacyWarnMessage, 1)
	for _, tc := range []struct {
		name, text string
		migrated   bool
	}{
		{"shipped", legacy, true},
		{"custom warn", strings.Replace(legacy, legacyWarnMessage, "Custom quota advice.", 1), true},
		{"custom threshold", strings.Replace(legacy, "stop_percent: 99", "stop_percent: 98", -1), false},
		{"custom selector", strings.Replace(legacy, "window: secondary", "window: secondary\n      limit_name: special", 1), false},
		{"custom grace", strings.Replace(legacy, "window: secondary", "window: secondary\n    grace_period: 2m", 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(p, []byte(tc.text), 0600); err != nil {
				t.Fatal(err)
			}
			for _, load := range []func(string) (Config, error){Load, LoadFile} {
				c, err := load(p)
				if err != nil {
					t.Fatal(err)
				}
				o := c.Overrides[1]
				if got := o.Match.MinWindowDuration.D() == 168*time.Hour && o.Match.Window == ""; got != tc.migrated {
					t.Fatalf("override: %+v", o)
				}
				if tc.name == "custom warn" {
					if strings.TrimSpace(c.Messages.Warn) != "Custom quota advice." {
						t.Fatal(c.Messages.Warn)
					}
				} else if c.Messages.Warn != DefaultWarnMessage {
					t.Fatal(c.Messages.Warn)
				}
			}
			after, err := os.ReadFile(p)
			if err != nil || string(after) != tc.text {
				t.Fatal("load rewrote configuration")
			}
		})
	}
}

func TestQuotaMessageMigrationPreservesCustomTemplates(t *testing.T) {
	for _, custom := range []bool{false, true} {
		c := Default()
		c.Messages.Warn, c.Messages.Drain, c.Resume.Prompt = legacyWarnMessage+"\n", legacyDrainMessage+"\n", legacyResumePrompt+"\n"
		if custom {
			c.Messages.Warn, c.Messages.Drain, c.Resume.Prompt = "custom advisory", "custom drain", "custom recovery"
		}
		c.migrateQuotaDefaults()
		wantWarn, wantDrain, wantResume := DefaultWarnMessage, DefaultDrainMessage, DefaultResumePrompt
		if custom {
			wantWarn, wantDrain, wantResume = "custom advisory", "custom drain", "custom recovery"
		}
		if c.Messages.Warn != wantWarn || c.Messages.Drain != wantDrain || c.Resume.Prompt != wantResume {
			t.Fatalf("custom=%v messages=%+v resume=%q", custom, c.Messages, c.Resume.Prompt)
		}
	}
}

func TestDurationOverrideValidation(t *testing.T) {
	c := Default()
	var o Override
	o.Match.MinWindowDuration = Duration(168 * time.Hour)
	c.Overrides = []Override{o}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.Overrides[0].Match.MinWindowDuration = -1
	if err := c.Validate(); err == nil {
		t.Fatal("accepted negative duration")
	}
}
