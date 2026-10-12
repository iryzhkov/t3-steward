package config

import (
	"gopkg.in/yaml.v3"
	"testing"
	"time"
)

func TestCoalesceConfigDefaults(t *testing.T) {
	for _, tc := range []struct {
		yaml    string
		want    time.Duration
		invalid bool
	}{
		{"{}", 0, false}, {"wait: {coalesce: {enabled: true}}", 2 * time.Minute, false},
		{"wait: {coalesce: {enabled: true, window: 0s}}", 0, false},
		{"wait: {coalesce: {window: -1s}}", 0, true},
	} {
		var cfg Config
		if err := yaml.Unmarshal([]byte(tc.yaml), &cfg); err != nil {
			t.Fatal(err)
		}
		if got := cfg.Wait.Coalesce.EffectiveWindow(); got != tc.want {
			t.Fatal(tc.yaml, got)
		}
		if tc.invalid && cfg.Validate() == nil {
			t.Fatal("negative accepted")
		}
	}
}
