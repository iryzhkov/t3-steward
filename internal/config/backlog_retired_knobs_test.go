package config

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

// Markdown intake is retired, so the sample no longer advertises its drop
// directory, quiet period or forwarding host, and no default pretends the quiet
// period still gates anything.
func TestSampleAndDefaultsNoLongerPublishRetiredIntakeKnobs(t *testing.T) {
	var sample struct {
		Backlog map[string]any `yaml:"backlog"`
	}
	if err := yaml.Unmarshal([]byte(Sample()), &sample); err != nil {
		t.Fatal(err)
	}
	if len(sample.Backlog) == 0 {
		t.Fatal("sample lost its backlog section")
	}
	for _, key := range []string{"dir", "quiet_for", "preamble", "default_host"} {
		if _, ok := sample.Backlog[key]; ok {
			t.Errorf("sample still publishes retired backlog.%s", key)
		}
	}
	for _, key := range []string{"enabled", "long_window_cap_percent", "history_days", "host_name", "safety_margin_percent", "fallback_per_hour_percent", "quantile", "min_samples"} {
		if _, ok := sample.Backlog[key]; !ok {
			t.Errorf("sample lost live backlog.%s", key)
		}
	}
	if quiet := Default().Backlog.QuietFor; quiet != 0 {
		t.Fatalf("retired quiet_for still defaults to %s", quiet.D())
	}
}

// A configuration file written by an older release still sets the retired
// keys, and strict decoding must keep accepting it.
func TestOlderConfigWithRetiredIntakeKnobsStillDecodes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	raw := "backlog:\n  enabled: false\n  dir: /srv/backlog\n  quiet_for: 30m\n  preamble: old text\n  default_host: remote\n"
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("older configuration refused: %v", err)
	}
	if cfg.Backlog.Dir != "/srv/backlog" || cfg.Backlog.QuietFor.D().String() != "30m0s" ||
		cfg.Backlog.Preamble != "old text" || cfg.Backlog.DefaultHost != "remote" {
		t.Fatalf("retired keys decoded as %+v", cfg.Backlog)
	}
}
