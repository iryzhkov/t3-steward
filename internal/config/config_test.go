package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestDefaultsValidate(t *testing.T) {
	c := Default()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if !c.Policy.DryRun || c.Resume.Enabled {
		t.Fatal("defaults must be dry-run with resume disabled")
	}
}

func TestSampleMatchesDefaults(t *testing.T) {
	c := Default()
	if err := yaml.Unmarshal([]byte(Sample()), &c); err != nil {
		t.Fatal(err)
	}
	d := Default()
	if c.Policy.WarnPercent != d.Policy.WarnPercent || c.Policy.StopPercent != d.Policy.StopPercent || c.Policy.GracePeriod != d.Policy.GracePeriod {
		t.Fatalf("sample policy differs: %+v vs %+v", c.Policy, d.Policy)
	}
	if c.Policy.DryRun != true || c.Resume.Enabled != false || c.Polling.SnapshotInterval != d.Polling.SnapshotInterval {
		t.Fatalf("sample differs from defaults: %+v", c)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadFileAndEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("policy:\n  warn_percent: 70\n  drain_percent: 80\n  stop_percent: 90\n  grace_period: 30s\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvPrefix+"STOP_PERCENT", "95")
	t.Setenv(EnvPrefix+"DRY_RUN", "false")
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Policy.WarnPercent != 70 || c.Policy.StopPercent != 95 || c.Policy.DryRun || c.Policy.GracePeriod.D() != 30*time.Second {
		t.Fatalf("config = %+v", c.Policy)
	}
}

func TestInvalidLadderRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("policy:\n  warn_percent: 95\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestMissingFileUsesDefaults(t *testing.T) {
	c, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Policy.WarnPercent != 85 {
		t.Fatal("defaults not applied")
	}
}
