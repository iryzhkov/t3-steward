package config

import (
	"os"
	"path/filepath"
	"strings"
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

// P0.2: a daemon started before T3 waits for it, two minutes by default; the
// sample says the same, and a negative bound is refused.
func TestT3DiscoveryTimeoutDefaultsToTwoMinutes(t *testing.T) {
	c := Default()
	if c.T3.DiscoveryTimeout.D() != 2*time.Minute {
		t.Fatalf("discovery_timeout default = %s", c.T3.DiscoveryTimeout.D())
	}
	sample := Default()
	sample.T3.DiscoveryTimeout = 0
	if err := yaml.Unmarshal([]byte(Sample()), &sample); err != nil {
		t.Fatal(err)
	}
	if sample.T3.DiscoveryTimeout != c.T3.DiscoveryTimeout {
		t.Fatalf("sample discovery_timeout = %s", sample.T3.DiscoveryTimeout.D())
	}
	c.T3.DiscoveryTimeout = Duration(-time.Second)
	if err := c.Validate(); err == nil {
		t.Fatal("a negative discovery_timeout was accepted")
	}
	c.T3.DiscoveryTimeout = 0
	if err := c.Validate(); err != nil {
		t.Fatalf("zero (one attempt) was refused: %v", err)
	}
}

// The two new durations as an operator writes them: omitted, zero, a value and
// a negative value, through the loader, on a host without and with the
// UpKeeper coordinator client file. The file fills the client block when
// config.yaml declares no coordinator, and it used to replace the block's
// defaults (quota_stale_after, and model too) with nothing.
func TestDiscoveryTimeoutAndQuotaStaleAfterLoad(t *testing.T) {
	for _, home := range []struct{ name, path string }{
		{name: "no client file", path: t.TempDir()},
		{name: "client file", path: bootstrapHome(t, validBootstrapDocument, 0o600)},
	} {
		t.Run(home.name, func(t *testing.T) {
			original := coordinatorClientHome
			coordinatorClientHome = func() string { return home.path }
			t.Cleanup(func() { coordinatorClientHome = original })
			testDiscoveryTimeoutAndQuotaStaleAfterLoad(t)
		})
	}
}

func testDiscoveryTimeoutAndQuotaStaleAfterLoad(t *testing.T) {
	for _, tc := range []struct {
		name, yaml string
		discovery  time.Duration
		staleAfter time.Duration
		wantErr    string
	}{
		{name: "omitted", yaml: "log_level: info\n", discovery: 2 * time.Minute},
		{name: "zero is a single attempt and the default threshold", yaml: "t3:\n  discovery_timeout: 0s\nbacklog_v2:\n  coordinator_client:\n    defaults:\n      quota_stale_after: 0s\n"},
		{name: "set", yaml: "t3:\n  discovery_timeout: 30s\nbacklog_v2:\n  coordinator_client:\n    defaults:\n      quota_stale_after: 90m\n", discovery: 30 * time.Second, staleAfter: 90 * time.Minute},
		{name: "negative discovery", yaml: "t3:\n  discovery_timeout: -1s\n", wantErr: "discovery_timeout must not be negative"},
		{name: "negative staleness", yaml: "backlog_v2:\n  coordinator_client:\n    defaults:\n      quota_stale_after: -1m\n", wantErr: "quota_stale_after must not be negative"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tc.yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			c, err := Load(path)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.T3.DiscoveryTimeout.D() != tc.discovery || c.BacklogV2.CoordinatorClient.Defaults.QuotaStaleAfter.D() != tc.staleAfter {
				t.Fatalf("discovery_timeout=%s quota_stale_after=%s", c.T3.DiscoveryTimeout.D(), c.BacklogV2.CoordinatorClient.Defaults.QuotaStaleAfter.D())
			}
		})
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
