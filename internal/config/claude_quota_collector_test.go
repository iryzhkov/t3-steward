package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const collectorAccount = "11111111-2222-3333-4444-555555555555"

func collectorConfig() Config {
	c := Default()
	c.ClaudeQuotaCollector.SourceID = "native-test"
	c.ClaudeQuotaCollector.OriginWorkerID = "collector"
	c.BacklogV2.LocalWorker.ID = "collector"
	c.ClaudeQuotaCollector.Enabled = true
	c.ClaudeQuotaCollector.ProviderInstanceID = "claudeAgent"
	c.ClaudeQuotaCollector.AccountID = collectorAccount
	c.ClaudeQuotaCollector.QuotaPool = "claude-primary"
	c.ClaudeQuotaCollector.ModelBindings = []ClaudeQuotaModelBinding{{DisplayName: "Fable",
		Window: "seven_day_overage_included", Label: "Fable weekly", Selector: "fable", Models: []string{"claude-fable-5-1"}}}
	c.BacklogV2.QuotaPools = map[string]V2QuotaPool{"claude-primary": {Collector: &V2QuotaCollectorCoverage{SourceID: "native-test", Origins: []domain.QuotaCollectorOrigin{{WorkerID: "collector", ProviderInstanceID: "claudeAgent"}}, RequiredWindows: []domain.QuotaCollectorWindow{{Window: "five_hour"}, {Window: "seven_day"}, {Window: "seven_day_overage_included", Models: []string{"claude-fable-5-1"}}}}, Provider: "claude", AccountID: collectorAccount, ObservationMaxAge: Duration(390 * time.Second), MaxConcurrent: 1}}
	c.BacklogV2.Workers = map[string]V2Worker{"collector": {Providers: map[string]V2Provider{"claudeAgent": {QuotaPool: "claude-primary", Models: []string{"claude-fable-5-1"}}}}}
	return c
}
func TestCollectorDisabledDefaultsPreserveQuotaFreshness(t *testing.T) {
	t.Parallel()
	c := Default()
	if c.ClaudeQuotaCollector.Enabled || c.BacklogV2.Freshness.QuotaMaxAge.D() != time.Minute {
		t.Fatal("default enabled or global horizon widened")
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}
func TestCollectorExplicitAccountScopedHorizonAndCatalog(t *testing.T) {
	t.Parallel()
	c := collectorConfig()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if c.BacklogV2.Freshness.QuotaMaxAge.D() != time.Minute {
		t.Fatal("global horizon widened")
	}
	tests := []struct {
		name    string
		edit    func(*Config)
		message string
	}{
		{"no account", func(c *Config) { c.ClaudeQuotaCollector.AccountID = "" }, "matching"},
		{"mismatch", func(c *Config) { c.ClaudeQuotaCollector.AccountID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" }, "matching"},
		{"unknown pool", func(c *Config) { c.ClaudeQuotaCollector.QuotaPool = "unknown" }, "matching"},
		{"too stale", func(c *Config) {
			p := c.BacklogV2.QuotaPools["claude-primary"]
			p.ObservationMaxAge = Duration(360 * time.Second)
			c.BacklogV2.QuotaPools["claude-primary"] = p
		}, "insufficient-scoped-age"},
		{"exact acquisition plus pipeline boundary", func(c *Config) {
			p := c.BacklogV2.QuotaPools["claude-primary"]
			p.ObservationMaxAge = Duration(375 * time.Second)
			c.BacklogV2.QuotaPools["claude-primary"] = p
		}, "insufficient-scoped-age"},
		{"global age substituted", func(c *Config) {
			p := c.BacklogV2.QuotaPools["claude-primary"]
			p.ObservationMaxAge = 0
			c.BacklogV2.QuotaPools["claude-primary"] = p
		}, "explicit"},
		{"pipeline missing pass", func(c *Config) { c.ClaudeQuotaCollector.PipelineBudget = Duration(40 * time.Second) }, "pipeline_budget"},
		{"fast poll", func(c *Config) { c.ClaudeQuotaCollector.Interval = Duration(60 * time.Second) }, "invalid-timing"},
		{"long timeout", func(c *Config) { c.ClaudeQuotaCollector.Timeout = Duration(11 * time.Second) }, "invalid-timing"},
		{"false local origin", func(c *Config) { c.ClaudeQuotaCollector.OriginWorkerID = "other" }, "runtime worker"},
		{"unproven source", func(c *Config) { c.ClaudeQuotaCollector.SourceID = "other" }, "source_id"},
		{"alias model", func(c *Config) { c.ClaudeQuotaCollector.ModelBindings[0].Models = []string{"fable"} }, "exact models"},
		{"unbound instance", func(c *Config) { c.ClaudeQuotaCollector.ProviderInstanceID = "claude-other" }, "explicitly bound"},
		{"duplicate window", func(c *Config) {
			c.ClaudeQuotaCollector.ModelBindings = append(c.ClaudeQuotaCollector.ModelBindings, c.ClaudeQuotaCollector.ModelBindings[0])
		}, "invalid-model-binding"},
		{"unproved legacy source", func(c *Config) {
			c.ClaudeQuotaCollector.ModelBindings[0].DisplayName = ""
			c.ClaudeQuotaCollector.ModelBindings[0].SourceKey = "seven_day_overage_included"
		}, "invalid-model-binding"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := collectorConfig()
			tt.edit(&c)
			err := c.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.message) {
				t.Fatalf("err=%v want %s", err, tt.message)
			}
		})
	}
}
func TestCollectorStrictFileProjectionHasNoAuthOrStatusEffects(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c := collectorConfig()
	c.ClaudeQuotaCollector.CredentialDir = filepath.Join(dir, "missing-credentials")
	c.ClaudeQuotaCollector.StatusPath = filepath.Join(dir, "not-created", "status.json")
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(c.ClaudeQuotaCollector.StatusPath); !os.IsNotExist(err) {
		t.Fatal("validation wrote status")
	}
	path := filepath.Join(dir, "config.yaml")
	yaml := `claude_quota_collector:
  enabled: false
  interval: 5m
  jitter: 15s
  timeout: 10s
  pipeline_budget: 50s
`
	if err := os.WriteFile(path, []byte(yaml), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.ClaudeQuotaCollector.Enabled || got.ClaudeQuotaCollector.Interval.D() != 5*time.Minute {
		t.Fatal("strict projection incorrect")
	}
	if err := os.WriteFile(path, []byte(yaml+"  typo: true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFile(path); err == nil {
		t.Fatal("unknown collector key accepted")
	}
}
