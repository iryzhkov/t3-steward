package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type inertQuotaSource struct{}

func (*inertQuotaSource) Run(ctx context.Context, _ chan<- domain.QuotaSnapshot) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestQuotaSourceCompositionDisabledAndEnabledAreInert(t *testing.T) {
	t.Parallel()
	cfg := config.Default()
	logs := &inertQuotaSource{}
	source, err := quotaSources(cfg, logs)
	if err != nil || source != logs {
		t.Fatalf("disabled source=%T err=%v", source, err)
	}
	dir := t.TempDir()
	cfg.StatePath = filepath.Join(dir, "state.db")
	cfg.ClaudeQuotaCollector.SourceID = "native-test"
	cfg.ClaudeQuotaCollector.OriginWorkerID = "collector"
	cfg.BacklogV2.LocalWorker.ID = "collector"
	cfg.ClaudeQuotaCollector.Enabled = true
	cfg.ClaudeQuotaCollector.ProviderInstanceID = "claudeAgent"
	cfg.ClaudeQuotaCollector.AccountID = "11111111-2222-3333-4444-555555555555"
	cfg.ClaudeQuotaCollector.QuotaPool = "primary"
	cfg.ClaudeQuotaCollector.CredentialDir = filepath.Join(dir, "missing-auth")
	cfg.BacklogV2.Workers = map[string]config.V2Worker{"collector": {Providers: map[string]config.V2Provider{"claudeAgent": {QuotaPool: "primary", Models: []string{"claude-test"}}}}}
	cfg.BacklogV2.QuotaPools = map[string]config.V2QuotaPool{"primary": {Provider: "claude", MaxConcurrent: 1, Collector: &config.V2QuotaCollectorCoverage{SourceID: "native-test", Origins: []domain.QuotaCollectorOrigin{{WorkerID: "collector", ProviderInstanceID: "claudeAgent"}}, RequiredWindows: []domain.QuotaCollectorWindow{{Window: "five_hour"}, {Window: "seven_day"}}}, AccountID: cfg.ClaudeQuotaCollector.AccountID, ObservationMaxAge: config.Duration(390 * time.Second)}}
	source, err = quotaSources(cfg, logs)
	if err != nil || source == logs {
		t.Fatalf("enabled source=%T err=%v", source, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatal("source construction touched auth/state")
	}
}
