package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestCollectorCoverageDoesNotRelaxCoordinatorDatabaseBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.AcquireCoordinator(context.Background(), "normandy"); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.StatePath = path
	cfg.BacklogV2.Workers = map[string]config.V2Worker{"normandy": {Providers: map[string]config.V2Provider{
		"claudeAgent": {Models: []string{"claude-fable-5-1"}, QuotaPool: "claude-main"},
	}}}
	cfg.BacklogV2.QuotaPools = map[string]config.V2QuotaPool{"claude-main": {Provider: "claudeAgent", AccountID: "11111111-2222-3333-4444-555555555555", ObservationMaxAge: config.Duration(360 * time.Second),
		Collector: &config.V2QuotaCollectorCoverage{SourceID: "native", Origins: []domain.QuotaCollectorOrigin{{WorkerID: "normandy", ProviderInstanceID: "claudeAgent"}}, RequiredWindows: []domain.QuotaCollectorWindow{{Window: "five_hour"}, {Window: "seven_day"}}}}}
	cfg.BacklogV2.CollectorRecovery = cfg.CollectorRecoveryMetadataForWorker("normandy")
	if cfg.BacklogV2.CollectorRecovery == nil {
		t.Fatal("fixture lacks declared coverage")
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, mode := range []string{"coordinator", "worker"} {
		cfg.BacklogV2.Mode = mode
		if guard := hostQuotaGuard(cfg, logger, nil); guard != nil {
			t.Fatalf("%s constructor exposed coordinator state as a watchdog guard: %T", mode, guard)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(path + suffix); !os.IsNotExist(err) {
			t.Fatalf("covered worker retained coordinator database sidecar %s: %v", suffix, err)
		}
	}
}
