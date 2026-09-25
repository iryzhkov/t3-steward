package main

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

// S8: the persistent worker every host runs once handed its exchange no usage
// source, so it forwarded nothing and every run read zero attributed samples.
// The options the daemon serves with must carry this host's readings.
func TestPersistentWorkerOptionsForwardTheHostsUsage(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	seeded, err := sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := seeded.RecordUsage(ctx, domain.UsageSample{
		ProviderInstanceID: "provider", ThreadID: "thread", Model: "model",
		ObservedAt: time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC), SourceEventID: "host-event",
		Kind: domain.UsageKindCall, FieldPresence: domain.UsageFieldsAll, InputTokens: 3,
	}); err != nil {
		t.Fatal(err)
	}
	if err := seeded.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.StatePath = path
	logger := slog.New(slog.DiscardHandler)
	usage := hostUsageStore(cfg, logger)
	if usage == nil {
		t.Fatal("the host's watchdog state was not opened")
	}
	defer usage.Close()

	options := persistentWorkerOptions(ctx, cfg, logger, nil, t.TempDir(), "digest", workerruntime.ProtocolResolver{}, nil, usage)
	if options.Usage == nil {
		t.Fatal("the persistent worker serves its exchange without a usage source")
	}
	samples, err := options.Usage.WorkerUsageBatch(ctx, nil, workerproto.MaxUsageDelivery)
	if err != nil || len(samples) != 1 || samples[0].SourceEventID != "host-event" {
		t.Fatalf("forwarded usage = %#v, %v", samples, err)
	}
	// Without the watchdog's database there is no source at all, rather than
	// one that fails every exchange.
	if options := persistentWorkerOptions(ctx, cfg, logger, nil, t.TempDir(), "digest", workerruntime.ProtocolResolver{}, nil, nil); options.Usage != nil {
		t.Fatalf("a missing watchdog database produced usage source %#v", options.Usage)
	}
}
