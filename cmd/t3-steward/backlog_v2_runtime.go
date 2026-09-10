package main

import (
	"context"
	"log/slog"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func runBacklogV2(ctx context.Context, cfg config.Config, logger *slog.Logger) (bool, error) {
	if cfg.BacklogV2.Mode == "disabled" {
		return false, nil
	}
	return true, runBacklogV2Coordinator(ctx, cfg, logger)
}

// runBacklogV2Coordinator establishes authority with admission closed. Later
// stages bind worker exchange and planning only after freshness is established.
func runBacklogV2Coordinator(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	statePath, err := cfg.ResolveStatePath()
	if err != nil {
		return err
	}
	store, err := sqlite.OpenMigrated(statePath)
	if err != nil {
		return err
	}
	defer store.Close()
	epoch, err := store.AcquireCoordinator(ctx, cfg.BacklogV2.Coordinator.ID)
	if err != nil {
		return err
	}
	logger.Info("backlog-v2 coordinator authority acquired",
		"coordinator", cfg.BacklogV2.Coordinator.ID,
		"epoch", epoch,
		"admission", "closed")
	<-ctx.Done()
	return nil
}
