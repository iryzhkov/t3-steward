package main

import (
	"context"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/coordinatoralerts"
)

func startCoordinatorAlerts(ctx context.Context, cfg config.Config, store coordinatoralerts.Store) (*coordinatoralerts.Listener, error) {
	settings := cfg.BacklogV2.Coordinator.Alerts
	if settings.Bind == "" {
		return nil, nil
	}
	stateDir, err := coordinatorStateDir(cfg)
	if err != nil {
		return nil, err
	}
	return coordinatoralerts.Start(ctx, settings.Bind, coordinatoralerts.Handler{
		Store: store, StateDir: stateDir, MinFreeMB: settings.MinFreeMB,
	})
}
