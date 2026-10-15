package main

import (
	"context"
	"net/http"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/coordinatoralerts"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func startCoordinatorAlerts(ctx context.Context, cfg config.Config, store coordinatoralerts.Store, callbacks ...workerproto.ExchangeCallback) (*coordinatoralerts.Listener, error) {
	settings := cfg.BacklogV2.Coordinator.Alerts
	if settings.Bind == "" {
		return nil, nil
	}
	stateDir, err := coordinatorStateDir(cfg)
	if err != nil {
		return nil, err
	}
	handler := workerproto.HTTPExchange{
		Enabled:    cfg.BacklogV2.Coordinator.WorkerHTTPEnabled,
		Monitoring: coordinatoralerts.Handler{Store: store, StateDir: stateDir, MinFreeMB: settings.MinFreeMB},
		Workers:    make(map[string]string), MaxMessageBytes: cfg.BacklogV2.MessageLimits.MaxBytes,
		MaxArtifactBytes: cfg.BacklogV2.MessageLimits.MaxArtifactBytes,
		MaxExchangeBytes: 64 << 20, Timeout: cfg.BacklogV2.Transport.RequestTimeout.D(),
	}
	for id, worker := range cfg.BacklogV2.Workers {
		if worker.ExchangeTransport == "http" {
			handler.Workers[id] = worker.Epoch
		}
	}
	if len(callbacks) > 0 {
		handler.Callback = callbacks[0]
	}
	return coordinatoralerts.StartWithHandler(ctx, settings.Bind, func(lifetime context.Context) http.Handler {
		handler.Lifetime = lifetime
		return handler
	})
}
