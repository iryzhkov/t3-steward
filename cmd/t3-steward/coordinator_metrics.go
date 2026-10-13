package main

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/coordinatormetrics"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type coordinatorMetricsListener struct {
	listener *coordinatormetrics.Listener
	source   *sqlite.QueryOnlyStore
}

func (l *coordinatorMetricsListener) Close() error {
	if l == nil {
		return nil
	}
	return errors.Join(l.listener.Close(), l.source.Close())
}
func startCoordinatorMetrics(ctx context.Context, cfg config.Config) (*coordinatorMetricsListener, error) {
	bind := cfg.BacklogV2.Coordinator.Metrics.Bind
	if bind == "" {
		return nil, nil
	}
	path, err := cfg.ResolveStatePath()
	if err != nil {
		return nil, err
	}
	source, err := sqlite.OpenQueryOnly(path)
	if err != nil {
		return nil, err
	}
	listener, err := coordinatormetrics.Start(ctx, bind, source, cfg.BacklogV2.Freshness.WorkerMaxAge.D(), cfg.BacklogV2.Freshness.QuotaMaxAge.D())
	if err != nil {
		source.Close()
		return nil, err
	}
	return &coordinatorMetricsListener{listener: listener, source: source}, nil
}
