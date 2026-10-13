package main

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/coordinatormetrics"
	"github.com/iryzhkov/t3-steward/internal/domain"
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
	pools, err := coordinatorMetricsQuotaPools(cfg)
	if err != nil {
		return nil, err
	}
	path, err := cfg.ResolveStatePath()
	if err != nil {
		return nil, err
	}
	source, err := sqlite.OpenQueryOnly(path)
	if err != nil {
		return nil, err
	}
	listener, err := coordinatormetrics.Start(ctx, bind, source, cfg.BacklogV2.Freshness.WorkerMaxAge.D(), cfg.BacklogV2.Freshness.QuotaMaxAge.D(), pools)
	if err != nil {
		source.Close()
		return nil, err
	}
	return &coordinatorMetricsListener{listener: listener, source: source}, nil
}

// coordinatorMetricsQuotaPools reuses admission's pure validated projection.
// It does not read the store or run reconciliation.
func coordinatorMetricsQuotaPools(cfg config.Config) ([]domain.QuotaPool, error) {
	bindings := coordinatorQuotaPoolBindings(cfg)
	if len(bindings) == 0 {
		return nil, nil
	}
	pools, _, err := backlog.QuotaBridgePools(bindings)
	return pools, err
}
