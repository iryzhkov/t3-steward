package main

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

type collectorCatalogPublisher interface {
	Catalog(context.Context, any) (map[string]string, error)
}

// publishCollectorCatalog completes capability and exact revision acceptance
// before the caller exposes any session for assignments or recovery.
func publishCollectorCatalog(ctx context.Context, client collectorCatalogPublisher, settings config.BacklogV2, workerID, expected string, knownCapable bool) (workerruntime.CatalogProjection, error) {
	projection, err := workerruntime.BuildCatalogProjection(settings, workerID)
	if err != nil {
		return projection, err
	}
	if projection.CollectorRecovery != nil && !knownCapable {
		legacy := settings
		legacy.CollectorRecovery = nil
		legacy.Workers = map[string]config.V2Worker{}
		worker := settings.Workers[workerID]
		worker.AcceptBacklog = false
		worker.Providers = map[string]config.V2Provider{}
		for instance, provider := range settings.Workers[workerID].Providers {
			if _, covered := projection.CollectorRecovery.Pools[provider.QuotaPool]; !covered {
				worker.Providers[instance] = provider
			}
		}
		legacy.Workers[workerID] = worker
		restricted, err := workerruntime.BuildCatalogProjection(legacy, workerID)
		if err != nil {
			return projection, err
		}
		ack, err := client.Catalog(ctx, workerruntime.CatalogRequest{Projection: restricted, ExpectedRevision: expected})
		if err != nil {
			return projection, err
		}
		if ack["revision"] != restricted.Revision || ack["workerId"] != workerID || ack["collectorProvenance"] != workerproto.CapabilityQuotaCollectorProvenance {
			return projection, errors.New("covered worker requires authenticated collector capability acceptance; restricted catalog retained")
		}
		expected = restricted.Revision
	}
	ack, err := client.Catalog(ctx, workerruntime.CatalogRequest{Projection: projection, ExpectedRevision: expected})
	if err != nil {
		return projection, err
	}
	if ack["revision"] != projection.Revision || ack["workerId"] != workerID {
		return projection, errors.New("worker accepted wrong catalog")
	}
	if projection.CollectorRecovery != nil && ack["collectorProvenance"] != workerproto.CapabilityQuotaCollectorProvenance {
		return projection, errors.New("covered worker requires collector-aware enriched catalog acceptance")
	}
	return projection, nil
}
