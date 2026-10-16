package config

import (
	"slices"
	"sort"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// V2QuotaCollectorCoverage declares shared acquisition coverage. It does not
// enable a local producer or assert that an observation is fresh or native.
type V2QuotaCollectorCoverage struct {
	SourceID        string                        `yaml:"source_id"`
	Origins         []domain.QuotaCollectorOrigin `yaml:"origins"`
	RequiredWindows []domain.QuotaCollectorWindow `yaml:"required_windows"`
}

func (c Config) collectorPool(id string) (domain.QuotaPool, bool) {
	if metadata := c.BacklogV2.CollectorRecovery; metadata != nil {
		if pool, ok := metadata.Pools[id]; ok {
			return pool, true
		}
	}
	binding, ok := c.BacklogV2.QuotaPools[id]
	if !ok || binding.Collector == nil {
		return domain.QuotaPool{}, false
	}
	pool := domain.QuotaPool{ID: id, Provider: binding.Provider, AccountID: binding.AccountID,
		MaxConcurrent: binding.MaxConcurrent, Collector: &domain.QuotaCollectorCoverage{
			SourceID: binding.Collector.SourceID, AccountID: binding.AccountID,
			Origins:           append([]domain.QuotaCollectorOrigin(nil), binding.Collector.Origins...),
			MaxObservationAge: binding.ObservationMaxAge.D()}}
	for _, window := range binding.Collector.RequiredWindows {
		pool.Collector.RequiredWindows = append(pool.Collector.RequiredWindows, domain.QuotaCollectorWindow{
			Window: window.Window, Models: append([]string(nil), window.Models...)})
	}
	for _, worker := range c.BacklogV2.Workers {
		for instance, provider := range worker.Providers {
			if provider.QuotaPool != id {
				continue
			}
			if !slices.Contains(pool.ProviderInstanceIDs, instance) {
				pool.ProviderInstanceIDs = append(pool.ProviderInstanceIDs, instance)
			}
			for _, model := range provider.Models {
				if !slices.Contains(pool.Models, model) {
					pool.Models = append(pool.Models, model)
				}
			}
		}
	}
	sort.Strings(pool.ProviderInstanceIDs)
	sort.Strings(pool.Models)
	return pool, true
}

// CollectorCoverage is configuration, independent of local Enabled or health.
// The exact route must already be in the configured worker/pool catalog.
func (c Config) CollectorCoverage(route domain.ProviderRoute) (domain.QuotaPool, bool) {
	if route.WorkerID != "" {
		worker, ok := c.BacklogV2.Workers[route.WorkerID]
		if !ok {
			return domain.QuotaPool{}, false
		}
		provider, ok := worker.Providers[route.ProviderInstanceID]
		if !ok || (route.QuotaPoolID != "" && route.QuotaPoolID != provider.QuotaPool) || !slices.Contains(provider.Models, route.Model) {
			return domain.QuotaPool{}, false
		}
		route.QuotaPoolID = provider.QuotaPool
	}
	if route.QuotaPoolID != "" {
		pool, ok := c.collectorPool(route.QuotaPoolID)
		return pool, ok && domain.CollectorCoversRoute(pool, route)
	}
	return c.CollectorCoverageForThread(route.ProviderInstanceID, route.Model)
}

// CollectorCoverageForThread projects a provider/model only when every matching
// declared worker route binds one pool. Ambiguity or an unknown model proves nothing.
func (c Config) CollectorCoverageForThread(provider, model string) (domain.QuotaPool, bool) {
	selected := ""
	for _, worker := range c.BacklogV2.Workers {
		p, ok := worker.Providers[provider]
		if !ok || !slices.Contains(p.Models, model) {
			continue
		}
		if selected != "" && selected != p.QuotaPool {
			return domain.QuotaPool{}, false
		}
		selected = p.QuotaPool
	}
	if selected == "" {
		return domain.QuotaPool{}, false
	}
	pool, ok := c.collectorPool(selected)
	return pool, ok && domain.CollectorCoversRoute(pool, domain.ProviderRoute{ProviderInstanceID: provider, Model: model, QuotaPoolID: selected})
}
