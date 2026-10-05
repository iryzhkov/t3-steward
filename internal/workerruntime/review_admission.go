package workerruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"slices"
	"strings"
	"time"
)

// ConfiguredAdmissionCatalog holds a detached operator snapshot. No observations enter this adapter.
type ConfiguredAdmissionCatalog struct{ settings config.BacklogV2 }

func NewConfiguredAdmissionCatalog(settings config.BacklogV2) (*ConfiguredAdmissionCatalog, error) {
	raw, err := json.Marshal(settings)
	if err != nil {
		return nil, err
	}
	var detached config.BacklogV2
	if err = json.Unmarshal(raw, &detached); err != nil {
		return nil, err
	}
	return &ConfiguredAdmissionCatalog{detached}, nil
}
func (c *ConfiguredAdmissionCatalog) ReviewAdmissionCatalog(_ context.Context, project string) (backlog.AdmissionCatalog, error) {
	var out backlog.AdmissionCatalog
	if c == nil {
		return out, fmt.Errorf("configured review admission catalog missing")
	}
	p, ok := c.settings.Projects[project]
	if !ok {
		return out, fmt.Errorf("configured review admission: project %q missing; configure backlog_v2.projects", project)
	}
	var issues []string
	ids := append([]string(nil), p.Workers...)
	slices.Sort(ids)
	for _, id := range ids {
		// Draining is a live admission choice, not a permanent policy revocation.
		settings := c.settings
		settings.Workers = make(map[string]config.V2Worker, len(c.settings.Workers))
		for k, v := range c.settings.Workers {
			settings.Workers[k] = v
		}
		w, ok := settings.Workers[id]
		if !ok {
			issues = append(issues, "worker "+id+" missing")
			continue
		}
		w.AcceptBacklog = true
		settings.Workers[id] = w
		binding, err := BuildWorkerBinding(settings, id, time.Time{})
		if err != nil {
			issues = append(issues, err.Error())
			continue
		}
		if !slices.ContainsFunc(binding.Inventory.Projects, func(p domain.WorkerProjectInventory) bool { return p.Name == project }) {
			issues = append(issues, id+": "+strings.Join(binding.CatalogIssues(), "; "))
			continue
		}
		// Only configured quota bindings grant routes. No live quota is consulted.
		providers := binding.Inventory.Providers[:0]
		for _, provider := range binding.Inventory.Providers {
			if pool, ok := settings.QuotaPools[provider.QuotaPoolID]; provider.QuotaPoolID != "" && strings.TrimSpace(provider.QuotaPoolID) == provider.QuotaPoolID && ok && pool.Provider != "" && strings.TrimSpace(pool.Provider) == pool.Provider && pool.MaxConcurrent > 0 {
				providers = append(providers, provider)
			}
		}
		binding.Inventory.Providers = providers
		out.AuthoredWorkers = append(out.AuthoredWorkers, binding.Inventory)
	}
	if len(out.AuthoredWorkers) == 0 {
		return out, fmt.Errorf("configured review admission: project %q has no eligible authored worker binding; repair configuration: %s", project, strings.Join(issues, "; "))
	}
	routes := make([]string, 0, len(c.settings.ReviewRoutes))
	for route := range c.settings.ReviewRoutes {
		routes = append(routes, route)
	}
	slices.Sort(routes)
	for _, route := range routes {
		m := c.settings.ReviewRoutes[route]
		out.Classifications = append(out.Classifications, backlog.AdmissionClassification{Route: route, ProviderFamily: m.ProviderFamily, Tier: m.Tier})
	}
	return out, nil
}
