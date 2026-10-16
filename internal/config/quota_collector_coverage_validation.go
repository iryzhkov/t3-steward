package config

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/source/claudequota"
)

func sameModelSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	left, right := append([]string(nil), a...), append([]string(nil), b...)
	slices.Sort(left)
	slices.Sort(right)
	return slices.Equal(left, right)
}

func (c Config) validateCollectorCoverage() error {
	sources := map[string]string{}
	for id, binding := range c.BacklogV2.QuotaPools {
		declaration := binding.Collector
		if declaration == nil {
			continue
		}
		pool, _ := c.collectorPool(id)
		if !strings.Contains(strings.ToLower(binding.Provider), "claude") || declaration.SourceID == "" ||
			strings.TrimSpace(declaration.SourceID) != declaration.SourceID || len(declaration.SourceID) > 256 ||
			binding.AccountID == "" || binding.ObservationMaxAge <= 0 || len(declaration.Origins) == 0 {
			return fmt.Errorf("quota pool %q collector requires a Claude source_id, explicit account and scoped horizon, and origins", id)
		}
		if owner, ok := sources[declaration.SourceID]; ok && owner != id {
			return errors.New("collector source_id must belong to one configured pool")
		}
		sources[declaration.SourceID] = id
		origins := map[string]bool{}
		for _, origin := range declaration.Origins {
			if origin.WorkerID == "" || origin.ProviderInstanceID == "" || strings.TrimSpace(origin.WorkerID) != origin.WorkerID || strings.TrimSpace(origin.ProviderInstanceID) != origin.ProviderInstanceID {
				return errors.New("collector origins require trimmed worker_id and provider_instance_id")
			}
			key := origin.WorkerID + "\x00" + origin.ProviderInstanceID
			if origins[key] {
				return errors.New("collector origins must be unique")
			}
			origins[key] = true
			worker, known := c.BacklogV2.Workers[origin.WorkerID]
			provider, bound := worker.Providers[origin.ProviderInstanceID]
			localCoordinator := c.BacklogV2.Coordinator.ID != "" && origin.WorkerID == c.BacklogV2.Coordinator.ID && slices.Contains(pool.ProviderInstanceIDs, origin.ProviderInstanceID)
			if !(known && bound && provider.QuotaPool == id) && !localCoordinator {
				return errors.New("collector origin must name an explicitly configured worker/pool or local coordinator route")
			}
		}
		windows := map[string]bool{}
		for _, window := range declaration.RequiredWindows {
			if window.Window == "" || strings.TrimSpace(window.Window) != window.Window || windows[window.Window] {
				return errors.New("collector required_windows must name unique trimmed windows")
			}
			windows[window.Window] = true
			models := map[string]bool{}
			for _, model := range window.Models {
				if model == "" || models[model] || !slices.Contains(pool.Models, model) {
					return errors.New("collector model windows must name unique exact models already authorized in the pool catalog")
				}
				models[model] = true
			}
			if window.Window == "five_hour" || window.Window == "seven_day" {
				if len(window.Models) > 0 {
					return errors.New("collector base windows must govern the entire account")
				}
			} else if len(window.Models) == 0 {
				return errors.New("collector model windows require explicit exact model bindings")
			}
		}
		if !windows["five_hour"] || !windows["seven_day"] {
			return errors.New("collector coverage requires five_hour and seven_day")
		}
		origin := declaration.Origins[0]
		// Reuse the native timing/account validator; configuration stays inert.
		options := claudequota.Options{SourceID: declaration.SourceID, OriginWorkerID: origin.WorkerID,
			ProviderInstanceID: origin.ProviderInstanceID, AccountID: binding.AccountID,
			Interval: c.ClaudeQuotaCollector.Interval.D(), Jitter: c.ClaudeQuotaCollector.Jitter.D(),
			Timeout: c.ClaudeQuotaCollector.Timeout.D(), PipelineBudget: c.ClaudeQuotaCollector.PipelineBudget.D(),
			MaxObservationAge: binding.ObservationMaxAge.D()}
		if err := claudequota.ValidateOptions(options); err != nil {
			return fmt.Errorf("quota pool %q collector: %w", id, err)
		}
	}
	return nil
}
