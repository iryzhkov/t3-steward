package main

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

// Validation uses configured authority, independent of worker freshness/quota.
// Scheduling still applies those dynamic gates before offering any work.
func graphTaskValidator(settings config.BacklogV2) func(domain.Workflow, domain.Task) error {
	return func(workflow domain.Workflow, task domain.Task) error {
		for _, route := range task.Routes {
			for name, value := range route.Options {
				if strings.TrimSpace(name) != name || name == "" || len(name) > 128 || strings.TrimSpace(value) != value || value == "" || len(value) > 1024 {
					return fmt.Errorf("invalid provider option")
				}
			}
			eligible := false
			for id, worker := range settings.Workers {
				if route.WorkerID != "" && route.WorkerID != id {
					continue
				}
				if len(task.Placement.Hosts) > 0 && !slices.Contains(task.Placement.Hosts, id) {
					continue
				}
				capable := true
				for _, cap := range task.Placement.Capabilities {
					if !slices.Contains(worker.Capabilities, cap) {
						capable = false
					}
				}
				if !capable {
					continue
				}
				provider, ok := worker.Providers[route.ProviderInstanceID]
				if !ok || !slices.Contains(provider.Models, route.Model) {
					continue
				}
				if route.QuotaPoolID != "" && route.QuotaPoolID != provider.QuotaPool {
					continue
				}
				binding, err := workerruntime.BuildWorkerBinding(settings, id, time.Now().UTC())
				if err != nil {
					continue
				}
				if _, err = binding.Catalog.Resolve(workflow, task); err != nil {
					continue
				}
				eligible = true
			}
			if !eligible {
				return fmt.Errorf("no configured worker supports project, placement and route %s/%s", route.ProviderInstanceID, route.Model)
			}
		}
		return nil
	}
}
