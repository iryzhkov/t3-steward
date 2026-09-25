package main

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

// Validation uses configured authority, independent of worker freshness/quota.
// Scheduling still applies those dynamic gates before offering any work.
//
// Whether a worker could serve the task is decided by
// backlogadmin.WorkerMatchReasons, the function readiness calls for "campaign
// check" and submit, and a reason refuses exactly when
// backlogadmin.PermanentViabilityReason says it refuses a submission. This
// validator used to keep its own copy of those rules, and the copy disagreed
// with readiness: it read only the configured capability list and refused
// every task requiring a build capability such as
// task-wait-collection-fence-v1 (S11), it demanded that every fallback route
// resolve, and it never read accept_backlog or the CPU-class floor. What stays
// here is only what configuration must stand in for, because validation has
// no snapshot to read: the inventory each configured worker would report.
func graphTaskValidator(settings config.BacklogV2) func(domain.Workflow, domain.Task) error {
	pools := configuredQuotaPools(settings)
	workerIDs := make([]string, 0, len(settings.Workers))
	for id := range settings.Workers {
		workerIDs = append(workerIDs, id)
	}
	sort.Strings(workerIDs)
	return func(workflow domain.Workflow, task domain.Task) error {
		for _, route := range task.Routes {
			for name, value := range route.Options {
				if strings.TrimSpace(name) != name || name == "" || len(name) > 128 || strings.TrimSpace(value) != value || value == "" || len(value) > 1024 {
					return fmt.Errorf("invalid provider option")
				}
			}
		}
		now := time.Now().UTC()
		refusals := make([]string, 0, len(workerIDs))
		for _, id := range workerIDs {
			binding, err := workerruntime.BuildWorkerBinding(settings, id, now)
			if err != nil {
				refusals = append(refusals, fmt.Sprintf("%s: %v", id, err))
				continue
			}
			if _, err = binding.Catalog.Resolve(workflow, task); err != nil {
				refusals = append(refusals, fmt.Sprintf("%s: %v", id, err))
				continue
			}
			inventory := configuredInventory(binding.Inventory, settings.Workers[id], task)
			reasons, err := backlogadmin.WorkerMatchReasons(task, workflow.Project, inventory, pools, now)
			if err != nil {
				refusals = append(refusals, fmt.Sprintf("%s: %v", id, err))
				continue
			}
			permanent := slices.IndexFunc(reasons, func(reason backlogadmin.ViabilityReason) bool {
				return backlogadmin.PermanentViabilityReason(reason.Code)
			})
			if permanent < 0 {
				return nil
			}
			refusals = append(refusals, fmt.Sprintf("%s: %s", id, reasons[permanent].Detail))
		}
		if len(refusals) == 0 {
			refusals = append(refusals, "no workers are configured")
		}
		return fmt.Errorf("no configured worker supports project, placement and route: %s", strings.Join(refusals, "; "))
	}
}

// configuredInventory is the inventory a configured worker would report for
// itself, which is what readiness reads from a snapshot.
//
// Two things differ between the configuration and that report. A worker
// advertises the capabilities its build supplies as well as its configured
// list, and an older worker lacking one is still excluded at scheduling, from
// its real inventory. And a provider configured with a sole "*" reports the
// concrete models it discovered, which configuration cannot enumerate; "*"
// authorizes any of them, so each concrete model a route of this task names
// stands in for the discovery that would have found it.
func configuredInventory(inventory domain.WorkerInventory, worker config.V2Worker, task domain.Task) domain.WorkerInventory {
	inventory.Capabilities = workerruntime.AdvertisedCapabilities(inventory.Capabilities)
	providers := make([]domain.WorkerProviderInventory, 0, len(inventory.Providers))
	for _, provider := range inventory.Providers {
		authorized := worker.Providers[provider.InstanceID].Models
		models := slices.DeleteFunc(slices.Clone(provider.Models), func(model string) bool { return model == "*" })
		for _, route := range task.Routes {
			if route.ProviderInstanceID == provider.InstanceID && domain.ModelAuthorized(authorized, route.Model) &&
				!slices.Contains(models, route.Model) {
				models = append(models, route.Model)
			}
		}
		provider.Models = models
		providers = append(providers, provider)
	}
	inventory.Providers = providers
	return inventory
}

// configuredQuotaPools is the fleet quota pools a route resolves against,
// projected from configuration the way the coordinator's quota bridge projects
// them. Admission and concurrency are left open: both are dynamic gates that
// scheduling applies, and the router does not treat either as refusing a route.
// A pool no configured provider instance charges cannot resolve any route and
// is omitted, as the bridge refuses it.
func configuredQuotaPools(settings config.BacklogV2) []domain.QuotaPool {
	bindings := coordinatorQuotaPoolBindings(config.Config{BacklogV2: settings})
	pools := make([]domain.QuotaPool, 0, len(bindings))
	for _, binding := range bindings {
		if len(binding.ProviderInstanceIDs) == 0 {
			continue
		}
		pools = append(pools, domain.QuotaPool{
			ID: binding.ID, Provider: binding.Provider, ProviderInstanceIDs: binding.ProviderInstanceIDs,
			MaxConcurrent: max(binding.MaxConcurrent, 1), Admission: domain.AdmissionOpen,
		})
	}
	return pools
}
