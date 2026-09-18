package backlogadmin

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// projects answers the projects query from the loaded catalog and the
// workers view. Nothing here is a new fact: the catalog is what the
// coordinator was configured with, and every worker judgement is the one the
// workers view already makes, so this cannot disagree with "backlog workers".
func (v view) projects(settings ViabilitySettings, filter Filter) []Project {
	workers := v.workersResponse(Filter{})
	result := make([]Project, 0, len(settings.Projects))
	for _, definition := range settings.Projects {
		if filter.Project != "" && definition.Name != filter.Project {
			continue
		}
		result = append(result, Project{
			Name: definition.Name, Repository: definition.Repository,
			DefaultRef: definition.DefaultRef, Type: definition.Type,
			SetupProfile: definition.SetupProfile,
			Workers:      projectWorkers(definition, settings.ProjectWorkers[definition.Name], workers),
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// projectWorkers lists the workers that could take one project's work, sorted
// by worker id. A worker is listed when the configuration names it for the
// project or its inventory advertises the project; the two facts are reported
// separately because they fail separately.
func projectWorkers(project backlog.ProjectDefinition, configured []string, workers []Worker) []ProjectWorker {
	result := make([]ProjectWorker, 0, len(workers))
	seen := make(map[string]bool, len(workers))
	for _, worker := range workers {
		id := worker.Snapshot.WorkerID
		if id == "" && worker.Requirement != nil {
			id = worker.Requirement.WorkerID
		}
		if id == "" {
			continue
		}
		entry := ProjectWorker{
			Worker:     id,
			Configured: slices.Contains(configured, id),
			Advertises: inventoryAdvertisesProject(worker.Snapshot.Inventory, project.Name),
			Enrolled:   worker.Enrolled,
			State:      worker.State,
			Health:     worker.Health,
		}
		if !entry.Configured && !entry.Advertises {
			continue
		}
		entry.Ready = worker.Enrolled && !worker.Stale && worker.Snapshot.Connected &&
			worker.State == "observed" && worker.Health == string(domain.WorkerHealthReady)
		entry.Routes = advertisedRoutes(worker.Snapshot.Inventory)
		seen[id] = true
		result = append(result, entry)
	}
	// A configured worker the coordinator has never heard from is still
	// eligible on paper, and an operator reading the list should see that it
	// is missing rather than find it silently dropped.
	for _, id := range configured {
		if seen[id] {
			continue
		}
		seen[id] = true
		result = append(result, ProjectWorker{Worker: id, Configured: true, State: "unknown"})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Worker < result[j].Worker })
	return result
}

func inventoryAdvertisesProject(inventory domain.WorkerInventory, name string) bool {
	for _, project := range inventory.Projects {
		if project.Name == name && project.Available {
			return true
		}
	}
	return false
}

// advertisedRoutes lists the available instance, model and pool triples one
// inventory offers, sorted by instance then model.
func advertisedRoutes(inventory domain.WorkerInventory) []ProjectRoute {
	var routes []ProjectRoute
	for _, provider := range inventory.Providers {
		if !provider.Available {
			continue
		}
		for _, model := range provider.Models {
			routes = append(routes, ProjectRoute{
				Instance: provider.InstanceID, Model: model, QuotaPool: provider.QuotaPoolID,
			})
		}
	}
	sort.Slice(routes, func(i, j int) bool {
		if routes[i].Instance != routes[j].Instance {
			return routes[i].Instance < routes[j].Instance
		}
		return routes[i].Model < routes[j].Model
	})
	return routes
}

// advertisedRouteList names every instance/model pair the eligible workers of
// a project advertise, deduplicated and sorted, for a refusal that has to say
// what the caller could have asked for instead. It reads "(none)" when no
// eligible worker advertises anything.
func advertisedRouteList(projectName string, workers []viabilityWorker) string {
	seen := make(map[string]bool)
	var pairs []string
	for _, worker := range workers {
		if !worker.hasSnapshot || !inventoryAdvertisesProject(worker.inventory, projectName) {
			continue
		}
		for _, route := range advertisedRoutes(worker.inventory) {
			pair := route.Instance + "/" + route.Model
			if seen[pair] {
				continue
			}
			seen[pair] = true
			pairs = append(pairs, pair)
		}
	}
	if len(pairs) == 0 {
		return "(none)"
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ", ")
}

// noRouteDetail is the one sentence a route-less task is refused with. It
// names the routes the fleet offers, because the fix is to declare one of them
// and the reader should not need a second command to find out which.
func noRouteDetail(taskName, projectName string, workers []viabilityWorker) string {
	return fmt.Sprintf("task %q declares no provider route (instance and model); "+
		"the coordinator never chooses one, and the eligible workers of project %q advertise: %s",
		taskName, projectName, advertisedRouteList(projectName, workers))
}
