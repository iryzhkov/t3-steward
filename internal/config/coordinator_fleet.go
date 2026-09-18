package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/privatefile"
)

const CoordinatorFleetPath = ".config/t3-steward/coordinator-fleet.json"
const coordinatorFleetLimit = 4 << 20

// CoordinatorFleet is authored authorization, never provider observation.
// Fields absent from this projection (transport, setup execution) remain
// operator configuration. A projection cannot invent those bindings.
//
// A quota binding is authorization too, and since the stage 6 release the
// projection may carry one per worker (quota_bindings): it says which pool an
// instance may charge its work to, never that the instance is installed,
// signed in or advertising a model. The pool itself is still operator
// configuration: backlog_v2.quota_pools defines its provider and concurrency,
// and a binding to a pool this coordinator does not define is not usable here.
type CoordinatorFleet struct {
	Kind          string                             `json:"kind"`
	SchemaVersion int                                `json:"schema_version"`
	CoordinatorID string                             `json:"coordinator_id"`
	Workers       map[string]CoordinatorFleetWorker  `json:"workers"`
	Projects      map[string]CoordinatorFleetProject `json:"projects"`
}
type CoordinatorFleetWorker struct {
	WorkerID          string              `json:"worker_id"`
	CPUClass          string              `json:"cpu_class"`
	ExecutorSlots     int                 `json:"executor_slots"`
	Capabilities      []string            `json:"capabilities"`
	ProviderInstances []string            `json:"provider_instances"`
	DesiredModels     map[string][]string `json:"desired_models"`
	QuotaPools        []string            `json:"quota_pools"`
	// QuotaBindings maps a provider instance to the one quota pool it may draw
	// from. It is optional and closed: a projection rendered before the field
	// existed decodes to nil and is applied exactly as it was before, and a
	// worker that binds nothing renders no key at all.
	QuotaBindings map[string]string `json:"quota_bindings,omitempty"`
}

// Reasons a fleet provider instance is not authorized on a worker after a
// load. They are the vocabulary "t3-steward models" reports, so they are
// written once here rather than spelled out at each reader.
const (
	// DroppedProviderMissingBinding is an instance with desired models that
	// neither the operator file nor the projection binds to a quota pool this
	// coordinator can charge.
	DroppedProviderMissingBinding = "missing binding"
	// DroppedProviderNoModels is an instance the projection authorizes with an
	// empty desired model list, which grants no execution authorization.
	DroppedProviderNoModels = "no models"
)

// DroppedFleetProvider is one provider instance the projection authorized for
// one worker that the load did not install in that worker's catalog.
type DroppedFleetProvider struct {
	Worker   string
	Instance string
	// Reason is DroppedProviderMissingBinding or DroppedProviderNoModels.
	Reason string
	// Remedy is the one sentence the coordinator logs beside a missing
	// binding. It is empty for a reason that is not a fault, and it is written
	// here because the cause (no binding at all, or a pool this coordinator
	// does not define) is known only where the drop happens.
	Remedy string
}
type CoordinatorFleetProject struct {
	Repository      string   `json:"repository"`
	DefaultRef      string   `json:"default_ref"`
	SetupProfile    string   `json:"setup_profile"`
	EligibleWorkers []string `json:"eligible_workers"`
}

func DecodeCoordinatorFleet(raw []byte) (CoordinatorFleet, error) {
	var fleet CoordinatorFleet
	if len(raw) > coordinatorFleetLimit {
		return fleet, errors.New("coordinator fleet projection exceeds 4 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fleet); err != nil {
		return fleet, fmt.Errorf("decode coordinator fleet projection: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fleet, errors.New("coordinator fleet projection must contain one JSON document")
	}
	if fleet.Kind != "steward-coordinator-catalog-input" || fleet.SchemaVersion != 1 || fleet.CoordinatorID == "" {
		return fleet, errors.New("unsupported coordinator fleet projection identity or version")
	}
	if fleet.Workers == nil || fleet.Projects == nil {
		return fleet, errors.New("coordinator fleet projection requires workers and projects")
	}
	for name, worker := range fleet.Workers {
		if name == "" || worker.WorkerID != name || !ValidCPUClass(worker.CPUClass) || worker.CPUClass == "" ||
			worker.ExecutorSlots < 1 || worker.ExecutorSlots > 64 {
			return fleet, fmt.Errorf("invalid coordinator fleet worker %q", name)
		}
		if worker.ProviderInstances == nil || worker.DesiredModels == nil ||
			len(worker.DesiredModels) != len(worker.ProviderInstances) {
			return fleet, fmt.Errorf("fleet worker %q requires explicit provider, model and quota authorization", name)
		}
		seen := map[string]bool{}
		for _, provider := range worker.ProviderInstances {
			if _, exists := worker.DesiredModels[provider]; provider == "" || seen[provider] || !exists {
				return fleet, fmt.Errorf("fleet worker %q has invalid provider authorization", name)
			}
			seen[provider] = true
			models := map[string]bool{}
			for _, model := range worker.DesiredModels[provider] {
				if model == "" || models[model] || (model == "*" && len(worker.DesiredModels[provider]) != 1) {
					return fleet, fmt.Errorf("fleet worker %q has empty or duplicate model authorization", name)
				}
				models[model] = true
			}
		}
		// A binding nobody can act on is dead authorization, and the renderer
		// never writes one: an instance the worker does not run, or a pool it
		// does not join, is a projection fault and is refused here.
		for instance, pool := range worker.QuotaBindings {
			if !seen[instance] {
				return fleet, fmt.Errorf("fleet worker %q binds provider instance %q it does not run", name, instance)
			}
			if pool == "" || !slices.Contains(worker.QuotaPools, pool) {
				return fleet, fmt.Errorf("fleet worker %q binds provider instance %q to quota pool %q it does not join", name, instance, pool)
			}
		}
	}
	return fleet, nil
}

// ApplyCoordinatorFleet replaces only explicitly owned fields, before normal
// configuration validation and catalog construction. Enrollment is unchanged:
// a changed catalog requires the existing explicit revision-fenced enrollment.
func (c *Config) ApplyCoordinatorFleet(fleet CoordinatorFleet) error {
	if c.BacklogV2.Mode != "coordinator" || c.BacklogV2.Coordinator.ID != fleet.CoordinatorID {
		return errors.New("coordinator fleet projection names a different coordinator")
	}
	workers := make(map[string]V2Worker, len(fleet.Workers))
	projects := make(map[string]V2Project, len(fleet.Projects))
	var dropped []DroppedFleetProvider
	for name, desired := range fleet.Workers {
		worker, exists := c.BacklogV2.Workers[name]
		if !exists {
			return fmt.Errorf("fleet worker %q needs an explicit local transport binding", name)
		}
		providers := make(map[string]V2Provider, len(desired.ProviderInstances))
		for _, instance := range desired.ProviderInstances {
			provider, exists := worker.Providers[instance]
			// An installed bootstrap route with no desired models grants no execution
			// authorization and needs no invented local quota/provider binding. It is
			// recorded rather than warned about: it is not a fault, and "models"
			// reports it as the reason that route is not advertised.
			if !exists && len(desired.DesiredModels[instance]) == 0 {
				dropped = append(dropped, DroppedFleetProvider{
					Worker: name, Instance: instance, Reason: DroppedProviderNoModels,
				})
				continue
			}
			// The operator's own binding wins wherever there is one, and a binding
			// to a pool this projection does not authorize for the worker is the
			// operator file contradicting the fleet: it still fails the load.
			if provider.QuotaPool != "" && !slices.Contains(desired.QuotaPools, provider.QuotaPool) {
				return fmt.Errorf("fleet worker %q provider %q is bound to quota pool %q, which the projection does not authorize for that worker",
					name, instance, provider.QuotaPool)
			}
			pool, remedy := c.authorizedQuotaPool(name, instance, provider.QuotaPool, desired)
			if pool == "" {
				// One instance without a usable binding is not a reason to refuse
				// the whole configuration: that took every admin query on the
				// coordinator down the first time a release authorized a provider
				// nobody had hand-bound. It is dropped for this worker and
				// recorded, so startup can warn about it and "models" can report
				// it as the reason a route is missing.
				dropped = append(dropped, DroppedFleetProvider{
					Worker: name, Instance: instance,
					Reason: DroppedProviderMissingBinding, Remedy: remedy,
				})
				continue
			}
			provider.QuotaPool = pool
			provider.Models = slices.Clone(desired.DesiredModels[instance])
			providers[instance] = provider
		}
		worker.Providers = providers
		worker.CPUClass = desired.CPUClass
		worker.Executors.Slots = desired.ExecutorSlots
		worker.Capabilities = slices.Clone(desired.Capabilities)
		workers[name] = worker
	}
	var defaulted []string
	for name, desired := range fleet.Projects {
		project, exists := c.BacklogV2.Projects[name]
		if !exists {
			// A project the projection names and backlog_v2.projects does not
			// is loaded with an empty local binding: no credentials, no
			// resource locks, no directory resources, and the type left to
			// the ordinary defaulting. Nothing in the local binding is
			// required for a plain Git project, and refusing the whole
			// configuration here took every coordinator admin query down
			// the first time a project was published before it was bound.
			// The name is recorded so the coordinator can say so at startup
			// and the readiness check can annotate the project's candidates.
			project = V2Project{}
			defaulted = append(defaulted, name)
		}
		if len(desired.EligibleWorkers) == 0 {
			return fmt.Errorf("fleet project %q requires explicit eligible workers", name)
		}
		for _, worker := range desired.EligibleWorkers {
			if _, exists := workers[worker]; !exists {
				return fmt.Errorf("fleet project %q names unknown worker %q", name, worker)
			}
		}
		project.Repository = desired.Repository
		project.DefaultRef = desired.DefaultRef
		project.SetupProfile = desired.SetupProfile
		project.Workers = slices.Clone(desired.EligibleWorkers)
		projects[name] = project
	}
	slices.Sort(defaulted)
	slices.SortFunc(dropped, func(a, b DroppedFleetProvider) int {
		if a.Worker != b.Worker {
			return strings.Compare(a.Worker, b.Worker)
		}
		return strings.Compare(a.Instance, b.Instance)
	})
	c.BacklogV2.Workers = workers
	c.BacklogV2.Projects = projects
	c.coordinatorFleetApplied = true
	c.defaultedFleetProjects = defaulted
	c.droppedFleetProviders = dropped
	return nil
}

// authorizedQuotaPool resolves the one pool an instance may charge its work
// to, and says what would fix it when there is none. The explicit
// backlog_v2 binding wins; the projected binding is used when the operator
// file binds nothing, which is what makes registering a provider one release
// edit. A pool the projection does not authorize for the worker, or that this
// coordinator does not define, is no binding: acting on it would either
// charge unauthorized work or fail the whole configuration in validation.
func (c *Config) authorizedQuotaPool(worker, instance, explicit string, desired CoordinatorFleetWorker) (string, string) {
	if explicit != "" {
		return explicit, ""
	}
	projected := desired.QuotaBindings[instance]
	if projected == "" || !slices.Contains(desired.QuotaPools, projected) {
		return "", fmt.Sprintf("authorize it in the release with \"upkeeper provider add %s --quota-pool POOL --hosts %s\", "+
			"or set backlog_v2.workers.%s.providers.%s.quota_pool on this coordinator",
			instance, worker, worker, instance)
	}
	if _, defined := c.BacklogV2.QuotaPools[projected]; !defined {
		return "", fmt.Sprintf("the projection binds %s to quota pool %q, which backlog_v2.quota_pools does not define on this coordinator: "+
			"define that pool, or bind the instance with \"upkeeper provider add %s --quota-pool POOL --hosts %s\" to a pool it defines",
			instance, projected, instance, worker)
	}
	return projected, ""
}

// DefaultedFleetProjects names, sorted, the fleet projects that were loaded
// with a default local binding because backlog_v2.projects has no entry for
// them. It is empty when no projection was applied or every project is bound.
// The coordinator logs one warning per name at startup; the readiness check
// reports the same names as an informational detail on their candidates.
func (c Config) DefaultedFleetProjects() []string {
	return slices.Clone(c.defaultedFleetProjects)
}

// LifecycleView returns the configuration with the state derived from a fleet
// projection cleared, so that two loads of the same operator file compare
// equal whatever the projection defaulted. The coordinator's reload check
// compares everything outside backlog_v2 with reflect.DeepEqual to tell a
// catalog change from a lifecycle change; with the derived list included, the
// first projection that added a project without a local binding was refused
// as a lifecycle change and the project stayed unknown until a restart.
func (c Config) LifecycleView() Config {
	c.defaultedFleetProjects = nil
	c.droppedFleetProviders = nil
	c.coordinatorFleetApplied = false
	return c
}

// DroppedFleetProviders names, sorted by worker and then instance, every
// provider instance the projection authorized that this load did not install
// in a worker's catalog, with the reason and, for a missing binding, the
// remedy. It is empty when no projection was applied or every authorized
// instance was bound. The coordinator logs one warning per missing binding at
// startup; "t3-steward models" reports the reason beside the route.
func (c Config) DroppedFleetProviders() []DroppedFleetProvider {
	return slices.Clone(c.droppedFleetProviders)
}

func (c *Config) applyCoordinatorFleet(home string) error {
	if c.BacklogV2.Mode != "coordinator" || home == "" {
		return nil
	}
	path := filepath.Join(home, CoordinatorFleetPath)
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	raw, err := privatefile.Read(path, coordinatorFleetLimit)
	if err != nil {
		return fmt.Errorf("read owner-only coordinator fleet projection: %w", err)
	}
	fleet, err := DecodeCoordinatorFleet(raw)
	if err != nil {
		return err
	}
	return c.ApplyCoordinatorFleet(fleet)
}
