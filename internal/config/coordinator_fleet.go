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

	"github.com/iryzhkov/t3-steward/internal/privatefile"
)

const CoordinatorFleetPath = ".config/t3-steward/coordinator-fleet.json"
const coordinatorFleetLimit = 4 << 20

// CoordinatorFleet is authored authorization, never provider observation.
// Fields absent from this projection (transport, quota bindings, setup execution)
// remain operator configuration. A projection cannot invent those bindings.
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
	for name, desired := range fleet.Workers {
		worker, exists := c.BacklogV2.Workers[name]
		if !exists {
			return fmt.Errorf("fleet worker %q needs an explicit local transport binding", name)
		}
		providers := make(map[string]V2Provider, len(desired.ProviderInstances))
		for _, instance := range desired.ProviderInstances {
			provider, exists := worker.Providers[instance]
			// An installed bootstrap route with no desired models grants no execution
			// authorization and needs no invented local quota/provider binding.
			if !exists && len(desired.DesiredModels[instance]) == 0 {
				continue
			}
			if !exists || provider.QuotaPool == "" || !slices.Contains(desired.QuotaPools, provider.QuotaPool) {
				return fmt.Errorf("fleet worker %q provider %q needs an explicit authorized quota binding", name, instance)
			}
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
	c.BacklogV2.Workers = workers
	c.BacklogV2.Projects = projects
	c.coordinatorFleetApplied = true
	c.defaultedFleetProjects = defaulted
	return nil
}

// DefaultedFleetProjects names, sorted, the fleet projects that were loaded
// with a default local binding because backlog_v2.projects has no entry for
// them. It is empty when no projection was applied or every project is bound.
// The coordinator logs one warning per name at startup; the readiness check
// reports the same names as an informational detail on their candidates.
func (c Config) DefaultedFleetProjects() []string {
	return slices.Clone(c.defaultedFleetProjects)
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
