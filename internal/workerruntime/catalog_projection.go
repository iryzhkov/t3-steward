package workerruntime

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/config"
	"path/filepath"
	"slices"
	"time"
)

// CatalogProjection contains only this worker's execution catalog. Host paths,
// secrets and shared quota admission policy never cross this boundary.
type CatalogProjection struct {
	SchemaVersion int                              `json:"schemaVersion"`
	Revision      string                           `json:"revision"`
	WorkerID      string                           `json:"workerId"`
	CoordinatorID string                           `json:"coordinatorId"`
	Worker        config.V2Worker                  `json:"worker"`
	Projects      map[string]config.V2Project      `json:"projects"`
	SetupProfiles map[string]config.V2SetupProfile `json:"setupProfiles"`
	Transport     config.V2Transport               `json:"transport"`
	MessageLimits config.V2MessageLimits           `json:"messageLimits"`
	Freshness     config.V2Freshness               `json:"freshness"`
	Leases        config.V2Leases                  `json:"leases"`
}

func BuildCatalogProjection(settings config.BacklogV2, workerID string) (CatalogProjection, error) {
	binding, err := BuildWorkerBinding(settings, workerID, time.Now())
	if err != nil {
		return CatalogProjection{}, err
	}
	p := CatalogProjection{SchemaVersion: 1, Revision: binding.CatalogRevision, WorkerID: workerID, CoordinatorID: settings.Coordinator.ID, Worker: settings.Workers[workerID],
		Projects: map[string]config.V2Project{}, SetupProfiles: map[string]config.V2SetupProfile{}, Transport: settings.Transport, MessageLimits: settings.MessageLimits, Freshness: settings.Freshness, Leases: settings.Leases}
	for name, project := range settings.Projects {
		if !slices.Contains(project.Workers, workerID) {
			continue
		}
		project.Workers = []string{workerID}
		p.Projects[name] = project
	}
	// Binding revisions historically include all setup profiles. Preserve that
	// digest until the versioned projection becomes the only catalog producer.
	for name, profile := range settings.SetupProfiles {
		p.SetupProfiles[name] = profile
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return p, err
	}
	var copy CatalogProjection
	err = json.Unmarshal(raw, &copy)
	return copy, err
}

func (p CatalogProjection) Settings(bootstrap WorkerBootstrap, home string) (config.BacklogV2, error) {
	if p.SchemaVersion != 1 || p.WorkerID != bootstrap.WorkerID || p.CoordinatorID != bootstrap.CoordinatorID ||
		p.Worker.Credential != bootstrap.CredentialRef || p.Worker.Epoch == "" || home == "" {
		return config.BacklogV2{}, errors.New("catalog projection authority mismatch")
	}
	if !slices.Equal(p.Worker.Capabilities, bootstrap.Capabilities) {
		return config.BacklogV2{}, errors.New("catalog capabilities disagree with bootstrap")
	}
	for instance := range p.Worker.Providers {
		if !slices.Contains(bootstrap.ProviderRoutes, instance) {
			return config.BacklogV2{}, fmt.Errorf("provider %q is absent from bootstrap", instance)
		}
	}
	settings := config.Default().BacklogV2
	settings.Mode = "worker"
	settings.Coordinator.ID = p.CoordinatorID
	settings.LocalWorker = config.V2LocalWorker{ID: p.WorkerID, Epoch: p.Worker.Epoch}
	settings.Workers = map[string]config.V2Worker{p.WorkerID: p.Worker}
	settings.Projects = p.Projects
	settings.SetupProfiles = p.SetupProfiles
	settings.Transport = p.Transport
	settings.MessageLimits = p.MessageLimits
	settings.Freshness = p.Freshness
	settings.Leases = p.Leases
	// Pools here are validation-only identifiers. The worker never reconstructs
	// admission or concurrency from them.
	settings.QuotaPools = map[string]config.V2QuotaPool{}
	for instance, provider := range p.Worker.Providers {
		settings.QuotaPools[provider.QuotaPool] = config.V2QuotaPool{Provider: instance, MaxConcurrent: 1}
	}
	root := filepath.Join(home, ".local/state/t3-steward/worker")
	settings.Storage = config.V2Storage{Bundles: filepath.Join(root, "bundles"), Artifacts: filepath.Join(root, "artifacts"), Workspaces: filepath.Join(root, "workspaces")}
	if p.MessageLimits.MaxBytes > 8<<20 || p.MessageLimits.MaxArtifactBytes > 16<<20 || p.Transport.RequestTimeout.D() > 2*time.Minute || p.Freshness.WorkerMaxAge.D() > 5*time.Minute {
		return config.BacklogV2{}, errors.New("catalog exceeds host runtime bounds")
	}
	c := config.Default()
	c.BacklogV2 = settings
	if err := c.Validate(); err != nil {
		return settings, err
	}
	binding, err := BuildWorkerBinding(settings, p.WorkerID, time.Now())
	if err != nil {
		return settings, err
	}
	if binding.CatalogRevision != p.Revision {
		return settings, errors.New("catalog projection digest mismatch")
	}
	return settings, nil
}
