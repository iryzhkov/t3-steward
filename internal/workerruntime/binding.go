package workerruntime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type catalogRuntimeIdentity struct {
	CoordinatorID string
	Transport     config.V2Transport
	MessageLimits config.V2MessageLimits
	Freshness     config.V2Freshness
	Leases        config.V2Leases
}

type WorkerBinding struct {
	CatalogRevision string
	Catalog         *backlog.ProjectCatalog
	Inventory       domain.WorkerInventory
	CredentialRef   string
}

func BuildWorkerBinding(settings config.BacklogV2, workerID string, now time.Time) (WorkerBinding, error) {
	worker, ok := settings.Workers[workerID]
	if !ok || workerID == "" {
		return WorkerBinding{}, fmt.Errorf("worker binding: unknown worker %q", workerID)
	}
	if (!worker.AcceptBacklog && worker.Connection == "") || worker.Address == "" || worker.Credential == "" {
		return WorkerBinding{}, errors.New("worker binding: worker is not eligible or has incomplete transport identity")
	}
	profileNames := make([]string, 0, len(settings.SetupProfiles))
	for name := range settings.SetupProfiles {
		profileNames = append(profileNames, name)
	}
	slices.Sort(profileNames)
	profiles := make([]backlog.SetupProfile, 0, len(profileNames))
	for _, name := range profileNames {
		profile := settings.SetupProfiles[name]
		profiles = append(profiles, backlog.SetupProfile{
			Name: name, Commands: append([]string(nil), profile.Commands...), Timeout: profile.Timeout.D(),
		})
	}
	projectNames := make([]string, 0, len(settings.Projects))
	for name := range settings.Projects {
		projectNames = append(projectNames, name)
	}
	slices.Sort(projectNames)
	projects := make([]backlog.ProjectDefinition, 0, len(projectNames))
	inventoryProjects := make([]domain.WorkerProjectInventory, 0, len(projectNames))
	for _, name := range projectNames {
		project := settings.Projects[name]
		if !slices.Contains(project.Workers, workerID) {
			continue
		}
		if project.SetupProfile == "" {
			return WorkerBinding{}, fmt.Errorf("worker binding: project %q has no setup profile", name)
		}
		projects = append(projects, backlog.ProjectDefinition{
			Name: name, Repository: project.Repository, DefaultRef: project.DefaultRef,
			T3ProjectTemplate: project.T3Project, SetupProfile: project.SetupProfile,
			ResourceLocks:       append([]string(nil), project.ResourceLocks...),
			RequiredCredentials: append([]string(nil), project.Credentials...),
		})
		inventoryProjects = append(inventoryProjects, domain.WorkerProjectInventory{
			Name: name, Available: true, Revision: project.DefaultRef, UpdatedAt: now.UTC(),
		})
	}
	if len(projects) == 0 {
		return WorkerBinding{}, errors.New("worker binding: worker has no eligible projects")
	}
	catalog, err := backlog.NewProjectCatalog(projects, profiles)
	if err != nil {
		return WorkerBinding{}, err
	}
	instanceNames := make([]string, 0, len(worker.Providers))
	for name := range worker.Providers {
		instanceNames = append(instanceNames, name)
	}
	slices.Sort(instanceNames)
	providers := make([]domain.WorkerProviderInventory, 0, len(instanceNames))
	for _, instance := range instanceNames {
		provider := worker.Providers[instance]
		providers = append(providers, domain.WorkerProviderInventory{
			InstanceID: instance, Models: append([]string(nil), provider.Models...),
			QuotaPoolID: provider.QuotaPool, Available: true,
		})
	}
	revisionInput := struct {
		WorkerID string
		Worker   config.V2Worker
		Projects []backlog.ProjectDefinition
		Profiles []backlog.SetupProfile
		Runtime  *catalogRuntimeIdentity `json:",omitempty"`
	}{WorkerID: workerID, Worker: worker, Projects: projects, Profiles: profiles}
	if worker.Connection != "" {
		// Admission is coordinator policy; draining does not replace execution packages.
		revisionInput.Worker.AcceptBacklog = true
		revisionInput.Runtime = &catalogRuntimeIdentity{CoordinatorID: settings.Coordinator.ID, Transport: settings.Transport, MessageLimits: settings.MessageLimits, Freshness: settings.Freshness, Leases: settings.Leases}
	}
	raw, err := json.Marshal(revisionInput)
	if err != nil {
		return WorkerBinding{}, fmt.Errorf("worker binding: catalog revision: %w", err)
	}
	sum := sha256.Sum256(raw)
	revision := hex.EncodeToString(sum[:])
	return WorkerBinding{
		CatalogRevision: revision, Catalog: catalog, CredentialRef: worker.Credential,
		Inventory: domain.WorkerInventory{
			ID: workerID, AcceptBacklog: worker.AcceptBacklog, Health: domain.WorkerHealthReady, CatalogRevision: revision,
			Capabilities: append([]string(nil), worker.Capabilities...),
			Projects:     inventoryProjects, Providers: providers, ObservedAt: now.UTC(),
		},
	}, nil
}
