package workerruntime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/directoryresource"
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
	// RejectedProjects names the configured projects this binding had to leave
	// out, with the exact validation failure for each. The binding is usable
	// without them; the operator still has to be told which project is broken
	// and why, by name.
	RejectedProjects []backlog.ProjectRejection
}

// CatalogIssues renders the rejected projects as one line each, for the health
// surfaces that report configuration problems.
func (b WorkerBinding) CatalogIssues() []string {
	issues := make([]string, 0, len(b.RejectedProjects))
	for _, rejection := range b.RejectedProjects {
		issues = append(issues, "project:"+rejection.Name+":invalid: "+rejection.Reason)
	}
	return issues
}

func rejectionList(rejections []backlog.ProjectRejection) string {
	parts := make([]string, 0, len(rejections))
	for _, rejection := range rejections {
		parts = append(parts, rejection.Error())
	}
	return strings.Join(parts, "; ")
}

// BuildFleetDefinitions is the coordinator's view of the same catalog
// BuildWorkerBinding gives one worker: every configured project and setup
// profile, with no per-worker filter.
//
// It returns definitions rather than a constructed catalog, and it drops
// nothing. A project whose repository syntax is wrong must be reported as
// having a wrong repository, not as a project this coordinator has never heard
// of, and the catalog constructor cannot hold such a project at all.
//
// It lives beside BuildWorkerBinding so the two cannot drift about what a
// project definition contains.
func BuildFleetDefinitions(settings config.BacklogV2) ([]backlog.ProjectDefinition, []backlog.SetupProfile) {
	profiles := fleetSetupProfiles(settings)
	projectNames := make([]string, 0, len(settings.Projects))
	for name := range settings.Projects {
		projectNames = append(projectNames, name)
	}
	slices.Sort(projectNames)
	projects := make([]backlog.ProjectDefinition, 0, len(projectNames))
	for _, name := range projectNames {
		project := settings.Projects[name]
		setupProfile := project.SetupProfile
		if setupProfile == "" {
			setupProfile = implicitFreshSetupProfile
			if !slices.ContainsFunc(profiles, func(p backlog.SetupProfile) bool { return p.Name == setupProfile }) {
				profiles = append(profiles, backlog.SetupProfile{Name: setupProfile, Timeout: time.Minute})
			}
		}
		projects = append(projects, backlog.ProjectDefinition{
			DirectoryBindings: directoryresource.CloneBindings(project.DirectoryResources),
			Type:              project.Type, Name: name, Repository: project.Repository,
			DefaultRef: project.DefaultRef, T3ProjectTemplate: project.T3Project,
			SetupProfile:        setupProfile,
			ResourceLocks:       append([]string(nil), project.ResourceLocks...),
			RequiredCredentials: append([]string(nil), project.Credentials...),
		})
	}
	return projects, profiles
}

// FleetCatalogIssues names every configured project this coordinator cannot
// use, with the exact validation failure for each.
//
// It is computed from the fleet definitions rather than from one worker's
// binding, so a project that is broken and assigned to no worker is still
// reported. An isolated failure that nothing else notices has to be announced,
// or the operator only learns about it from a refused submission.
func FleetCatalogIssues(settings config.BacklogV2) []string {
	projects, profiles := BuildFleetDefinitions(settings)
	_, _, rejected := backlog.PartitionCatalog(projects, profiles)
	issues := make([]string, 0, len(rejected))
	for _, rejection := range rejected {
		issues = append(issues, "project:"+rejection.Name+":invalid: "+rejection.Reason)
	}
	return issues
}

// implicitFreshSetupProfile is the profile a fresh-workspace project is given
// when it declares none.
const implicitFreshSetupProfile = "steward-fresh-empty"

func fleetSetupProfiles(settings config.BacklogV2) []backlog.SetupProfile {
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
	return profiles
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
		if project.SetupProfile == "" && project.Type == backlog.EnvironmentFresh {
			const emptyProfile = "steward-fresh-empty"
			if _, exists := settings.SetupProfiles[emptyProfile]; exists {
				return WorkerBinding{}, errors.New("worker binding: steward-fresh-empty is reserved for implicit fresh setup")
			}
			project.SetupProfile = emptyProfile
			if !slices.ContainsFunc(profiles, func(p backlog.SetupProfile) bool { return p.Name == emptyProfile }) {
				profiles = append(profiles, backlog.SetupProfile{Name: emptyProfile, Timeout: time.Minute})
			}
		}
		if project.SetupProfile == "" {
			return WorkerBinding{}, fmt.Errorf("worker binding: project %q has no setup profile", name)
		}
		if err := directoryresource.ValidateCatalog(project.DirectoryResources); err != nil {
			return WorkerBinding{}, err
		}
		var directories []directoryresource.Binding
		for _, binding := range project.DirectoryResources {
			if !slices.Contains(project.Workers, binding.Identity.Registration.WorkerID) {
				return WorkerBinding{}, errors.New("directory worker is not eligible for project")
			}
			if binding.Identity.Registration.WorkerID == workerID {
				directories = append(directories, binding)
			}
		}
		projects = append(projects, backlog.ProjectDefinition{
			DirectoryBindings: directoryresource.CloneBindings(directories),
			Type:              project.Type, Name: name, Repository: project.Repository, DefaultRef: project.DefaultRef,
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
	// A project the catalog cannot hold is isolated rather than fatal. Failing
	// the whole binding meant one malformed repository URL stopped every worker
	// on the coordinator from reporting, which is a fleet outage caused by one
	// project's configuration.
	projects, profiles, rejected := backlog.PartitionCatalog(projects, profiles)
	if len(rejected) != 0 {
		usable := make([]domain.WorkerProjectInventory, 0, len(projects))
		for _, entry := range inventoryProjects {
			if slices.ContainsFunc(projects, func(p backlog.ProjectDefinition) bool { return p.Name == entry.Name }) {
				usable = append(usable, entry)
			}
		}
		// A project that cannot be prepared is not advertised as available. The
		// worker would accept work for it and fail at preparation time, which is
		// the class of failure this whole readiness path exists to move earlier.
		inventoryProjects = usable
	}
	if len(projects) == 0 {
		return WorkerBinding{}, fmt.Errorf(
			"worker binding: every eligible project of worker %q is misconfigured: %s",
			workerID, rejectionList(rejected))
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
		RejectedProjects: rejected,
		Inventory: domain.WorkerInventory{
			ID: workerID, AcceptBacklog: worker.AcceptBacklog, Health: domain.WorkerHealthReady, CatalogRevision: revision,
			Capabilities: append([]string(nil), worker.Capabilities...),
			Projects:     inventoryProjects, Providers: providers, ObservedAt: now.UTC(),
			// The operator assigns the CPU class and the allocatable executor
			// capacity; both are configuration, not measurements. Pressure is
			// absent here on purpose: it is the worker's own live observation
			// of itself and arrives with a capacity report, never from the
			// coordinator's view of its own configuration file.
			CPUClass: domain.CPUClass(worker.CPUClass),
			Allocatable: domain.AllocatableCapacity{
				ExecutorSlots: worker.Executors.Slots,
				CPUUnits:      worker.Executors.CPUUnits,
				MemoryMB:      worker.Executors.MemoryMB,
				ScratchMB:     worker.Executors.ScratchMB,
			},
		},
	}, nil
}
