package config

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

func (c *Config) validateBacklogV2() error {
	v := &c.BacklogV2
	v.Mode = strings.ToLower(strings.TrimSpace(v.Mode))
	switch v.Mode {
	case "", "disabled":
		v.Mode = "disabled"
		return nil
	case "coordinator", "worker":
	default:
		return fmt.Errorf("backlog_v2: mode must be disabled, coordinator, or worker (got %q)", v.Mode)
	}
	if v.Mode == "coordinator" && c.Backlog.Enabled {
		return errors.New("backlog_v2: coordinator mode and legacy backlog.enabled are mutually exclusive")
	}
	if strings.TrimSpace(v.Coordinator.ID) == "" {
		return errors.New("backlog_v2: coordinator.id is required")
	}
	if v.Mode == "coordinator" && v.StartupAdmission != "closed" {
		return errors.New("backlog_v2: startup_admission must be closed")
	}
	if len(v.Workers) == 0 || len(v.Projects) == 0 || len(v.QuotaPools) == 0 {
		return errors.New("backlog_v2: workers, projects, and quota_pools are required")
	}
	if v.Transport.Kind != "ssh" {
		return fmt.Errorf("backlog_v2: transport.kind must be ssh (got %q)", v.Transport.Kind)
	}
	if v.Transport.RequestTimeout.D() <= 0 {
		return errors.New("backlog_v2: transport.request_timeout must be positive")
	}
	if v.MessageLimits.MaxBytes <= 0 || v.MessageLimits.MaxFiles <= 0 ||
		v.MessageLimits.MaxArtifactBytes <= 0 {
		return errors.New("backlog_v2: message byte, file, and artifact limits must be positive")
	}
	if v.Freshness.WorkerMaxAge.D() <= 0 || v.Freshness.QuotaMaxAge.D() <= 0 {
		return errors.New("backlog_v2: freshness limits must be positive")
	}
	if v.Leases.Duration.D() <= 0 || v.Leases.RenewInterval.D() <= 0 ||
		v.Leases.RenewInterval.D() >= v.Leases.Duration.D() {
		return errors.New("backlog_v2: lease renew_interval must be positive and less than duration")
	}
	if v.Scheduling.Interval.D() <= 0 || v.Scheduling.CatchUpMax < 1 {
		return errors.New("backlog_v2: scheduling interval and catch_up_max must be positive")
	}
	if err := validateV2Storage(v.Storage); err != nil {
		return err
	}
	for id, pool := range v.QuotaPools {
		if strings.TrimSpace(id) != id || id == "" ||
			strings.TrimSpace(pool.Provider) != pool.Provider || pool.Provider == "" {
			return fmt.Errorf("backlog_v2: quota pool %q requires trimmed id and provider", id)
		}
		if pool.MaxConcurrent < 1 {
			return fmt.Errorf("backlog_v2: quota pool %q max_concurrent must be positive", id)
		}
	}
	providerInstancePools := make(map[string]string)
	usedQuotaPools := make(map[string]bool)
	for id, worker := range v.Workers {
		if strings.TrimSpace(id) == "" || strings.TrimSpace(worker.Address) == "" ||
			strings.TrimSpace(worker.Epoch) != worker.Epoch || worker.Epoch == "" ||
			strings.TrimSpace(worker.Credential) == "" {
			return fmt.Errorf("backlog_v2: worker %q requires address, epoch, and credential", id)
		}
		if !worker.AcceptBacklog {
			return fmt.Errorf("backlog_v2: configured worker %q must accept backlog work", id)
		}
		if len(worker.Providers) == 0 {
			return fmt.Errorf("backlog_v2: worker %q requires at least one provider", id)
		}
		for instance, provider := range worker.Providers {
			if strings.TrimSpace(instance) == "" || len(provider.Models) == 0 {
				return fmt.Errorf("backlog_v2: worker %q provider %q requires models", id, instance)
			}
			if _, ok := v.QuotaPools[provider.QuotaPool]; !ok {
				return fmt.Errorf("backlog_v2: worker %q provider %q references unknown quota pool %q", id, instance, provider.QuotaPool)
			}
			if owner, exists := providerInstancePools[instance]; exists && owner != provider.QuotaPool {
				return fmt.Errorf("backlog_v2: provider instance %q belongs to quota pools %q and %q", instance, owner, provider.QuotaPool)
			}
			providerInstancePools[instance] = provider.QuotaPool
			usedQuotaPools[provider.QuotaPool] = true
		}
	}
	for poolID := range v.QuotaPools {
		if !usedQuotaPools[poolID] {
			return fmt.Errorf("backlog_v2: quota pool %q has no provider instances", poolID)
		}
	}
	for name, profile := range v.SetupProfiles {
		if strings.TrimSpace(name) == "" || len(profile.Commands) == 0 || profile.Timeout.D() <= 0 {
			return fmt.Errorf("backlog_v2: setup profile %q requires commands and a positive timeout", name)
		}
		for _, command := range profile.Commands {
			if strings.TrimSpace(command) == "" {
				return fmt.Errorf("backlog_v2: setup profile %q contains an empty command", name)
			}
		}
	}
	for name, project := range v.Projects {
		if strings.TrimSpace(name) == "" || strings.TrimSpace(project.Repository) == "" ||
			strings.TrimSpace(project.DefaultRef) == "" || strings.TrimSpace(project.T3Project) == "" {
			return fmt.Errorf("backlog_v2: project %q requires repository, default_ref, and t3_project", name)
		}
		if project.SetupProfile != "" {
			if _, ok := v.SetupProfiles[project.SetupProfile]; !ok {
				return fmt.Errorf("backlog_v2: project %q references unknown setup profile %q", name, project.SetupProfile)
			}
		}
		if len(project.Workers) == 0 {
			return fmt.Errorf("backlog_v2: project %q requires eligible workers", name)
		}
		for _, worker := range project.Workers {
			if _, ok := v.Workers[worker]; !ok {
				return fmt.Errorf("backlog_v2: project %q references unknown worker %q", name, worker)
			}
		}
		for _, credential := range project.Credentials {
			if strings.TrimSpace(credential) == "" {
				return fmt.Errorf("backlog_v2: project %q contains an empty credential reference", name)
			}
		}
	}
	if v.Mode == "worker" {
		local := v.LocalWorker
		if strings.TrimSpace(local.ID) != local.ID || local.ID == "" ||
			strings.TrimSpace(local.Epoch) != local.Epoch || local.Epoch == "" ||
			local.CoordinatorEpoch < 0 {
			return errors.New("backlog_v2: worker mode requires trimmed local_worker id/epoch and a nonnegative coordinator_epoch")
		}
		if _, ok := v.Workers[local.ID]; !ok {
			return fmt.Errorf("backlog_v2: local worker %q is not declared in workers", local.ID)
		}
		if v.Workers[local.ID].Epoch != local.Epoch {
			return fmt.Errorf("backlog_v2: local worker %q epoch does not match its worker declaration", local.ID)
		}
	}
	return nil
}

func validateV2Storage(storage V2Storage) error {
	roots := []struct {
		name string
		path string
	}{
		{"bundles", storage.Bundles},
		{"artifacts", storage.Artifacts},
		{"workspaces", storage.Workspaces},
	}
	cleaned := make([]string, len(roots))
	for i, root := range roots {
		if !filepath.IsAbs(root.path) {
			return fmt.Errorf("backlog_v2: storage.%s must be an absolute path", root.name)
		}
		cleaned[i] = filepath.Clean(root.path)
		if cleaned[i] == string(filepath.Separator) {
			return fmt.Errorf("backlog_v2: storage.%s must not be the filesystem root", root.name)
		}
	}
	for i := range cleaned {
		for j := i + 1; j < len(cleaned); j++ {
			if cleaned[i] == cleaned[j] ||
				strings.HasPrefix(cleaned[i], cleaned[j]+string(filepath.Separator)) ||
				strings.HasPrefix(cleaned[j], cleaned[i]+string(filepath.Separator)) {
				return fmt.Errorf("backlog_v2: storage roots %s and %s must not overlap", roots[i].name, roots[j].name)
			}
		}
	}
	return nil
}
