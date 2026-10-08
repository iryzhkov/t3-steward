package backlog

import (
	"context"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"maps"
)

type ManifestRoleResolver interface {
	ResolveManifestRoles(context.Context, Manifest) (map[string]domain.RoleSelection, error)
}

func resolveManifestRoles(ctx context.Context, resolver ManifestRoleResolver, m Manifest) (map[string]domain.RoleSelection, error) {
	hasRoles := false
	for _, task := range m.Tasks {
		if task.Role != "" {
			hasRoles = true
			break
		}
	}
	if !hasRoles {
		return nil, nil
	}
	if resolver == nil {
		return nil, fmt.Errorf("%w: coordinator role resolution is required; upgrade the coordinator", ErrValidationUnavailable)
	}
	selections, err := resolver.ResolveManifestRoles(ctx, m)
	if err != nil {
		return nil, err
	}
	for name, task := range m.Tasks {
		if task.Role != "" {
			s, ok := selections[name]
			if !ok || s.Role != task.Role || !s.Valid() {
				return nil, fmt.Errorf("role %s task %s has no valid coordinator selection", task.Role, name)
			}
		}
	}
	return selections, nil
}

// This clone is used only for readiness validation. The authored manifest and
// its digest stay unchanged, while readiness evaluates exactly the sealed routes.
func manifestWithRoleSelections(m Manifest, selections map[string]domain.RoleSelection) (Manifest, error) {
	result := m
	result.Tasks = maps.Clone(m.Tasks)
	result.Role = ""
	result.Options = nil
	for name, task := range result.Tasks {
		if task.Role == "" {
			continue
		}
		selection, ok := selections[name]
		if !ok {
			return Manifest{}, fmt.Errorf("role %s task %s unresolved", task.Role, name)
		}
		task.Role = ""
		task.Options = nil
		route := selection.ProviderRoute()
		task.Routes = []ManifestRoute{{Host: route.WorkerID, Instance: route.ProviderInstanceID, Model: route.Model, Options: maps.Clone(route.Options), QuotaPool: route.QuotaPoolID}}
		result.Tasks[name] = task
	}
	return result, nil
}
