package domain

import "slices"

// AuthorizesRoute checks an operator-authored inventory, never a worker observation.
// Empty provider/model lists revoke authorization; draining does not revoke existing work.
func (i WorkerInventory) AuthorizesRoute(route ProviderRoute) bool {
	if route.WorkerID != i.ID {
		return false
	}
	for _, provider := range i.Providers {
		if provider.InstanceID == route.ProviderInstanceID && provider.QuotaPoolID == route.QuotaPoolID && slices.Contains(provider.Models, route.Model) {
			return true
		}
	}
	return false
}
