package domain

import "slices"

// ModelAuthorized interprets a sole "*" as explicit authorization for any
// concrete model on this provider. Discovery, authentication and routing still
// require a concrete observed model; "*" is never a runnable model.
func ModelAuthorized(allowed []string, model string) bool {
	return model != "" && model != "*" && ((len(allowed) == 1 && allowed[0] == "*") || slices.Contains(allowed, model))
}

// AuthorizesRoute checks an operator-authored inventory, never a worker observation.
// Empty provider/model lists revoke authorization; draining does not revoke existing work.
func (i WorkerInventory) AuthorizesRoute(route ProviderRoute) bool {
	if route.WorkerID != i.ID {
		return false
	}
	for _, provider := range i.Providers {
		if provider.InstanceID == route.ProviderInstanceID && provider.QuotaPoolID == route.QuotaPoolID && ModelAuthorized(provider.Models, route.Model) {
			return true
		}
	}
	return false
}
