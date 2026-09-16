package domain

import "slices"

// ModelAuthorized interprets a sole "*" as explicit authorization for any
// concrete model on this provider. Discovery, authentication and routing still
// require a concrete observed model; "*" is never a runnable model.
func ModelAuthorized(allowed []string, model string) bool {
	return model != "" && model != "*" && ((len(allowed) == 1 && allowed[0] == "*") || slices.Contains(allowed, model))
}

// ModelsObserved reports whether a worker observation satisfies an authored
// model policy: every concrete authorized model must be observed, and a sole
// "*" is satisfied by any non-empty observation. An empty policy authorizes
// nothing and is never satisfied.
func ModelsObserved(authorized, observed []string) bool {
	if len(authorized) == 0 {
		return false
	}
	for _, model := range authorized {
		if model == "*" {
			if len(authorized) != 1 || len(observed) == 0 {
				return false
			}
			continue
		}
		if !slices.Contains(observed, model) {
			return false
		}
	}
	return true
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
