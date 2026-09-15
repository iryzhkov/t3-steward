package domain

import "testing"

func TestAuthoredWorkerRouteRevocation(t *testing.T) {
	inventory := WorkerInventory{ID: "worker", Providers: []WorkerProviderInventory{{InstanceID: "codex", Models: []string{"gpt"}, QuotaPoolID: "pool"}}}
	route := ProviderRoute{WorkerID: "worker", ProviderInstanceID: "codex", Model: "gpt", QuotaPoolID: "pool"}
	if !inventory.AuthorizesRoute(route) {
		t.Fatal("authored route denied while worker draining or observation unavailable")
	}
	for _, field := range []string{"worker", "provider", "model", "pool", "empty-models", "empty-providers"} {
		t.Run(field, func(t *testing.T) {
			candidate := route
			current := inventory
			switch field {
			case "worker":
				candidate.WorkerID = "other"
			case "provider":
				candidate.ProviderInstanceID = "other"
			case "model":
				candidate.Model = "other"
			case "pool":
				candidate.QuotaPoolID = "other"
			case "empty-models":
				current.Providers = []WorkerProviderInventory{{InstanceID: "codex", QuotaPoolID: "pool"}}
			case "empty-providers":
				current.Providers = nil
			}
			if current.AuthorizesRoute(candidate) {
				t.Fatal("revoked route authorized")
			}
		})
	}
}
