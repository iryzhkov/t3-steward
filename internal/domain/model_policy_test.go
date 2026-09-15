package domain

import "testing"

func TestModelPolicyPreservesProviderWorkerAndQuotaBoundaries(t *testing.T) {
	inventory := WorkerInventory{ID: "worker", Providers: []WorkerProviderInventory{{InstanceID: "codex", Models: []string{"*"}, QuotaPoolID: "pool"}}}
	route := ProviderRoute{WorkerID: "worker", ProviderInstanceID: "codex", Model: "future-model", QuotaPoolID: "pool"}
	if !inventory.AuthorizesRoute(route) {
		t.Fatal("future model should be authorized")
	}
	for _, field := range []string{"worker", "provider", "quota", "empty", "wildcard"} {
		invalid := route
		switch field {
		case "worker":
			invalid.WorkerID = "other"
		case "provider":
			invalid.ProviderInstanceID = "other"
		case "quota":
			invalid.QuotaPoolID = "other"
		case "empty":
			invalid.Model = ""
		case "wildcard":
			invalid.Model = "*"
		}
		if inventory.AuthorizesRoute(invalid) {
			t.Fatalf("%s boundary bypassed", field)
		}
	}
	inventory.Providers[0].Models = []string{"old"}
	if inventory.AuthorizesRoute(route) {
		t.Fatal("explicit allowlist expanded")
	}
	inventory.Providers[0].Models = nil
	if inventory.AuthorizesRoute(route) {
		t.Fatal("revoked models authorized")
	}
}
