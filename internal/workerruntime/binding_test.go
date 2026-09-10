package workerruntime

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
)

func TestBuildWorkerBindingIsDeterministicAndWorkerScoped(t *testing.T) {
	settings := config.BacklogV2{
		Workers: map[string]config.V2Worker{
			"normandy": {
				Address: "normandy", AcceptBacklog: true, Credential: "ssh:normandy",
				Capabilities: []string{"internet"},
				Providers: map[string]config.V2Provider{
					"codex": {Models: []string{"gpt-5.6-sol"}, QuotaPool: "codex-main"},
				},
			},
			"other": {
				Address: "other", AcceptBacklog: true, Credential: "ssh:other",
				Providers: map[string]config.V2Provider{"codex": {Models: []string{"gpt"}, QuotaPool: "codex-main"}},
			},
		},
		Projects: map[string]config.V2Project{
			"steward": {
				Repository: "https://example.com/steward.git", DefaultRef: "main",
				T3Project: "development", SetupProfile: "go", Workers: []string{"normandy"},
			},
			"other": {
				Repository: "https://example.com/other.git", DefaultRef: "main",
				T3Project: "other", SetupProfile: "go", Workers: []string{"other"},
			},
		},
		SetupProfiles: map[string]config.V2SetupProfile{
			"go": {Commands: []string{"go mod download"}, Timeout: config.Duration(time.Minute)},
		},
	}
	first, err := BuildWorkerBinding(settings, "normandy", runtimeTestNow)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildWorkerBinding(settings, "normandy", runtimeTestNow)
	if err != nil {
		t.Fatal(err)
	}
	if first.CatalogRevision == "" || first.CatalogRevision != second.CatalogRevision {
		t.Fatalf("catalog revisions = %q and %q", first.CatalogRevision, second.CatalogRevision)
	}
	if first.CredentialRef != "ssh:normandy" || len(first.Inventory.Projects) != 1 ||
		first.Inventory.Projects[0].Name != "steward" || len(first.Inventory.Providers) != 1 {
		t.Fatalf("binding = %+v", first)
	}
	if _, err := BuildWorkerBinding(settings, "missing", runtimeTestNow); err == nil {
		t.Fatal("unknown worker accepted")
	}
}
