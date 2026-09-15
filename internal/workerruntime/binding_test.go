package workerruntime

import (
	"slices"
	"strings"
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
	project := settings.Projects["steward"]
	project.T3Project = ""
	settings.Projects["steward"] = project
	managed, err := BuildWorkerBinding(settings, "normandy", runtimeTestNow)
	if err != nil {
		t.Fatalf("managed project catalog rejected: %v", err)
	}
	if managed.CatalogRevision == first.CatalogRevision {
		t.Fatal("switching project ownership did not change the catalog fence")
	}
	otherBefore, err := BuildWorkerBinding(settings, "other", runtimeTestNow)
	if err != nil {
		t.Fatal(err)
	}
	settings.Projects["fresh"] = config.V2Project{Type: "fresh", Workers: []string{"normandy"}}
	fresh, err := BuildWorkerBinding(settings, "normandy", runtimeTestNow)
	if err != nil {
		t.Fatal(err)
	}
	otherAfter, err := BuildWorkerBinding(settings, "other", runtimeTestNow)
	if err != nil {
		t.Fatal(err)
	}
	if otherBefore.CatalogRevision != otherAfter.CatalogRevision {
		t.Fatal("fresh default setup changed an unrelated worker catalog")
	}
	if fresh.CatalogRevision == managed.CatalogRevision {
		t.Fatal("fresh project did not change target catalog")
	}
	// Declaring the reserved implicit profile still refuses the project that
	// would have used it, but it no longer refuses the whole binding: one
	// project's configuration must not take the worker, and with it every other
	// project, out of the fleet.
	settings.SetupProfiles["steward-fresh-empty"] = config.V2SetupProfile{Commands: []string{"false"}, Timeout: config.Duration(time.Minute)}
	shadowed, err := BuildWorkerBinding(settings, "normandy", runtimeTestNow)
	if err != nil {
		t.Fatalf("a shadowed implicit profile disabled the worker: %v", err)
	}
	shadowedProject := ""
	for _, rejection := range shadowed.RejectedProjects {
		if strings.Contains(rejection.Reason, "reserved for implicit fresh setup") {
			shadowedProject = rejection.Name
		}
	}
	if shadowedProject == "" {
		t.Fatalf("implicit setup can be shadowed without being reported: %+v", shadowed.RejectedProjects)
	}
	if slices.Contains(shadowed.Catalog.ProjectNames(), shadowedProject) {
		t.Fatal("a project that would use the shadowed implicit profile stayed in the catalog")
	}
	if _, err := BuildWorkerBinding(settings, "missing", runtimeTestNow); err == nil {
		t.Fatal("unknown worker accepted")
	}
}
