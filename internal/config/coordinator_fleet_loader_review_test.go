package config

import (
	"encoding/json"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"testing"
)

func TestCoordinatorFleetFullLoaderRevocation(t *testing.T) {
	for _, kind := range []string{"models", "providers", "projects", "workers"} {
		t.Run(kind, func(t *testing.T) {
			home := t.TempDir()
			originalHome := coordinatorClientHome
			coordinatorClientHome = func() string { return home }
			t.Cleanup(func() { coordinatorClientHome = originalHome })
			cfg := validBacklogV2Config(t)
			raw, err := yaml.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(home, "config.yaml")
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			fleet := CoordinatorFleet{Kind: "steward-coordinator-catalog-input", SchemaVersion: 1, CoordinatorID: "normandy",
				Workers: map[string]CoordinatorFleetWorker{"normandy": {WorkerID: "normandy", CPUClass: "high", ExecutorSlots: 1,
					Capabilities: []string{"git", "huyang"}, ProviderInstances: []string{"codex"}, DesiredModels: map[string][]string{"codex": {"gpt"}}, QuotaPools: []string{"codex-main"}}},
				Projects: map[string]CoordinatorFleetProject{"steward": {Repository: "https://example.invalid/steward.git", DefaultRef: "main", SetupProfile: "go", EligibleWorkers: []string{"normandy"}}}}
			switch kind {
			case "models":
				w := fleet.Workers["normandy"]
				w.DesiredModels["codex"] = []string{}
				fleet.Workers["normandy"] = w
			case "providers":
				w := fleet.Workers["normandy"]
				w.ProviderInstances = []string{}
				w.DesiredModels = map[string][]string{}
				w.QuotaPools = []string{}
				fleet.Workers["normandy"] = w
			case "projects":
				fleet.Projects = map[string]CoordinatorFleetProject{}
			case "workers":
				fleet.Workers = map[string]CoordinatorFleetWorker{}
				fleet.Projects = map[string]CoordinatorFleetProject{}
			}
			projection, err := json.Marshal(fleet)
			if err != nil {
				t.Fatal(err)
			}
			dest := filepath.Join(home, CoordinatorFleetPath)
			if err := os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(dest, projection, 0600); err != nil {
				t.Fatal(err)
			}
			for _, load := range []func(string) (Config, error){LoadFile, Load} {
				loaded, err := load(path)
				if err != nil {
					t.Fatalf("full loader rejected %s revocation: %v", kind, err)
				}
				if err := loaded.Validate(); err != nil {
					t.Fatal(err)
				}
				switch kind {
				case "models":
					if len(loaded.BacklogV2.Workers["normandy"].Providers["codex"].Models) != 0 {
						t.Fatal("models survived")
					}
				case "providers":
					if len(loaded.BacklogV2.Workers["normandy"].Providers) != 0 {
						t.Fatal("providers survived")
					}
				case "projects":
					if len(loaded.BacklogV2.Projects) != 0 {
						t.Fatal("projects survived")
					}
				case "workers":
					if len(loaded.BacklogV2.Workers) != 0 {
						t.Fatal("workers survived")
					}
				}
			}
		})
	}
}
func TestEmptyLocalCatalogStillRefused(t *testing.T) {
	for _, mode := range []string{"coordinator", "worker"} {
		c := validBacklogV2Config(t)
		c.BacklogV2.Mode = mode
		c.BacklogV2.Workers = map[string]V2Worker{}
		if err := c.Validate(); err == nil {
			t.Fatalf("ordinary %s catalog accepted empty workers", mode)
		}
	}
	c := validBacklogV2Config(t)
	if err := c.ValidateWorkerCatalog(); err == nil {
		t.Fatal("coordinator used worker-only validation")
	}
}
