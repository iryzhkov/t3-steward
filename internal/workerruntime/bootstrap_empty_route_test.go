package workerruntime

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
)

func TestBootstrapOnlyEmptyProviderPreservesCatalogBinding(t *testing.T) {
	for _, models := range [][]string{{}, {"unauthorized-model"}} {
		t.Run(map[bool]string{true: "empty", false: "nonempty"}[len(models) == 0], func(t *testing.T) {
			cfg := config.Default()
			cfg.BacklogV2.Mode = "coordinator"
			cfg.BacklogV2.Coordinator.ID = "coordinator"
			cfg.BacklogV2.Workers = map[string]config.V2Worker{"homelab": {
				Address: "homelab", Epoch: "epoch", Credential: "secretref:f02-protocol/homelab", AcceptBacklog: true,
				CPUClass: "low", Executors: config.V2Executors{Slots: 2}, Capabilities: []string{"git", "huyang"},
				Providers: map[string]config.V2Provider{"codex": {Models: []string{"gpt"}, QuotaPool: "pool"}},
			}}
			cfg.BacklogV2.Projects = map[string]config.V2Project{"project": {
				Repository: "https://example.invalid/repo.git", DefaultRef: "main", SetupProfile: "go", T3Project: "project", Workers: []string{"homelab"},
			}}
			cfg.BacklogV2.SetupProfiles = map[string]config.V2SetupProfile{"go": {Commands: []string{"true"}, Timeout: config.Duration(time.Minute)}}
			now := time.Now()
			before, err := BuildWorkerBinding(cfg.BacklogV2, "homelab", now)
			if err != nil {
				t.Fatal(err)
			}
			original, _ := json.Marshal(cfg.BacklogV2)
			authored := config.CoordinatorFleet{Kind: "steward-coordinator-catalog-input", SchemaVersion: 1, CoordinatorID: "coordinator",
				Workers: map[string]config.CoordinatorFleetWorker{"homelab": {
					WorkerID: "homelab", CPUClass: "low", ExecutorSlots: 2, Capabilities: []string{"git", "huyang"},
					ProviderInstances: []string{"codex", "opencode"}, DesiredModels: map[string][]string{"codex": {"gpt"}, "opencode": models}, QuotaPools: []string{"pool"},
				}},
				Projects: map[string]config.CoordinatorFleetProject{"project": {
					Repository: "https://example.invalid/repo.git", DefaultRef: "main", SetupProfile: "go", EligibleWorkers: []string{"homelab"},
				}},
			}
			raw, _ := json.Marshal(authored)
			decoded, err := config.DecodeCoordinatorFleet(raw)
			if err != nil {
				t.Fatal(err)
			}
			err = cfg.ApplyCoordinatorFleet(decoded)
			if len(models) > 0 {
				if err == nil {
					t.Fatal("unmapped nonempty provider was authorized")
				}
				after, _ := json.Marshal(cfg.BacklogV2)
				if string(after) != string(original) {
					t.Fatal("refusal changed config")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			after, err := BuildWorkerBinding(cfg.BacklogV2, "homelab", now)
			if err != nil {
				t.Fatal(err)
			}
			if before.CatalogRevision != after.CatalogRevision || !reflect.DeepEqual(before.Inventory, after.Inventory) {
				t.Fatal("bootstrap-only route changed effective catalog")
			}
			if _, exists := cfg.BacklogV2.Workers["homelab"].Providers["opencode"]; exists {
				t.Fatal("invented local provider/quota binding")
			}
			if !reflect.DeepEqual(authored.Workers["homelab"].ProviderInstances, []string{"codex", "opencode"}) {
				t.Fatal("authored bootstrap routes changed")
			}
		})
	}
}
