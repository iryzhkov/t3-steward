package main

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestFleetRevocationCatalogAndCoordinatorStartup(t *testing.T) {
	for _, kind := range []string{"models", "providers", "projects", "workers"} {
		t.Run(kind, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			cfg := config.Default()
			cfg.T3.URL = "http://127.0.0.1:1"
			setCoordinatorTestRoots(t, &cfg)
			cfg.BacklogV2.Mode = "coordinator"
			cfg.BacklogV2.Coordinator.ID = "normandy"
			w := cfg.BacklogV2.Workers["normandy"]
			w.Connection = "persistent-ssh"
			w.Address = "qualification-unreachable.invalid"
			w.Credential = "secretref:f02-protocol/normandy"
			cfg.BacklogV2.Workers["normandy"] = w
			cfg.BacklogV2.MessageLimits.MaxArtifactBytes = 16 << 20
			cfg.BacklogV2.Freshness.WorkerMaxAge = config.Duration(time.Minute)
			cfg.BacklogV2.Transport.RequestTimeout = config.Duration(time.Second)
			f := config.CoordinatorFleet{Kind: "steward-coordinator-catalog-input", SchemaVersion: 1, CoordinatorID: "normandy",
				Workers: map[string]config.CoordinatorFleetWorker{"normandy": {WorkerID: "normandy", CPUClass: "high", ExecutorSlots: 1, Capabilities: []string{"git", "huyang"},
					ProviderInstances: []string{"codex"}, DesiredModels: map[string][]string{"codex": {"test"}}, QuotaPools: []string{"codex-main"}}},
				Projects: map[string]config.CoordinatorFleetProject{"steward": {Repository: "https://example.invalid/steward.git", DefaultRef: "main", SetupProfile: "go", EligibleWorkers: []string{"normandy"}}}}
			switch kind {
			case "models":
				w := f.Workers["normandy"]
				w.DesiredModels["codex"] = []string{}
				f.Workers["normandy"] = w
			case "providers":
				w := f.Workers["normandy"]
				w.ProviderInstances = []string{}
				w.DesiredModels = map[string][]string{}
				w.QuotaPools = []string{}
				f.Workers["normandy"] = w
			case "projects":
				f.Projects = map[string]config.CoordinatorFleetProject{}
			case "workers":
				f.Workers = map[string]config.CoordinatorFleetWorker{}
				f.Projects = map[string]config.CoordinatorFleetProject{}
			}
			if err := cfg.ApplyCoordinatorFleet(f); err != nil {
				t.Fatal(err)
			}
			if err := cfg.Validate(); err != nil {
				t.Fatal(err)
			}
			for id, w := range cfg.BacklogV2.Workers {
				p, err := workerruntime.BuildCatalogProjection(cfg.BacklogV2, id)
				if err != nil {
					t.Fatal(err)
				}
				bootstrap := workerruntime.WorkerBootstrap{SchemaVersion: 1, WorkerID: id, CoordinatorID: "normandy", Transport: "ssh", CredentialRef: w.Credential, Capabilities: w.Capabilities, ProviderRoutes: f.Workers[id].ProviderInstances}
				settings, err := p.Settings(bootstrap, t.TempDir())
				if err != nil {
					t.Fatalf("worker refused revoked catalog: %v", err)
				}
				binding, err := workerruntime.BuildWorkerBinding(settings, id, time.Now())
				if err != nil {
					t.Fatal(err)
				}
				if kind == "models" || kind == "providers" {
					for _, provider := range binding.Inventory.Providers {
						if len(provider.Models) != 0 || provider.Available {
							t.Fatal("revoked provider advertised usable")
						}
					}
				}
				if kind == "projects" && len(binding.Inventory.Projects) != 0 {
					t.Fatal("revoked project advertised usable")
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			handled, err := runBacklogV2(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err != nil || !handled {
				t.Fatalf("idle coordinator startup handled=%t error=%v", handled, err)
			}
		})
	}
}
