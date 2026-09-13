package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
	"gopkg.in/yaml.v3"
)

func cloneReloadConfig(t *testing.T, c config.Config) config.Config {
	t.Helper()
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var result config.Config
	if err = json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	return result
}
func TestCoordinatorReloadSeparatesPolicyDrainAndLifecycle(t *testing.T) {
	base := qualificationConfig(t.TempDir())
	worker := base.BacklogV2.Workers["normandy"]
	worker.Connection = "persistent-ssh"
	base.BacklogV2.Workers["normandy"] = worker
	before, err := workerruntime.BuildWorkerBinding(base.BacklogV2, "normandy", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	drain := cloneReloadConfig(t, base)
	w := drain.BacklogV2.Workers["normandy"]
	w.AcceptBacklog = false
	drain.BacklogV2.Workers["normandy"] = w
	if err := validateCoordinatorReload(base, drain); err != nil {
		t.Fatal(err)
	}
	after, err := workerruntime.BuildWorkerBinding(drain.BacklogV2, "normandy", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if before.CatalogRevision != after.CatalogRevision || after.Inventory.AcceptBacklog {
		t.Fatal("drain replaced execution identity or left admission open")
	}
	policy := cloneReloadConfig(t, base)
	policy.BacklogV2.Leases.Duration += config.Duration(time.Second)
	if err := validateCoordinatorReload(base, policy); err != nil {
		t.Fatal(err)
	}
	changed, err := workerruntime.BuildWorkerBinding(policy.BacklogV2, "normandy", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if changed.CatalogRevision == before.CatalogRevision {
		t.Fatal("lease policy missing from catalog digest")
	}
	for _, kind := range []string{"worker-epoch", "storage", "coordinator", "host-policy"} {
		t.Run(kind, func(t *testing.T) {
			next := cloneReloadConfig(t, base)
			switch kind {
			case "worker-epoch":
				w := next.BacklogV2.Workers["normandy"]
				w.Epoch = "different"
				next.BacklogV2.Workers["normandy"] = w
			case "storage":
				next.BacklogV2.Storage.Artifacts += "-different"
			case "coordinator":
				next.BacklogV2.Coordinator.ID = "other"
			case "host-policy":
				next.Policy.DryRun = !next.Policy.DryRun
			}
			if err := validateCoordinatorReload(base, next); err == nil {
				t.Fatal("lifecycle change accepted")
			}
		})
	}
}
func TestCoordinatorReloadRejectsMissingAndMalformedFile(t *testing.T) {
	ctx := context.Background()
	cfg := qualificationConfig(t.TempDir())
	cfg.Path = filepath.Join(t.TempDir(), "config.yaml")
	s, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := loadCoordinatorReload(ctx, cfg, s); err == nil {
		t.Fatal("missing file became defaults")
	}
	if err := os.WriteFile(cfg.Path, []byte("unknown_authority: true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadCoordinatorReload(ctx, cfg, s); err == nil {
		t.Fatal("unknown configuration accepted")
	}
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.Path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err = config.LoadFile(cfg.Path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loadCoordinatorReload(ctx, cfg, s); err != nil {
		t.Fatal(err)
	}
}
