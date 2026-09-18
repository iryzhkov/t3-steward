package main

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
)

// quotaBindingProjection is a fleet projection that authorizes the instance
// the operator file binds (codex) and one it does not bind at all (opencode),
// with the projected binding present or absent.
func quotaBindingProjection(bound bool) config.CoordinatorFleet {
	worker := config.CoordinatorFleetWorker{
		WorkerID: "normandy", CPUClass: "low", ExecutorSlots: 1,
		Capabilities:      []string{"git", "huyang"},
		ProviderInstances: []string{"codex", "opencode"},
		DesiredModels:     map[string][]string{"codex": {"test"}, "opencode": {"glm-4.6"}},
		QuotaPools:        []string{"codex-main", "opencode-free"},
	}
	if bound {
		worker.QuotaBindings = map[string]string{"opencode": "opencode-free"}
	}
	return config.CoordinatorFleet{
		Kind: "steward-coordinator-catalog-input", SchemaVersion: 1, CoordinatorID: "normandy",
		Workers: map[string]config.CoordinatorFleetWorker{"normandy": worker},
		Projects: map[string]config.CoordinatorFleetProject{"steward": {
			Repository: "https://example.invalid/steward.git", DefaultRef: "main",
			SetupProfile: "go", EligibleWorkers: []string{"normandy"},
		}},
	}
}

// quotaBindingCoordinator is a coordinator whose own configuration binds codex
// and knows the opencode-free pool, loaded with the projection above.
func quotaBindingCoordinator(t *testing.T, bound bool) config.Config {
	t.Helper()
	cfg := config.Default()
	setCoordinatorTestRoots(t, &cfg)
	cfg.BacklogV2.Mode = "coordinator"
	cfg.BacklogV2.Coordinator.ID = "normandy"
	cfg.BacklogV2.StartupAdmission = "closed"
	cfg.BacklogV2.QuotaPools["opencode-free"] = config.V2QuotaPool{Provider: "opencode", MaxConcurrent: 1}
	if err := cfg.ApplyCoordinatorFleet(quotaBindingProjection(bound)); err != nil {
		t.Fatalf("the projection failed the whole configuration: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

// The coordinator starts with the unbound instance dropped and says so exactly
// once, naming the instance, the worker and the remedy. Before this change the
// same projection failed the configuration outright and the coordinator did
// not start at all.
func TestCoordinatorStartupWarnsOncePerDroppedProviderInstance(t *testing.T) {
	cfg := quotaBindingCoordinator(t, false)
	if _, ok := cfg.BacklogV2.Workers["normandy"].Providers["opencode"]; ok {
		t.Fatal("fixture: the unbound instance was authorized")
	}
	logs := &strings.Builder{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	handled, err := runBacklogV2(ctx, cfg, slog.New(slog.NewTextHandler(logs, nil)))
	if err != nil {
		t.Fatalf("an unbound provider instance stopped the coordinator from starting: %v", err)
	}
	if !handled {
		t.Fatal("the coordinator did not run")
	}
	warnings := 0
	for _, line := range strings.Split(logs.String(), "\n") {
		if !strings.Contains(line, "no authorized quota binding") {
			continue
		}
		warnings++
		for _, want := range []string{"level=WARN", "instance=opencode", "worker=normandy", "remedy="} {
			if !strings.Contains(line, want) {
				t.Fatalf("the warning does not carry %q: %q", want, line)
			}
		}
		if !strings.Contains(line, "upkeeper provider add") {
			t.Fatalf("the remedy does not name the one release edit that fixes it: %q", line)
		}
	}
	if warnings != 1 {
		t.Fatalf("expected one warning for the one dropped instance, got %d:\n%s", warnings, logs.String())
	}
	if strings.Contains(logs.String(), "instance=codex") {
		t.Fatal("the bound instance was reported as dropped")
	}
}

// A coordinator whose projection binds every instance warns about none.
func TestCoordinatorStartupIsSilentWhenEveryInstanceIsBound(t *testing.T) {
	cfg := quotaBindingCoordinator(t, true)
	if pool := cfg.BacklogV2.Workers["normandy"].Providers["opencode"].QuotaPool; pool != "opencode-free" {
		t.Fatalf("fixture: opencode pool = %q, want the projected binding", pool)
	}
	logs := &strings.Builder{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := runBacklogV2(ctx, cfg, slog.New(slog.NewTextHandler(logs, nil))); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "no authorized quota binding") {
		t.Fatalf("a fully bound coordinator warned about a binding:\n%s", logs.String())
	}
}

// Adding a binding to the projection is a catalog change, which SIGHUP
// applies. The list of dropped instances is derived state and must not reach
// the lifecycle comparison, or the first release that registers a provider is
// refused with "host lifecycle settings require restart".
func TestCoordinatorReloadAcceptsAProjectionThatAddsAQuotaBinding(t *testing.T) {
	current := quotaBindingCoordinator(t, false)
	next := quotaBindingCoordinator(t, true)
	next.StatePath, next.Backlog.Dir = current.StatePath, current.Backlog.Dir
	next.BacklogV2.Storage = current.BacklogV2.Storage
	if err := validateCoordinatorReload(current, next); err != nil {
		t.Fatalf("a projection that adds a quota binding was refused: %v", err)
	}
}
