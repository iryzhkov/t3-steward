package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
)

// defaultedProjectCoordinator is a coordinator whose fleet projection names
// one project that backlog_v2.projects binds (steward) and one it does not
// (home-assistant-config), applied the way the file loader applies it.
func defaultedProjectCoordinator(t *testing.T) config.Config {
	t.Helper()
	cfg := config.Default()
	setCoordinatorTestRoots(t, &cfg)
	cfg.BacklogV2.Mode = "coordinator"
	cfg.BacklogV2.Coordinator.ID = "normandy"
	cfg.BacklogV2.StartupAdmission = "closed"
	fleet := config.CoordinatorFleet{
		Kind: "steward-coordinator-catalog-input", SchemaVersion: 1, CoordinatorID: "normandy",
		Workers: map[string]config.CoordinatorFleetWorker{"normandy": {
			WorkerID: "normandy", CPUClass: "low", ExecutorSlots: 1,
			Capabilities: []string{"git", "huyang"}, ProviderInstances: []string{"codex"},
			DesiredModels: map[string][]string{"codex": {"test"}}, QuotaPools: []string{"codex-main"},
		}},
		Projects: map[string]config.CoordinatorFleetProject{
			"steward":               {Repository: "https://example.invalid/steward.git", DefaultRef: "main", SetupProfile: "go", EligibleWorkers: []string{"normandy"}},
			"home-assistant-config": {Repository: "https://example.invalid/home-assistant-config.git", DefaultRef: "main", SetupProfile: "go", EligibleWorkers: []string{"normandy"}},
		},
	}
	if err := cfg.ApplyCoordinatorFleet(fleet); err != nil {
		t.Fatalf("projection with an unbound project refused: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

// TestCoordinatorStartupWarnsOncePerDefaultedProject: the coordinator starts
// with the unbound project and says so exactly once, naming it, so an operator
// reading the first lines learns which project runs with default bindings.
func TestCoordinatorStartupWarnsOncePerDefaultedProject(t *testing.T) {
	cfg := defaultedProjectCoordinator(t)
	logs := &strings.Builder{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	handled, err := runBacklogV2(ctx, cfg, slog.New(slog.NewTextHandler(logs, nil)))
	if err != nil {
		t.Fatalf("an unbound project stopped the coordinator from starting: %v", err)
	}
	if !handled {
		t.Fatal("the coordinator did not run")
	}
	warnings := 0
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "loaded with default local bindings") {
			warnings++
			if !strings.Contains(line, "level=WARN") || !strings.Contains(line, "project=home-assistant-config") {
				t.Fatalf("warning line = %q", line)
			}
		}
	}
	if warnings != 1 {
		t.Fatalf("expected exactly one warning for the one defaulted project, got %d:\n%s", warnings, logs.String())
	}
	if strings.Contains(logs.String(), "project=steward") {
		t.Fatal("the explicitly bound project was reported as defaulted")
	}
}

// A coordinator with every project bound logs no such warning.
func TestCoordinatorStartupIsSilentWhenEveryProjectIsBound(t *testing.T) {
	cfg := malformedProjectCoordinator(t)
	logs := &strings.Builder{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := runBacklogV2(ctx, cfg, slog.New(slog.NewTextHandler(logs, nil))); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "default local bindings") {
		t.Fatalf("a fully bound coordinator warned about default bindings:\n%s", logs.String())
	}
	_ = io.Discard
}
