package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// malformedProjectCoordinator is a coordinator whose worker has a persistent
// connection, so its binding is built during startup, with one healthy project
// and one whose repository the catalog cannot accept.
func malformedProjectCoordinator(t *testing.T) config.Config {
	t.Helper()
	cfg := config.Default()
	setCoordinatorTestRoots(t, &cfg)
	cfg.BacklogV2.Mode = "coordinator"
	cfg.BacklogV2.Coordinator.ID = "normandy"
	cfg.BacklogV2.StartupAdmission = "closed"
	cfg.BacklogV2.SetupProfiles = map[string]config.V2SetupProfile{
		"go": {Commands: []string{"go build ./..."}, Timeout: config.Duration(time.Minute)},
	}
	worker := cfg.BacklogV2.Workers["normandy"]
	worker.Connection = "persistent-ssh"
	worker.Credential = "secretref:f02-protocol/normandy"
	cfg.BacklogV2.Workers["normandy"] = worker

	healthy := cfg.BacklogV2.Projects["steward"]
	healthy.SetupProfile = "go"
	cfg.BacklogV2.Projects["steward"] = healthy
	// "argument-injection" is the name the qualification harness used.
	cfg.BacklogV2.Projects["argument-injection"] = config.V2Project{
		Repository: "--upload-pack=touch /tmp/pwned", DefaultRef: "main",
		SetupProfile: "go", T3Project: "argument-injection development",
		Workers: []string{"normandy"},
	}
	return cfg
}

// TestCoordinatorStartsWithOneMalformedProject is the startup half of the
// isolation property.
//
// A worker with a persistent connection has its binding built during startup,
// so one project whose repository syntax is invalid used to stop the
// coordinator from starting at all. Every project and every worker went with
// it, and the operator saw a coordinator that would not come up rather than a
// message naming the project. The qualification harness had to serve two of its
// cases from a coordinator whose workers had no persistent connection.
//
// Existing coordinator tests do not reach this path: their worker declares no
// connection, so no binding is built at startup.
func TestCoordinatorStartsWithOneMalformedProject(t *testing.T) {
	cfg := malformedProjectCoordinator(t)
	path := cfg.StatePath

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	handled, err := runBacklogV2(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("one malformed project stopped the coordinator from starting: %v", err)
	}
	if !handled {
		t.Fatal("the coordinator did not run")
	}

	store, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// The worker is still enrolled as a requirement, which is what scheduling
	// for every other project depends on.
	requirements, err := store.LoadWorkerRequirements(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(requirements) != 1 || requirements[0].WorkerID != "normandy" {
		t.Fatalf("worker requirements = %+v, want the worker to survive", requirements)
	}
	if requirements[0].CatalogRevision == "" {
		t.Fatal("the worker has no catalog revision, so it cannot be enrolled")
	}
}

// TestCoordinatorStartupNamesTheMalformedProject states that an isolated
// failure is announced. Nothing else breaks to make an operator look, so the
// coordinator has to say which project is broken and why.
func TestCoordinatorStartupNamesTheMalformedProject(t *testing.T) {
	cfg := malformedProjectCoordinator(t)
	logs := &strings.Builder{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := runBacklogV2(ctx, cfg, slog.New(slog.NewTextHandler(logs, nil))); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"argument-injection", "repository"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("startup logs do not name %q", want)
		}
	}
}
