package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/t3projects"
)

// The watchdog removes the empty T3 projects its own worker created on this
// host, and nothing else in the sidebar.
//
// This is the wiring rather than the rule: that the daemon has a pass at all,
// that the pass knows which directories this host provisions projects under,
// and that what it sends T3 is one project.delete with no force.
func TestWatchdogSweepsItsOwnEmptyProjects(t *testing.T) {
	workspaces := filepath.Join(t.TempDir(), "worker", "workspaces")
	managedRoot := filepath.Join(workspaces, "workers", "abc", ".projects", "hash")
	var mu sync.Mutex
	var commands []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/orchestration/shell":
			_ = json.NewEncoder(w).Encode(map[string]any{"projects": []map[string]any{
				{"id": "managed", "title": "Steward: fleet-ops", "workspaceRoot": managedRoot,
					"createdAt": "2026-09-01T00:00:00.000Z", "updatedAt": "2026-09-01T00:00:00.000Z"},
				{"id": "human", "title": "huyang development", "workspaceRoot": "/home/igor/Work/huyang",
					"createdAt": "2026-09-01T00:00:00.000Z", "updatedAt": "2026-09-01T00:00:00.000Z"},
				{"id": "occupied", "title": "Steward: busy", "workspaceRoot": filepath.Join(workspaces, "workers", "abc", ".projects", "busy"),
					"createdAt": "2026-09-01T00:00:00.000Z", "updatedAt": "2026-09-01T00:00:00.000Z"},
			}, "threads": []any{}})
		case "/api/orchestration/snapshot":
			// The full read model, where an archived thread is still a thread.
			// The shell above shows none of them, which is the defect this route
			// exists in the pass for.
			_ = json.NewEncoder(w).Encode(map[string]any{"threads": []map[string]any{
				{"id": "thread-1", "projectId": "occupied", "archivedAt": "2026-09-02T00:00:00.000Z",
					"updatedAt": "2026-09-02T00:00:00.000Z"},
			}})
		case "/api/orchestration/dispatch":
			var command map[string]any
			if err := json.NewDecoder(r.Body).Decode(&command); err != nil {
				t.Error(err)
				http.Error(w, "bad command", http.StatusBadRequest)
				return
			}
			mu.Lock()
			commands = append(commands, command)
			mu.Unlock()
			_, _ = io.WriteString(w, `{"sequence":1}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cfg := config.Default()
	cfg.T3.URL, cfg.T3.Token, cfg.T3.DataDir = server.URL, "test-token", t.TempDir()
	cfg.Archive.Enabled = false
	// A live watchdog, as a deployed one is; the dry-run default is covered
	// below and in the package's own tests.
	cfg.Policy.DryRun = false
	cfg.BacklogV2.Storage.Workspaces = workspaces
	home := t.TempDir()
	projectCleanupHome = func() (string, error) { return home, nil }
	t.Cleanup(func() { projectCleanupHome = os.UserHomeDir })
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_, daemon, err := buildWatchdog(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), store, false)
	if err != nil {
		t.Fatal(err)
	}
	sweeper, ok := daemon.Projects.(*t3projects.Sweeper)
	if !ok {
		t.Fatalf("the daemon has no project cleanup pass: %T", daemon.Projects)
	}
	// Both sources of owned roots reach the pass: the configured workspaces root
	// and the one a worker running from the private bootstrap derives.
	if !t3projects.Owned(managedRoot, sweeper.Options.OwnedRoots) ||
		!t3projects.Owned(filepath.Join(config.WorkerWorkspacesRoot(home), "workers", "x", "y"), sweeper.Options.OwnedRoots) {
		t.Fatalf("owned roots = %v", sweeper.Options.OwnedRoots)
	}
	ctx := context.Background()
	sweeper.Tick(ctx, nil, nil)
	mu.Lock()
	defer mu.Unlock()
	if len(commands) != 1 {
		t.Fatalf("dispatched %+v, want one deletion", commands)
	}
	if commands[0]["type"] != "project.delete" || commands[0]["projectId"] != "managed" {
		t.Fatalf("dispatched %+v", commands[0])
	}
	if _, forced := commands[0]["force"]; forced {
		t.Fatalf("the deletion was forced: %+v", commands[0])
	}
	actions, err := store.RecentActions(ctx, 10)
	if err != nil || len(actions) != 1 || actions[0].Kind != t3projects.ActionSweep {
		t.Fatalf("audit = %+v, err=%v", actions, err)
	}
}

// A watchdog that enforces nothing deletes nothing either: the global dry run
// reaches this pass, and so does its own switch.
func TestProjectCleanupFollowsBothDryRunSwitches(t *testing.T) {
	for _, tc := range []struct {
		name            string
		policy, cleanup bool
		want            bool
	}{
		{"live", false, false, false},
		{"watchdog dry run", true, false, true},
		{"cleanup dry run", false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Default()
			cfg.T3.URL, cfg.T3.Token, cfg.T3.DataDir = "http://127.0.0.1:1", "test", t.TempDir()
			cfg.Archive.Enabled = false
			cfg.Policy.DryRun, cfg.ProjectCleanup.DryRun = tc.policy, tc.cleanup
			cfg.BacklogV2.Storage.Workspaces = filepath.Join(t.TempDir(), "workspaces")
			store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			_, daemon, err := buildWatchdog(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), store, false)
			if err != nil {
				t.Fatal(err)
			}
			sweeper, ok := daemon.Projects.(*t3projects.Sweeper)
			if !ok {
				t.Fatalf("the daemon has no project cleanup pass: %T", daemon.Projects)
			}
			if sweeper.Options.DryRun != tc.want {
				t.Fatalf("dry run = %t, want %t", sweeper.Options.DryRun, tc.want)
			}
		})
	}
}

// The pass is one setting away, and a host that turns it off has none.
func TestWatchdogWithoutProjectCleanup(t *testing.T) {
	cfg := config.Default()
	cfg.T3.URL, cfg.T3.Token, cfg.T3.DataDir = "http://127.0.0.1:1", "test", t.TempDir()
	cfg.Archive.Enabled = false
	cfg.ProjectCleanup.Enabled = false
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_, daemon, err := buildWatchdog(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), store, false)
	if err != nil {
		t.Fatal(err)
	}
	if daemon.Projects != nil {
		t.Fatalf("project cleanup is off and the daemon still has a pass: %T", daemon.Projects)
	}
}
