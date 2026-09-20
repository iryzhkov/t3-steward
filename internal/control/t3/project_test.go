package t3

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/t3api"
)

func TestEnsureProjectReconcilesCreation(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "acknowledged", true: "lost response"}[lost], func(t *testing.T) {
			input := ManagedProject{Key: "worker/project", Title: "Steward: repo-free", WorkspaceRoot: t.TempDir()}
			var mu sync.Mutex
			projects := []t3api.ProjectShell{{ID: "unmanaged", Title: input.Title, WorkspaceRoot: "/unrelated"}}
			creates := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if r.Method == http.MethodGet && r.URL.Path == "/api/orchestration/shell" {
					_ = json.NewEncoder(w).Encode(t3api.ShellSnapshot{Projects: projects})
					return
				}
				if r.Method != http.MethodPost || r.URL.Path != "/api/orchestration/dispatch" {
					http.Error(w, "unexpected request", 404)
					return
				}
				var command map[string]any
				if err := json.NewDecoder(r.Body).Decode(&command); err != nil {
					t.Error(err)
					http.Error(w, "bad command", 400)
					return
				}
				if command["type"] != "project.create" ||
					command["commandId"] != deterministicID(input.Key, "steward.project.create") ||
					command["createWorkspaceRootIfMissing"] != true {
					t.Errorf("unexpected creation command: %+v", command)
				}
				creates++
				projects = append(projects, t3api.ProjectShell{
					ID: command["projectId"].(string), Title: command["title"].(string),
					WorkspaceRoot: command["workspaceRoot"].(string),
				})
				if lost {
					http.Error(w, "response lost after commit", http.StatusBadGateway)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]int{"sequence": 1})
			}))
			defer server.Close()
			for i := 0; i < 2; i++ {
				// A new adapter has no process-local memory of the first request.
				control := New(t3api.New(server.URL, t3api.StaticToken("test"), time.Second),
					slog.New(slog.NewTextHandler(io.Discard, nil)), false)
				id, err := control.EnsureProject(context.Background(), input)
				if err != nil || id != deterministicID(input.Key, "steward.project") {
					t.Fatalf("ensure = %q, %v", id, err)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if creates != 1 || len(projects) != 2 {
				t.Fatalf("created %d commands, %d projects", creates, len(projects))
			}
		})
	}
}

// A managed project that was deleted is provisioned again for the same key.
//
// T3 keeps a deleted project's record and refuses to create the same ID twice,
// so an identity derived from the key alone is spent by the first deletion and
// every later dispatch for that key would fail. The owned workspace root is
// what this call looks for, and a spent identity is replaced rather than
// retried, so cleaning a project up is not the same thing as breaking it.
func TestEnsureProjectRecreatesADeletedProject(t *testing.T) {
	input := ManagedProject{Key: "worker/project", Title: "Steward: swept", WorkspaceRoot: t.TempDir()}
	var mu sync.Mutex
	var active []t3api.ProjectShell
	spent := map[string]bool{}
	created := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(t3api.ShellSnapshot{Projects: active})
			return
		}
		var command map[string]any
		if err := json.NewDecoder(r.Body).Decode(&command); err != nil {
			t.Error(err)
			http.Error(w, "bad command", 400)
			return
		}
		id, _ := command["projectId"].(string)
		// T3's own invariant: the record of a deleted project stays, and its
		// identity is refused for good.
		if spent[id] {
			http.Error(w, "Project '"+id+"' already exists and cannot be created twice.", http.StatusConflict)
			return
		}
		spent[id] = true
		created = append(created, id)
		active = append(active, t3api.ProjectShell{ID: id, Title: command["title"].(string), WorkspaceRoot: command["workspaceRoot"].(string)})
		_ = json.NewEncoder(w).Encode(map[string]int{"sequence": 1})
	}))
	defer server.Close()
	control := New(t3api.New(server.URL, t3api.StaticToken("test"), time.Second),
		slog.New(slog.NewTextHandler(io.Discard, nil)), false)
	ctx := context.Background()
	first, err := control.EnsureProject(ctx, input)
	if err != nil || first != deterministicID(input.Key, "steward.project") {
		t.Fatalf("first ensure = %q, %v", first, err)
	}
	// The sweep: the project stops being active and its identity stays spent.
	mu.Lock()
	active = nil
	mu.Unlock()
	second, err := control.EnsureProject(ctx, input)
	if err != nil {
		t.Fatalf("ensure after the sweep: %v", err)
	}
	if second == first || second == "" {
		t.Fatalf("ensure after the sweep returned %q, want a fresh identity (first was %q)", second, first)
	}
	third, err := control.EnsureProject(ctx, input)
	if err != nil || third != second {
		t.Fatalf("third ensure = %q, %v; want the project already at the owned root (%q)", third, err, second)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(created) != 2 {
		t.Fatalf("created %v, want exactly the spent identity and its replacement", created)
	}
}

// A project this call must not adopt, a snapshot it cannot read and a creation
// it cannot observe are all refusals rather than guesses.
//
// The owned workspace root is the identity, so a project carrying the derived
// ID at some other root is not this project and is not a closed failure; it is
// covered by TestEnsureProjectRecreatesADeletedProject, where T3 refuses the
// spent ID and the root is created under a fresh one.
func TestEnsureProjectFailsClosed(t *testing.T) {
	for _, scenario := range []string{"wrong title", "snapshot unavailable", "not projected", "dry run"} {
		t.Run(scenario, func(t *testing.T) {
			input := ManagedProject{Key: "stable", Title: "owned", WorkspaceRoot: t.TempDir()}
			project := t3api.ProjectShell{ID: deterministicID(input.Key, "steward.project"), Title: input.Title, WorkspaceRoot: input.WorkspaceRoot}
			if scenario == "wrong title" {
				project.Title = "another"
			}
			posts := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					posts++
					_ = json.NewEncoder(w).Encode(map[string]int{"sequence": 1})
					return
				}
				if scenario == "snapshot unavailable" {
					http.Error(w, "unavailable", http.StatusServiceUnavailable)
					return
				}
				projects := []t3api.ProjectShell{project}
				if scenario == "not projected" || scenario == "dry run" {
					projects = nil
				}
				_ = json.NewEncoder(w).Encode(t3api.ShellSnapshot{Projects: projects})
			}))
			defer server.Close()
			control := New(t3api.New(server.URL, t3api.StaticToken("test"), time.Second),
				slog.New(slog.NewTextHandler(io.Discard, nil)), scenario == "dry run")
			_, err := control.EnsureProject(context.Background(), input)
			if (err == nil) != (scenario == "dry run") {
				t.Fatalf("unexpected error: %v", err)
			}
			wantPosts := 0
			if scenario == "not projected" {
				wantPosts = 1
			}
			if posts != wantPosts {
				t.Fatalf("posts = %d, want %d", posts, wantPosts)
			}
		})
	}
}
