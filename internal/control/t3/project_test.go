package t3

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
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

func TestEnsureProjectRecreatesDeletedProjectFromOldAcceptedReceipt(t *testing.T) {
	input := ManagedProject{Key: "worker/project", Title: "Steward: swept", WorkspaceRoot: t.TempDir()}
	oldID := deterministicID(input.Key, "steward.project")
	oldCommand := deterministicID(input.Key, "steward.project.create")
	const oldSequence int64 = 7
	const snapshotSequence int64 = 42
	replacementToken := input.Key + "\x00receipt\x00" + strconv.FormatInt(oldSequence, 10)
	replacementID := deterministicID(replacementToken, "steward.project")
	replacementCommand := deterministicID(replacementToken, "steward.project.create")
	var mu sync.Mutex
	var active []t3api.ProjectShell
	dispatches := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodGet && r.URL.Path == "/api/orchestration/shell" {
			_ = json.NewEncoder(w).Encode(t3api.ShellSnapshot{SnapshotSequence: snapshotSequence, Projects: active})
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/api/orchestration/dispatch" {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		var command map[string]any
		if err := json.NewDecoder(r.Body).Decode(&command); err != nil {
			t.Error(err)
			http.Error(w, "bad command", http.StatusBadRequest)
			return
		}
		commandID, _ := command["commandId"].(string)
		dispatches = append(dispatches, commandID)
		switch commandID {
		case oldCommand:
			if command["projectId"] != oldID {
				t.Errorf("old command used project %v, want %s", command["projectId"], oldID)
			}
			_ = json.NewEncoder(w).Encode(t3api.DispatchResult{Sequence: oldSequence})
		case replacementCommand:
			if command["projectId"] != replacementID {
				t.Errorf("replacement command used project %v, want %s", command["projectId"], replacementID)
			}
			active = []t3api.ProjectShell{{ID: replacementID, Title: input.Title, WorkspaceRoot: input.WorkspaceRoot}}
			_ = json.NewEncoder(w).Encode(t3api.DispatchResult{Sequence: snapshotSequence + 1})
		default:
			t.Errorf("unexpected command ID %s", commandID)
			http.Error(w, "unexpected command", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	for i := 0; i < 2; i++ {
		control := New(t3api.New(server.URL, t3api.StaticToken("test"), time.Second),
			slog.New(slog.NewTextHandler(io.Discard, nil)), false)
		got, err := control.EnsureProject(context.Background(), input)
		if err != nil || got != replacementID {
			t.Fatalf("ensure attempt %d = %q, %v; want %s", i, got, err, replacementID)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(dispatches) != 2 || dispatches[0] != oldCommand || dispatches[1] != replacementCommand {
		t.Fatalf("dispatches = %v; want one old receipt and one stable replacement", dispatches)
	}
}

func TestEnsureProjectDelayedVisibilityStartsOneThread(t *testing.T) {
	input := ManagedProject{Key: "task-thread", Title: "Steward: git-task", WorkspaceRoot: t.TempDir()}
	const threadID = "task-thread"
	const dispatchToken = "task-dispatch"
	var mu sync.Mutex
	var project *t3api.ProjectShell
	creates := 0
	threadCreates := 0
	turnStarts := 0
	postCreateSnapshots := 0
	seen := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodGet && r.URL.Path == "/api/orchestration/shell" {
			var projects []t3api.ProjectShell
			if project != nil {
				postCreateSnapshots++
				if postCreateSnapshots > 1 {
					projects = append(projects, *project)
				}
			}
			_ = json.NewEncoder(w).Encode(t3api.ShellSnapshot{Projects: projects})
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/api/orchestration/dispatch" {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		var command map[string]any
		if err := json.NewDecoder(r.Body).Decode(&command); err != nil {
			t.Error(err)
			http.Error(w, "bad command", http.StatusBadRequest)
			return
		}
		commandID, _ := command["commandId"].(string)
		if seen[commandID] {
			_ = json.NewEncoder(w).Encode(map[string]int{"sequence": 1})
			return
		}
		seen[commandID] = true
		switch command["type"] {
		case "project.create":
			creates++
			if commandID != deterministicID(input.Key, "steward.project.create") {
				t.Errorf("unexpected project command ID %q", commandID)
			}
			project = &t3api.ProjectShell{ID: command["projectId"].(string), Title: command["title"].(string), WorkspaceRoot: command["workspaceRoot"].(string)}
		case "thread.create":
			threadCreates++
			if project == nil || postCreateSnapshots <= 1 || command["projectId"] != project.ID {
				t.Errorf("thread created before verified project: %+v", command)
			}
		case "thread.turn.start":
			turnStarts++
		default:
			t.Errorf("unexpected command: %+v", command)
		}
		_ = json.NewEncoder(w).Encode(map[string]int{"sequence": 1})
	}))
	defer server.Close()

	for i := 0; i < 2; i++ {
		// Reconstruct the adapter, as retry/restart does. Stable command IDs
		// make the repeated thread request one T3 effect.
		control := New(t3api.New(server.URL, t3api.StaticToken("test"), time.Second),
			slog.New(slog.NewTextHandler(io.Discard, nil)), false)
		projectID, err := control.EnsureProject(context.Background(), input)
		if err != nil {
			t.Fatalf("ensure on attempt %d: %v", i, err)
		}
		gotThread, err := control.CreateAndStartThread(context.Background(), NewThreadInput{
			ThreadID: threadID, DispatchToken: dispatchToken, ProjectID: projectID,
			Title: "seed", WorktreePath: input.WorkspaceRoot, Prompt: "go",
		})
		if err != nil || gotThread != threadID {
			t.Fatalf("start on attempt %d: thread %q, error %v", i, gotThread, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if creates != 1 || threadCreates != 1 || turnStarts != 1 {
		t.Fatalf("effects: projects=%d threads=%d turns=%d", creates, threadCreates, turnStarts)
	}
}

func TestEnsureProjectRestartBeforeVisibilityReplaysOneCommand(t *testing.T) {
	input := ManagedProject{Key: "task-thread", Title: "Steward: git-task", WorkspaceRoot: t.TempDir()}
	var mu sync.Mutex
	var project *t3api.ProjectShell
	visible := false
	dispatches := 0
	creates := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodGet {
			var projects []t3api.ProjectShell
			if visible {
				projects = append(projects, *project)
			}
			_ = json.NewEncoder(w).Encode(t3api.ShellSnapshot{Projects: projects})
			return
		}
		var command map[string]any
		if err := json.NewDecoder(r.Body).Decode(&command); err != nil {
			t.Error(err)
			http.Error(w, "bad command", http.StatusBadRequest)
			return
		}
		if command["type"] != "project.create" ||
			command["commandId"] != deterministicID(input.Key, "steward.project.create") {
			t.Errorf("unexpected command: %+v", command)
		}
		dispatches++
		if project == nil {
			creates++
			project = &t3api.ProjectShell{ID: command["projectId"].(string), Title: command["title"].(string), WorkspaceRoot: command["workspaceRoot"].(string)}
		} else {
			// The replayed command is idempotent and its project becomes
			// visible only after this second dispatch.
			visible = true
		}
		_ = json.NewEncoder(w).Encode(map[string]int{"sequence": 1})
	}))
	defer server.Close()

	newControl := func() *Control {
		return New(t3api.New(server.URL, t3api.StaticToken("test"), time.Second),
			slog.New(slog.NewTextHandler(io.Discard, nil)), false)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if _, err := newControl().EnsureProject(ctx, input); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("hidden project should hit deadline, got %v", err)
	}
	got, err := newControl().EnsureProject(context.Background(), input)
	if err != nil || got != deterministicID(input.Key, "steward.project") {
		t.Fatalf("restart ensure = %q, %v", got, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if dispatches != 2 || creates != 1 {
		t.Fatalf("project dispatches=%d effects=%d, want replay of one effect", dispatches, creates)
	}
}

func TestEnsureProjectDelayedVisibilityFailsClosed(t *testing.T) {
	for _, scenario := range []string{"wrong title", "deadline", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			input := ManagedProject{Key: "task-thread", Title: "Steward: git-task", WorkspaceRoot: t.TempDir()}
			creates := 0
			observations := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					creates++
					_ = json.NewEncoder(w).Encode(map[string]int{"sequence": 1})
					return
				}
				observations++
				var projects []t3api.ProjectShell
				if scenario == "wrong title" && observations > 2 {
					projects = []t3api.ProjectShell{{ID: "conflict", Title: "another", WorkspaceRoot: input.WorkspaceRoot}}
				}
				_ = json.NewEncoder(w).Encode(t3api.ShellSnapshot{Projects: projects})
			}))
			defer server.Close()
			control := New(t3api.New(server.URL, t3api.StaticToken("test"), time.Second),
				slog.New(slog.NewTextHandler(io.Discard, nil)), false)
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			if scenario == "cancelled" {
				cancel()
			} else {
				defer cancel()
			}
			_, err := control.EnsureProject(ctx, input)
			if err == nil {
				t.Fatal("unverified project was accepted")
			}
			if scenario == "wrong title" && !strings.Contains(err.Error(), "rather than") {
				t.Fatalf("title conflict was not reported: %v", err)
			}
			if scenario == "deadline" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("deadline not preserved: %v", err)
			}
			if scenario == "cancelled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation not preserved: %v", err)
			}
			wantCreates := 1
			if scenario == "cancelled" {
				wantCreates = 0
			}
			if creates != wantCreates {
				t.Fatalf("create commands = %d, want %d", creates, wantCreates)
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
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			_, err := control.EnsureProject(ctx, input)
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
