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
	replacementToken := fencedCreationToken(input.Key, snapshotSequence)
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

// fakeProjectT3 keeps what T3 keeps about project creation: a receipt for
// every accepted command, the record of every project ever created (deleted
// ones included, whose IDs are refused for good), at most one active project
// per workspace root, and a projection that can lag the event sequence.
type fakeProjectT3 struct {
	t              *testing.T
	mu             sync.Mutex
	sequence       int64
	held           bool  // the projection is held at projected
	projected      int64 // projection position while held
	receipts       map[string]int64
	records        map[string]*fakeProjectRecord
	dispatched     []string          // command IDs in dispatch order
	effects        int               // creations accepted through dispatch
	refuse         map[string]string // command ID -> refusal the steward does not know
	lose           map[string]bool   // command ID -> first response lost after commit
	deleteOnCreate bool              // a sweep removes every new project at once
}

type fakeProjectRecord struct {
	shell            t3api.ProjectShell
	created, deleted int64
}

func newFakeProjectT3(t *testing.T) (*fakeProjectT3, *httptest.Server) {
	f := &fakeProjectT3{t: t, receipts: map[string]int64{}, records: map[string]*fakeProjectRecord{},
		refuse: map[string]string{}, lose: map[string]bool{}}
	server := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(server.Close)
	return f, server
}

func (f *fakeProjectT3) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Method == http.MethodGet && r.URL.Path == "/api/orchestration/shell" {
		position := f.sequence
		if f.held {
			position = f.projected
		}
		var projects []t3api.ProjectShell
		for _, record := range f.records {
			if record.created <= position && (record.deleted == 0 || record.deleted > position) {
				projects = append(projects, record.shell)
			}
		}
		_ = json.NewEncoder(w).Encode(t3api.ShellSnapshot{SnapshotSequence: position, Projects: projects})
		return
	}
	if r.Method != http.MethodPost || r.URL.Path != "/api/orchestration/dispatch" {
		http.Error(w, "unexpected request", http.StatusNotFound)
		return
	}
	var command map[string]any
	if err := json.NewDecoder(r.Body).Decode(&command); err != nil || command["type"] != "project.create" {
		f.t.Errorf("unexpected command %+v: %v", command, err)
		http.Error(w, "bad command", http.StatusBadRequest)
		return
	}
	commandID, _ := command["commandId"].(string)
	projectID, _ := command["projectId"].(string)
	root, _ := command["workspaceRoot"].(string)
	f.dispatched = append(f.dispatched, commandID)
	if receipt, ok := f.receipts[commandID]; ok {
		_ = json.NewEncoder(w).Encode(t3api.DispatchResult{Sequence: receipt})
		return
	}
	if refusal, ok := f.refuse[commandID]; ok {
		http.Error(w, refusal, http.StatusInternalServerError)
		return
	}
	if f.records[projectID] != nil {
		http.Error(w, "Project '"+projectID+"' already exists and cannot be created twice.", http.StatusConflict)
		return
	}
	for _, record := range f.records {
		if record.deleted == 0 && record.shell.WorkspaceRoot == root {
			http.Error(w, "workspace root "+root+" belongs to project "+record.shell.ID, http.StatusConflict)
			return
		}
	}
	f.sequence++
	f.records[projectID] = &fakeProjectRecord{created: f.sequence,
		shell: t3api.ProjectShell{ID: projectID, Title: command["title"].(string), WorkspaceRoot: root}}
	f.receipts[commandID] = f.sequence
	f.effects++
	receipt := f.sequence
	if f.deleteOnCreate {
		f.sequence++
		f.records[projectID].deleted = f.sequence
	}
	if f.lose[commandID] {
		http.Error(w, "response lost after commit", http.StatusBadGateway)
		return
	}
	_ = json.NewEncoder(w).Encode(t3api.DispatchResult{Sequence: receipt})
}

// seedLegacyHistory records generations accepted and then swept at one root
// under the receipt-chained identities an earlier steward derived, with
// unrelated events between them, as a long-lived worker root retains them.
func (f *fakeProjectT3) seedLegacyHistory(in ManagedProject, generations int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	token := in.Key
	for i := 0; i < generations; i++ {
		id := deterministicID(token, "steward.project")
		f.sequence += 3
		f.records[id] = &fakeProjectRecord{created: f.sequence,
			shell: t3api.ProjectShell{ID: id, Title: in.Title, WorkspaceRoot: in.WorkspaceRoot}}
		f.receipts[deterministicID(token, "steward.project.create")] = f.sequence
		token = in.Key + "\x00receipt\x00" + strconv.FormatInt(f.sequence, 10)
		f.sequence += 5
		f.records[id].deleted = f.sequence
	}
	f.sequence += 11
}

func (f *fakeProjectT3) active(root string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var ids []string
	for _, record := range f.records {
		if record.deleted == 0 && record.shell.WorkspaceRoot == root {
			ids = append(ids, record.shell.ID)
		}
	}
	return ids
}

func newProjectControl(server *httptest.Server) *Control {
	return New(t3api.New(server.URL, t3api.StaticToken("test"), time.Second),
		slog.New(slog.NewTextHandler(io.Discard, nil)), false)
}

func fencedProjectIdentity(key string, sequence int64) (projectID, commandID string) {
	token := fencedCreationToken(key, sequence)
	return deterministicID(token, "steward.project"), deterministicID(token, "steward.project.create")
}

// A root that has been created and swept any number of times is provisioned
// again with one replay of the key's own receipt and one fenced creation; the
// retained generations are skipped, not replayed. The earlier receipt chain
// replayed all of them and gave up after eight.
func TestEnsureProjectSkipsRetainedDeletedGenerations(t *testing.T) {
	for _, generations := range []int{8, 9, 64} {
		t.Run(strconv.Itoa(generations), func(t *testing.T) {
			input := ManagedProject{Key: "worker/project", Title: "Steward: swept", WorkspaceRoot: t.TempDir()}
			f, server := newFakeProjectT3(t)
			f.seedLegacyHistory(input, generations)
			fence := f.sequence
			wantID, wantCommand := fencedProjectIdentity(input.Key, fence)
			for attempt := 0; attempt < 2; attempt++ {
				got, err := newProjectControl(server).EnsureProject(context.Background(), input)
				if err != nil || got != wantID {
					t.Fatalf("ensure attempt %d = %q, %v; want %s", attempt, got, err, wantID)
				}
			}
			f.mu.Lock()
			dispatched, effects := append([]string(nil), f.dispatched...), f.effects
			f.mu.Unlock()
			keyCommand := deterministicID(input.Key, "steward.project.create")
			if len(dispatched) != 2 || dispatched[0] != keyCommand || dispatched[1] != wantCommand || effects != 1 {
				t.Fatalf("dispatched %v with %d effects; want the key's receipt, then one fenced creation", dispatched, effects)
			}
			if active := f.active(input.WorkspaceRoot); len(active) != 1 || active[0] != wantID {
				t.Fatalf("active at owned root = %v, want only %s", active, wantID)
			}
		})
	}
}

// Retained history changes nothing about a root that is already held: the
// project there is adopted when its title matches and refused when it does
// not, and neither dispatches anything.
func TestEnsureProjectWithRetainedHistoryResolvesAnActiveRootFirst(t *testing.T) {
	for _, title := range []string{"Steward: swept", "someone else's"} {
		t.Run(title, func(t *testing.T) {
			input := ManagedProject{Key: "worker/project", Title: "Steward: swept", WorkspaceRoot: t.TempDir()}
			f, server := newFakeProjectT3(t)
			f.seedLegacyHistory(input, 9)
			f.mu.Lock()
			f.sequence++
			f.records["holder"] = &fakeProjectRecord{created: f.sequence,
				shell: t3api.ProjectShell{ID: "holder", Title: title, WorkspaceRoot: input.WorkspaceRoot}}
			f.mu.Unlock()
			got, err := newProjectControl(server).EnsureProject(context.Background(), input)
			if title == input.Title && (err != nil || got != "holder") {
				t.Fatalf("ensure = %q, %v; want the active project at the owned root", got, err)
			}
			if title != input.Title && (err == nil || !strings.Contains(err.Error(), "rather than")) {
				t.Fatalf("foreign title at the owned root was not refused: %q, %v", got, err)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if len(f.dispatched) != 0 {
				t.Fatalf("dispatched %v at a held root", f.dispatched)
			}
		})
	}
}

// A fenced creation whose response is lost and whose project is not yet
// projected is the same command for every caller that observes the same
// snapshot: the call that lost it, a restart, and concurrent retries all end at
// one project. A caller that observes a newer but still lagging snapshot fences
// a different command, which T3 refuses because the root is already held, and
// it adopts the held project once it is projected.
func TestEnsureProjectFencedCreationSurvivesAmbiguityAndDelay(t *testing.T) {
	for _, scenario := range []string{"same snapshot", "projection advanced"} {
		t.Run(scenario, func(t *testing.T) {
			input := ManagedProject{Key: "worker/project", Title: "Steward: swept", WorkspaceRoot: t.TempDir()}
			f, server := newFakeProjectT3(t)
			f.seedLegacyHistory(input, 9)
			f.mu.Lock()
			fence := f.sequence
			f.held, f.projected = true, fence
			f.sequence += 4 // unrelated events the projection has not reached
			wantID, wantCommand := fencedProjectIdentity(input.Key, fence)
			f.lose[wantCommand] = true
			f.mu.Unlock()

			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			if _, err := newProjectControl(server).EnsureProject(ctx, input); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("unprojected creation should hit the deadline, got %v", err)
			}
			if scenario == "projection advanced" {
				f.mu.Lock()
				f.projected = fence + 2 // still behind the lost creation
				f.mu.Unlock()
			}

			var wg sync.WaitGroup
			results := make([]string, 3)
			failures := make([]error, 3)
			for i := range results {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					results[i], failures[i] = newProjectControl(server).EnsureProject(context.Background(), input)
				}(i)
			}
			time.Sleep(300 * time.Millisecond)
			f.mu.Lock()
			f.held = false
			f.mu.Unlock()
			wg.Wait()
			for i := range results {
				if failures[i] != nil || results[i] != wantID {
					t.Fatalf("retry %d = %q, %v; want %s", i, results[i], failures[i], wantID)
				}
			}
			f.mu.Lock()
			effects := f.effects
			f.mu.Unlock()
			if active := f.active(input.WorkspaceRoot); effects != 1 || len(active) != 1 || active[0] != wantID {
				t.Fatalf("effects=%d active=%v; want one project %s", effects, active, wantID)
			}
		})
	}
}

// A refusal the steward does not recognise is returned, never retried under a
// further identity, even when retained history made the refused creation a
// fenced one.
func TestEnsureProjectUnknownRefusalIsNotRetried(t *testing.T) {
	input := ManagedProject{Key: "worker/project", Title: "Steward: swept", WorkspaceRoot: t.TempDir()}
	f, server := newFakeProjectT3(t)
	f.seedLegacyHistory(input, 9)
	_, fencedCommand := fencedProjectIdentity(input.Key, f.sequence)
	f.refuse[fencedCommand] = "invariant nobody has seen before"
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := newProjectControl(server).EnsureProject(ctx, input)
	if err == nil || !strings.Contains(err.Error(), "invariant nobody has seen before") {
		t.Fatalf("unknown refusal not returned: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.dispatched) != 2 || f.dispatched[1] != fencedCommand || f.effects != 0 {
		t.Fatalf("dispatched %v with %d effects; want the refused fenced creation and nothing after it", f.dispatched, f.effects)
	}
}

// Creation is bounded however T3 behaves: a root swept as fast as it is
// created exhausts the dispatch budget, and a fence no newer snapshot can
// replace stops at once rather than dispatching it again.
func TestEnsureProjectCreationBudgetIsBounded(t *testing.T) {
	t.Run("swept on creation", func(t *testing.T) {
		input := ManagedProject{Key: "worker/project", Title: "Steward: swept", WorkspaceRoot: t.TempDir()}
		f, server := newFakeProjectT3(t)
		f.seedLegacyHistory(input, 9)
		f.deleteOnCreate = true
		started := time.Now()
		_, err := newProjectControl(server).EnsureProject(context.Background(), input)
		if err == nil || !strings.Contains(err.Error(), "exhausted 4 creation dispatches") {
			t.Fatalf("budget not enforced: %v", err)
		}
		if elapsed := time.Since(started); elapsed > 5*time.Second {
			t.Fatalf("exhausting the budget took %s", elapsed)
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if len(f.dispatched) != 4 || f.effects != 3 {
			t.Fatalf("dispatched %d commands with %d effects; want 4 and 3", len(f.dispatched), f.effects)
		}
	})
	t.Run("fence cannot advance", func(t *testing.T) {
		input := ManagedProject{Key: "worker/project", Title: "Steward: swept", WorkspaceRoot: t.TempDir()}
		f, server := newFakeProjectT3(t)
		f.seedLegacyHistory(input, 9)
		fencedID, _ := fencedProjectIdentity(input.Key, f.sequence)
		// The fenced ID is already on record under some other command.
		f.records[fencedID] = &fakeProjectRecord{created: 1, deleted: 2,
			shell: t3api.ProjectShell{ID: fencedID, Title: input.Title, WorkspaceRoot: input.WorkspaceRoot}}
		_, err := newProjectControl(server).EnsureProject(context.Background(), input)
		if err == nil || !strings.Contains(err.Error(), "no root-absent snapshot after sequence") {
			t.Fatalf("stalled fence not refused: %v", err)
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if len(f.dispatched) != 2 || f.effects != 0 {
			t.Fatalf("dispatched %v with %d effects; want two refused commands", f.dispatched, f.effects)
		}
	})
}
