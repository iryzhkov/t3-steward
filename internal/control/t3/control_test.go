package t3

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/t3api"
)

type dispatchRecorder struct {
	mu       sync.Mutex
	commands []map[string]any
	status   int
	body     string
}

func (r *dispatchRecorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost || req.URL.Path != "/api/orchestration/dispatch" {
		http.Error(w, "unexpected request", http.StatusNotFound)
		return
	}
	if req.Header.Get("Authorization") != "Bearer test-token" {
		http.Error(w, "missing bearer token", http.StatusUnauthorized)
		return
	}
	var command map[string]any
	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.mu.Lock()
	r.commands = append(r.commands, command)
	r.mu.Unlock()
	status := r.status
	if status == 0 {
		status = http.StatusOK
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if r.body != "" {
		_, _ = io.WriteString(w, r.body)
		return
	}
	_, _ = io.WriteString(w, `{"sequence":1}`)
}

func (r *dispatchRecorder) snapshot() []map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]map[string]any, len(r.commands))
	copy(out, r.commands)
	return out
}

func TestIsRunningUsesLatestTurnAsSessionBoundary(t *testing.T) {
	archivedAt := time.Now()
	tests := []struct {
		name   string
		thread domain.Thread
		want   bool
	}{
		{name: "running turn", thread: domain.Thread{TurnID: "turn-1", TurnState: "running", SessionStatus: "running"}, want: true},
		{name: "completed turn with reusable running session", thread: domain.Thread{TurnID: "turn-1", TurnState: "completed", SessionStatus: "running"}, want: false},
		{name: "errored turn with reusable starting session", thread: domain.Thread{TurnID: "turn-1", TurnState: "error", SessionStatus: "starting"}, want: false},
		{name: "background work after completed turn", thread: domain.Thread{TurnID: "turn-1", TurnState: "completed", SessionStatus: "running", BackgroundWork: "working"}, want: true},
		{name: "running session before first turn projection", thread: domain.Thread{SessionStatus: "running"}, want: true},
		{name: "archived thread is stopped", thread: domain.Thread{TurnState: "running", SessionStatus: "running", BackgroundWork: "working", ArchivedAt: &archivedAt}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsRunning(test.thread); got != test.want {
				t.Fatalf("IsRunning(%+v) = %v, want %v", test.thread, got, test.want)
			}
		})
	}
}

func TestResolveProjectID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet || req.URL.Path != "/api/orchestration/shell" {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"projects":[{"id":"project-1","title":"Citadel"},{"id":"project-2","title":"Duplicate"},{"id":"project-3","title":"Duplicate"}],"threads":[]}`)
	}))
	defer server.Close()
	control := New(
		t3api.New(server.URL, t3api.StaticToken("test-token"), time.Second),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		false,
	)
	for _, test := range []struct {
		name        string
		input       string
		want        string
		errContains string
	}{
		{name: "id", input: "project-1", want: "project-1"},
		{name: "title", input: "Citadel", want: "project-1"},
		{name: "missing", input: "Missing", errContains: "not found"},
		{name: "ambiguous", input: "Duplicate", errContains: "ambiguous"},
		{name: "empty", input: " ", errContains: "required"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := control.ResolveProjectID(context.Background(), test.input)
			if test.errContains != "" {
				if err == nil || !strings.Contains(err.Error(), test.errContains) {
					t.Fatalf("ResolveProjectID error = %v, want %q", err, test.errContains)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("ResolveProjectID = %q, %v; want %q", got, err, test.want)
			}
		})
	}
}

func TestCreateAndStartThreadSerializesPreparedWorkspace(t *testing.T) {
	workspace := t.TempDir()
	if err := os.Mkdir(filepath.Join(workspace, ".git"), 0o755); err != nil {
		t.Fatalf("create checkout marker: %v", err)
	}
	recorder := &dispatchRecorder{}
	server := httptest.NewServer(recorder)
	defer server.Close()

	control := New(
		t3api.New(server.URL, t3api.StaticToken("test-token"), time.Second),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		false,
	)
	const threadID = "3124c35e-1551-5d86-b45a-9f859871881b"
	gotID, err := control.CreateAndStartThread(context.Background(), NewThreadInput{
		ThreadID:       threadID,
		ProjectID:      "project-1",
		Title:          "prepared attempt",
		ModelSelection: map[string]any{"instanceId": "codex", "model": "gpt-5.6-sol"},
		Branch:         "feature/backlog-orchestrator",
		WorktreePath:   workspace,
		Prompt:         "perform the prepared task",
	})
	if err != nil {
		t.Fatalf("create prepared thread: %v", err)
	}
	if gotID != threadID {
		t.Fatalf("thread id = %q, want caller-provided %q", gotID, threadID)
	}

	commands := recorder.snapshot()
	if len(commands) != 2 {
		t.Fatalf("dispatch count = %d, want create and turn", len(commands))
	}
	create, turn := commands[0], commands[1]
	assertCommandField(t, create, "type", "thread.create")
	assertCommandField(t, create, "threadId", threadID)
	assertCommandField(t, create, "branch", "feature/backlog-orchestrator")
	assertCommandField(t, create, "worktreePath", workspace)
	assertCommandField(t, turn, "type", "thread.turn.start")
	assertCommandField(t, turn, "threadId", threadID)
	if create["commandId"] == "" || turn["commandId"] == "" || create["commandId"] == turn["commandId"] {
		t.Fatalf("command ids are missing or reused: create=%v turn=%v", create["commandId"], turn["commandId"])
	}
}

func TestCreateThreadRejectionReturnsDeterministicIDAndPreservesWorkspace(t *testing.T) {
	workspace := t.TempDir()
	marker := filepath.Join(workspace, "prepared.txt")
	if err := os.WriteFile(marker, []byte("ready"), 0o644); err != nil {
		t.Fatalf("write workspace marker: %v", err)
	}
	recorder := &dispatchRecorder{
		status: http.StatusUnprocessableEntity,
		body:   `{"_tag":"InvalidWorktreePath","reason":"path rejected by isolated endpoint"}`,
	}
	server := httptest.NewServer(recorder)
	defer server.Close()

	control := New(
		t3api.New(server.URL, t3api.StaticToken("test-token"), time.Second),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		false,
	)
	const threadID = "3124c35e-1551-5d86-b45a-9f859871881b"
	gotID, err := control.CreateAndStartThread(context.Background(), NewThreadInput{
		ThreadID:     threadID,
		ProjectID:    "project-1",
		Title:        "rejected prepared attempt",
		Branch:       "main",
		WorktreePath: workspace,
		Prompt:       "must not start",
	})
	if err == nil {
		t.Fatal("create prepared thread unexpectedly succeeded")
	}
	if gotID != threadID {
		t.Fatalf("thread id on rejection = %q, want %q for reconciliation", gotID, threadID)
	}
	var apiErr *t3api.APIError
	if !errors.As(err, &apiErr) || apiErr.Status != http.StatusUnprocessableEntity || apiErr.Tag != "InvalidWorktreePath" {
		t.Fatalf("error = %v, want tagged 422 API error", err)
	}
	if got := len(recorder.snapshot()); got != 1 {
		t.Fatalf("dispatch count = %d, want only rejected create", got)
	}
	content, readErr := os.ReadFile(marker)
	if readErr != nil {
		t.Fatalf("prepared workspace was not retained: %v", readErr)
	}
	if string(content) != "ready" {
		t.Fatalf("prepared workspace marker = %q, want ready", content)
	}
}

func TestCreateThreadTransportErrorReturnsDeterministicIDAndPreservesWorkspace(t *testing.T) {
	workspace := t.TempDir()
	marker := filepath.Join(workspace, "prepared.txt")
	if err := os.WriteFile(marker, []byte("ready"), 0o644); err != nil {
		t.Fatalf("write workspace marker: %v", err)
	}
	server := httptest.NewServer(http.NotFoundHandler())
	server.Close()

	control := New(
		t3api.New(server.URL, t3api.StaticToken("test-token"), time.Second),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		false,
	)
	const threadID = "3124c35e-1551-5d86-b45a-9f859871881b"
	gotID, err := control.CreateAndStartThread(context.Background(), NewThreadInput{
		ThreadID:     threadID,
		ProjectID:    "project-1",
		Title:        "unreachable prepared attempt",
		Branch:       "main",
		WorktreePath: workspace,
	})
	if err == nil {
		t.Fatal("create against closed endpoint unexpectedly succeeded")
	}
	if gotID != threadID {
		t.Fatalf("thread id on transport error = %q, want %q for reconciliation", gotID, threadID)
	}
	if content, readErr := os.ReadFile(marker); readErr != nil || string(content) != "ready" {
		t.Fatalf("prepared workspace was not retained: content=%q err=%v", content, readErr)
	}
}

func TestCreateThreadWithoutWorkspaceSerializesNullFields(t *testing.T) {
	recorder := &dispatchRecorder{
		status: http.StatusUnprocessableEntity,
		body:   `{"_tag":"Rejected","reason":"stop after create"}`,
	}
	server := httptest.NewServer(recorder)
	defer server.Close()

	control := New(t3api.New(server.URL, t3api.StaticToken("test-token"), time.Second), nil, false)
	gotID, err := control.CreateAndStartThread(context.Background(), NewThreadInput{ProjectID: "project-1", Title: "legacy"})
	if err == nil {
		t.Fatal("create unexpectedly succeeded")
	}
	if gotID != "" {
		t.Fatalf("generated thread id leaked on rejected legacy create: %q", gotID)
	}
	commands := recorder.snapshot()
	if len(commands) != 1 {
		t.Fatalf("dispatch count = %d, want one", len(commands))
	}
	if value, ok := commands[0]["branch"]; !ok || value != nil {
		t.Fatalf("branch = %#v, present=%v; want explicit null", value, ok)
	}
	if value, ok := commands[0]["worktreePath"]; !ok || value != nil {
		t.Fatalf("worktreePath = %#v, present=%v; want explicit null", value, ok)
	}
}

func TestCreateAndStartThreadUsesDeterministicDispatchToken(t *testing.T) {
	recorder := &dispatchRecorder{}
	server := httptest.NewServer(recorder)
	defer server.Close()
	control := New(
		t3api.New(server.URL, t3api.StaticToken("test-token"), time.Second),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		false,
	)
	input := NewThreadInput{
		ThreadID:      "3124c35e-1551-5d86-b45a-9f859871881b",
		DispatchToken: "dispatch-1",
		ProjectID:     "project-1",
		Title:         "deterministic dispatch",
		Prompt:        "continue",
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := control.CreateAndStartThread(context.Background(), input); err != nil {
			t.Fatalf("dispatch attempt %d: %v", attempt+1, err)
		}
	}
	commands := recorder.snapshot()
	if len(commands) != 4 {
		t.Fatalf("recorded %d commands, want 4", len(commands))
	}
	if commands[0]["commandId"] != commands[2]["commandId"] ||
		commands[1]["commandId"] != commands[3]["commandId"] {
		t.Fatalf("dispatch command IDs changed across retry: %#v", commands)
	}
	firstMessage := commands[1]["message"].(map[string]any)
	secondMessage := commands[3]["message"].(map[string]any)
	if firstMessage["messageId"] != secondMessage["messageId"] {
		t.Fatalf("dispatch message ID changed across retry: %v != %v",
			firstMessage["messageId"], secondMessage["messageId"])
	}
	if commands[0]["commandId"] == commands[1]["commandId"] ||
		commands[0]["commandId"] == firstMessage["messageId"] ||
		commands[1]["commandId"] == firstMessage["messageId"] {
		t.Fatal("dispatch token did not derive purpose-specific IDs")
	}
}

func TestSettleThreadUsesDeterministicEffectAndConfirmsProjection(t *testing.T) {
	var mu sync.Mutex
	var commands []map[string]any
	settled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case req.Method == http.MethodPost && req.URL.Path == "/api/orchestration/dispatch":
			var command map[string]any
			if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			mu.Lock()
			commands = append(commands, command)
			settled = true
			mu.Unlock()
			_, _ = io.WriteString(w, `{"sequence":1}`)
		case req.Method == http.MethodGet && req.URL.Path == "/api/orchestration/shell":
			mu.Lock()
			isSettled := settled
			mu.Unlock()
			if isSettled {
				_, _ = io.WriteString(w, `{"projects":[],"threads":[{"id":"thread-1","title":"task","updatedAt":"2026-09-11T22:00:00.000Z","modelSelection":{},"settledAt":"2026-09-11T22:00:00.000Z","settledOverride":"settled"}]}`)
				return
			}
			_, _ = io.WriteString(w, `{"projects":[],"threads":[{"id":"thread-1","title":"task","updatedAt":"2026-09-11T21:59:00.000Z","modelSelection":{}}]}`)
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()
	control := New(t3api.New(server.URL, t3api.StaticToken("test-token"), time.Second), nil, false)
	for attempt := 0; attempt < 2; attempt++ {
		if err := control.SettleThread(context.Background(), "thread-1", "dispatch-1"); err != nil {
			t.Fatalf("settle attempt %d: %v", attempt+1, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(commands) != 2 {
		t.Fatalf("commands = %d, want 2", len(commands))
	}
	assertCommandField(t, commands[0], "type", "thread.settle")
	assertCommandField(t, commands[0], "threadId", "thread-1")
	if commands[0]["commandId"] == "" || commands[0]["commandId"] != commands[1]["commandId"] {
		t.Fatalf("settlement command IDs are not deterministic: %#v", commands)
	}
}

func assertCommandField(t *testing.T, command map[string]any, key string, want any) {
	t.Helper()
	if got := command[key]; got != want {
		t.Fatalf("%s = %#v, want %#v", key, got, want)
	}
}
