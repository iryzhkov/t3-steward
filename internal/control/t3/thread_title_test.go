package t3

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/testtiming"

	"github.com/iryzhkov/t3-steward/internal/t3api"
)

// The title mutation is T3's public thread.meta.update with only a title. It
// carries no other metadata, so it cannot move a branch, worktree or model.
func TestUpdateThreadTitleDispatchesMetaUpdateWithTitleOnly(t *testing.T) {
	recorder := &dispatchRecorder{}
	server := httptest.NewServer(recorder)
	defer server.Close()
	control := New(t3api.New(server.URL, t3api.StaticToken("test-token"), testtiming.Bound(time.Second)), slog.New(slog.NewTextHandler(io.Discard, nil)), false)
	if err := control.UpdateThreadTitle(context.Background(), "thread-1", "[Steward] M16 · executor · running"); err != nil {
		t.Fatal(err)
	}
	commands := recorder.snapshot()
	if len(commands) != 1 {
		t.Fatalf("commands = %v", commands)
	}
	command := commands[0]
	if command["type"] != "thread.meta.update" || command["threadId"] != "thread-1" || command["title"] != "[Steward] M16 · executor · running" {
		t.Fatalf("command = %v", command)
	}
	if id, _ := command["commandId"].(string); id == "" {
		t.Fatalf("command has no id: %v", command)
	}
	for key := range command {
		switch key {
		case "type", "commandId", "threadId", "title":
		default:
			t.Fatalf("title update carries %q: %v", key, command)
		}
	}
}

func TestUpdateThreadTitleRefusesEmptyInputAndReportsRejection(t *testing.T) {
	recorder := &dispatchRecorder{status: http.StatusBadRequest, body: `{"error":"rejected"}`}
	server := httptest.NewServer(recorder)
	defer server.Close()
	control := New(t3api.New(server.URL, t3api.StaticToken("test-token"), testtiming.Bound(time.Second)), slog.New(slog.NewTextHandler(io.Discard, nil)), false)
	if err := control.UpdateThreadTitle(context.Background(), "", "title"); err == nil {
		t.Fatal("an empty thread id was accepted")
	}
	if err := control.UpdateThreadTitle(context.Background(), "thread-1", "  "); err == nil {
		t.Fatal("an empty title was accepted")
	}
	if len(recorder.snapshot()) != 0 {
		t.Fatal("refused input was dispatched")
	}
	if err := control.UpdateThreadTitle(context.Background(), "thread-1", "title"); err == nil {
		t.Fatal("a rejected dispatch reported success")
	}
	control.DryRun = true
	before := len(recorder.snapshot())
	if err := control.UpdateThreadTitle(context.Background(), "thread-1", "title"); err != nil || len(recorder.snapshot()) != before {
		t.Fatalf("dry run dispatched or failed: %v", err)
	}
}
