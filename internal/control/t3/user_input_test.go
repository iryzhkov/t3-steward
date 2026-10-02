package t3

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/t3api"
)

// The activity shapes are T3 0.0.38's, from ProviderRuntimeIngestion: a
// Claude AskUserQuestion card keys its question by the question text.
const userInputDetail = `{"thread":{"id":"relay","messages":[],"activities":[
 {"id":"a1","tone":"tool","kind":"tool.started","summary":"x","payload":{},"turnId":"t1","createdAt":"2026-10-02T12:00:00.000Z"},
 {"id":"a2","tone":"info","kind":"user-input.requested","summary":"User input requested","turnId":"t1","createdAt":"2026-10-02T12:00:01.000Z",
  "payload":{"requestId":"r1","questions":[{"id":"Pick one","header":"Steward ask","question":"Pick one","options":[{"label":"alpha","description":""},{"label":"beta","description":""}],"multiSelect":false}]}},
 {"id":"a3","tone":"info","kind":"user-input.resolved","summary":"User input submitted","turnId":"t1","createdAt":"2026-10-02T12:03:00.000Z",
  "payload":{"requestId":"r1","answers":{"Pick one":"beta"}}}
]}}`

func TestUserInputEventsDecodesTheQuestionCardAndItsAnswer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("turnLimit") == "" {
			t.Error("the read was not bounded")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(userInputDetail))
	}))
	defer server.Close()
	control := New(t3api.New(server.URL, t3api.StaticToken("test"), time.Second), nil, true)
	events, err := control.UserInputEvents(context.Background(), "relay")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Kind != domain.UserInputRequested || events[1].Kind != domain.UserInputResolved ||
		events[0].RequestID != "r1" || events[0].TurnID != "t1" || len(events[0].Questions) != 1 ||
		len(events[0].Questions[0].Options) != 2 || events[1].At.IsZero() {
		t.Fatalf("events = %+v", events)
	}
	if values := events[1].AnswerValues(events[0].Questions[0]); len(values) != 1 || values[0] != "beta" {
		t.Fatalf("answer = %v", values)
	}
}

func TestStartAskRelayCreatesTheThreadAndItsTurnWithDerivedIdentity(t *testing.T) {
	var commands []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var sent map[string]any
		if err := json.NewDecoder(r.Body).Decode(&sent); err != nil {
			t.Error(err)
		}
		commands = append(commands, sent)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"sequence": len(commands)})
	}))
	defer server.Close()
	control := New(t3api.New(server.URL, t3api.StaticToken("test"), time.Second), nil, false)
	start := domain.AskRelayStart{AskID: "tw-ask-1", ThreadID: domain.AskRelayThreadID("tw-ask-1"), ProjectID: "p",
		Title: "Ask run/task: Pick one", Instance: "claudeAgent", Model: "claude-haiku-4-5", Prompt: "ask it"}
	for range 2 {
		if err := control.StartAskRelay(context.Background(), start); err != nil {
			t.Fatal(err)
		}
	}
	if len(commands) != 4 || commands[0]["type"] != "thread.create" || commands[1]["type"] != "thread.turn.start" ||
		commands[0]["threadId"] != start.ThreadID || commands[0]["projectId"] != "p" {
		t.Fatalf("commands = %+v", commands)
	}
	if commands[0]["commandId"] != commands[2]["commandId"] || commands[1]["commandId"] != commands[3]["commandId"] {
		t.Fatal("a retried relay start sent different command IDs")
	}
	selection := commands[0]["modelSelection"].(map[string]any)
	if selection["instanceId"] != "claudeAgent" || selection["model"] != "claude-haiku-4-5" {
		t.Fatalf("model selection = %+v", selection)
	}
	if err := control.ArchiveThread(context.Background(), start.ThreadID); err != nil || commands[4]["type"] != "thread.archive" {
		t.Fatalf("archive: %v %+v", err, commands[len(commands)-1])
	}
}
