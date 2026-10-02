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

// A card and answer as T3 0.0.38 recorded them in the RC1 field test
// (2026-09-24, homelab), with identifiers zeroed: answers keyed by the exact
// question text, the option label as the value, nothing trimmed.
const capturedRC1Card = `{"thread":{"id":"relay","messages":[],"activities":[
 {"id":"00000000-0000-0000-0000-000000000001","tone":"info","kind":"user-input.requested","summary":"User input requested","turnId":"00000000-0000-0000-0000-000000000003","createdAt":"2026-09-24T06:20:14.337Z",
  "payload":{"requestId":"00000000-0000-0000-0000-000000000004","questions":[{"id":"For this RC1 field test, choose amber or violet. Your choice determines the dependent rendered result.","header":"RC1 choice","question":"For this RC1 field test, choose amber or violet. Your choice determines the dependent rendered result.","options":[{"label":"amber","description":"Selects the amber-rendered result path."},{"label":"violet","description":"Selects the violet-rendered result path."}],"multiSelect":false}]}},
 {"id":"00000000-0000-0000-0000-000000000002","tone":"info","kind":"user-input.resolved","summary":"User input submitted","turnId":"00000000-0000-0000-0000-000000000003","createdAt":"2026-09-24T06:25:45.963Z",
  "payload":{"requestId":"00000000-0000-0000-0000-000000000004","answers":{"For this RC1 field test, choose amber or violet. Your choice determines the dependent rendered result.":"amber"}}}
]}}`

func TestTheCapturedRC1CardMatchesItsAskExactly(t *testing.T) {
	var detail struct {
		Thread t3api.ThreadDetail `json:"thread"`
	}
	if err := json.Unmarshal([]byte(capturedRC1Card), &detail); err != nil {
		t.Fatal(err)
	}
	events, err := UserInputEventsOf(detail.Thread.Activities)
	if err != nil || len(events) != 2 {
		t.Fatalf("events=%v err=%v", events, err)
	}
	ask := domain.AskRequest{
		Question: "For this RC1 field test, choose amber or violet. Your choice determines the dependent rendered result.",
		Options:  []string{"amber", "violet"}, OnDeadline: domain.AskDeadlineFail,
	}
	if !events[0].Questions[0].MatchesAsk(ask) {
		t.Fatal("the captured card did not match the question it asked")
	}
	if options, free := ask.SplitAnswer(events[1].AnswerValues(events[0].Questions[0])); len(options) != 1 || options[0] != "amber" || free != "" {
		t.Fatalf("answer = %v %q", options, free)
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
		if err := control.CreateAskRelayThread(context.Background(), start); err != nil {
			t.Fatal(err)
		}
		if err := control.StartAskRelayTurn(context.Background(), start); err != nil {
			t.Fatal(err)
		}
	}
	if commands[0]["runtimeMode"] != AskRelayRuntimeMode || commands[1]["runtimeMode"] != AskRelayRuntimeMode || AskRelayRuntimeMode != "approval-required" {
		t.Fatalf("the relay is not on the most restricted runtime mode: %v %v", commands[0]["runtimeMode"], commands[1]["runtimeMode"])
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
