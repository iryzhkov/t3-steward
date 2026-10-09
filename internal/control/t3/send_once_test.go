package t3

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/testtiming"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/t3api"
)

// A turn started with SendOnce carries identities derived from its token, and
// a retry after the message is visible in the thread sends nothing, so a
// worker that restarts mid-send never starts a second turn.
func TestSendOnceUsesStableIdentitiesAndSkipsADeliveredMessage(t *testing.T) {
	const token, purpose = "dispatch-1/turn-end/turn-1", "turn-end-nudge"
	var mu sync.Mutex
	var commands []map[string]any
	var delivered bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodPost {
			var sent map[string]any
			if err := json.NewDecoder(r.Body).Decode(&sent); err != nil {
				t.Error(err)
			}
			commands = append(commands, sent)
			_ = json.NewEncoder(w).Encode(map[string]any{"sequence": 1})
			return
		}
		messages := []map[string]any{}
		if delivered {
			messages = append(messages, map[string]any{"id": deterministicID(token, purpose+"-message"), "role": "user", "text": "wait"})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"thread": map[string]any{"id": "thread", "messages": messages}})
	}))
	defer server.Close()
	control := New(t3api.New(server.URL, t3api.StaticToken("test"), testtiming.Bound(time.Second)), nil, false)
	thread := domain.Thread{ID: "thread", ModelSelection: map[string]any{"provider": "test"}}

	for range 2 {
		if err := control.SendOnce(context.Background(), thread, token, purpose, "wait"); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	if len(commands) != 2 || commands[0]["commandId"] != commands[1]["commandId"] ||
		commands[0]["commandId"] != deterministicID(token, purpose+"-command") ||
		commands[0]["message"].(map[string]any)["messageId"] != deterministicID(token, purpose+"-message") {
		t.Fatalf("undelivered sends did not reuse the derived identities: %+v", commands)
	}
	delivered = true
	mu.Unlock()

	if err := control.SendOnce(context.Background(), thread, token, purpose, "wait"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(commands) != 2 {
		t.Fatalf("a delivered message was sent again: %d commands", len(commands))
	}
}
