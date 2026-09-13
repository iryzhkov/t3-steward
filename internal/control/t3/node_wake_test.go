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

func TestNodeWakeUsesObservableStableMessageIdentity(t *testing.T) {
	const token = "node-wake:nw-test"
	commands := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			var sent map[string]any
			if err := json.NewDecoder(r.Body).Decode(&sent); err != nil {
				t.Error(err)
			}
			commands <- sent
			_ = json.NewEncoder(w).Encode(map[string]any{"sequence": 1})
			return
		}
		if r.URL.Query().Get("turnLimit") != "100" {
			t.Error("observation was not bounded")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"thread": map[string]any{"id": "thread", "messages": []map[string]any{{"id": nodeWakeID(token, "message"), "role": "user", "text": "wake"}}}})
	}))
	defer server.Close()
	control := New(t3api.New(server.URL, t3api.StaticToken("test"), time.Second), nil, true)
	thread := domain.Thread{ID: "thread", ModelSelection: map[string]any{"provider": "test"}}
	if err := control.SendNodeWake(context.Background(), thread, token, "wake"); err != nil {
		t.Fatal(err)
	}
	sent := <-commands
	if sent["commandId"] != nodeWakeID(token, "command") || sent["message"].(map[string]any)["messageId"] != nodeWakeID(token, "message") {
		t.Fatalf("command=%+v", sent)
	}
	found, err := control.ObserveNodeWake(context.Background(), "thread", token)
	if err != nil || !found {
		t.Fatal(found, err)
	}
	found, err = control.ObserveNodeWake(context.Background(), "thread", "other")
	if err != nil || found {
		t.Fatal(found, err)
	}
}
