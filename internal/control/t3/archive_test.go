package t3

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/t3api"
)

func TestArchiveConfirmsLostResponseAndRefusesChangedSettlement(t *testing.T) {
	now := time.Now().UTC()
	settled := now.Add(-25 * time.Hour)
	var mu sync.Mutex
	var archived *string
	settlement := settled.Format(time.RFC3339Nano)
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path == "/api/orchestration/shell" {
			json.NewEncoder(w).Encode(map[string]any{"threads": []map[string]any{{"id": "thread", "settledAt": settlement, "archivedAt": archived, "latestTurn": map[string]string{"state": "completed"}}}})
			return
		}
		if r.URL.Path == "/api/orchestration/dispatch" {
			var command map[string]any
			if err := json.NewDecoder(r.Body).Decode(&command); err != nil {
				t.Error(err)
			}
			if command["type"] != "thread.archive" || command["threadId"] != "thread" {
				t.Errorf("unexpected command: %v", command)
			}
			calls++
			stamp := now.Format(time.RFC3339Nano)
			archived = &stamp
			w.WriteHeader(500) // Effect applied, response lost: observation is authoritative.
			return
		}
		w.WriteHeader(404)
	}))
	defer server.Close()
	control := New(t3api.New(server.URL, t3api.StaticToken("token"), time.Second), nil, false)
	expected := domain.Thread{ID: "thread", SettledAt: &settled}
	if err := control.ArchiveSettledThread(context.Background(), expected); err != nil {
		t.Fatal(err)
	}
	if err := control.ArchiveSettledThread(context.Background(), expected); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if calls != 1 {
		t.Errorf("dispatches=%d", calls)
	}
	archived = nil
	settlement = now.Format(time.RFC3339Nano)
	mu.Unlock()
	if err := control.ArchiveSettledThread(context.Background(), expected); err == nil {
		t.Fatal("new settlement archived")
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatal("changed settlement dispatched")
	}
}
