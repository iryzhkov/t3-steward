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

func TestArchiveUsesCommitReceiptWhenArchivedThreadLeavesShell(t *testing.T) {
	for _, test := range []struct {
		name      string
		status    int
		sequence  int
		wantError bool
	}{
		{"committed", 200, 42, false}, {"lost response", 500, 0, true}, {"invalid receipt", 200, 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now().UTC()
			settled := now.Add(-25 * time.Hour)
			var mu sync.Mutex
			archived := false
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch r.URL.Path {
				case "/api/orchestration/shell":
					threads := []map[string]any{}
					if !archived {
						threads = append(threads, map[string]any{"id": "thread", "settledAt": settled.Format(time.RFC3339Nano), "latestTurn": map[string]string{"state": "completed"}})
					}
					json.NewEncoder(w).Encode(map[string]any{"threads": threads})
				case "/api/orchestration/dispatch":
					var command map[string]any
					json.NewDecoder(r.Body).Decode(&command)
					if command["type"] != "thread.archive" || command["threadId"] != "thread" {
						t.Errorf("unexpected command: %v", command)
					}
					calls++
					archived = true
					w.WriteHeader(test.status)
					json.NewEncoder(w).Encode(map[string]int{"sequence": test.sequence})
				default:
					w.WriteHeader(404)
				}
			}))
			defer server.Close()
			control := New(t3api.New(server.URL, t3api.StaticToken("token"), time.Second), nil, false)
			err := control.ArchiveSettledThread(context.Background(), domain.Thread{ID: "thread", SettledAt: &settled})
			if (err != nil) != test.wantError {
				t.Fatalf("err=%v wantError=%t", err, test.wantError)
			}
			mu.Lock()
			defer mu.Unlock()
			if calls != 1 {
				t.Fatalf("dispatches=%d", calls)
			}
		})
	}
}

func TestArchiveRefusesChangedSettlement(t *testing.T) {
	now := time.Now().UTC()
	old := now.Add(-25 * time.Hour)
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/orchestration/shell" {
			json.NewEncoder(w).Encode(map[string]any{"threads": []map[string]any{{"id": "thread", "settledAt": now.Format(time.RFC3339Nano)}}})
			return
		}
		calls++
		w.WriteHeader(500)
	}))
	defer server.Close()
	control := New(t3api.New(server.URL, t3api.StaticToken("token"), time.Second), nil, false)
	if err := control.ArchiveSettledThread(context.Background(), domain.Thread{ID: "thread", SettledAt: &old}); err == nil {
		t.Fatal("new settlement archived")
	}
	if calls != 0 {
		t.Fatal("changed settlement dispatched")
	}
}
