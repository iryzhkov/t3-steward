package domain

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestWorkerInventoryJSONRoundTrip(t *testing.T) {
	now := time.Date(2026, 9, 10, 18, 0, 0, 0, time.UTC)
	want := WorkerInventory{
		ID: "normandy", AcceptBacklog: true, Health: WorkerHealthReady,
		Capabilities: []string{"docker", "internet"},
		Projects: []WorkerProjectInventory{{
			Name: "t3-steward", Available: true, Revision: "abc123", UpdatedAt: now,
		}},
		Providers: []WorkerProviderInventory{{
			InstanceID: "codex", Models: []string{"gpt-5.6-sol"},
			QuotaPoolID: "openai-account", Available: true,
		}},
		WebBaseURL: "https://normandy.example.test",
		ObservedAt: now,
	}
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal worker inventory: %v", err)
	}
	var got WorkerInventory
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal worker inventory: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip mismatch:\n got: %#v\nwant: %#v", got, want)
	}
}
