package workerruntime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestDiscoveredModelPolicyUsesAuthenticatedConcreteCatalog(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "caches"), 0700); err != nil {
		t.Fatal(err)
	}
	write := func(models []string, auth string) {
		entries := []map[string]string{}
		for _, model := range models {
			entries = append(entries, map[string]string{"slug": model})
		}
		raw, _ := json.Marshal(map[string]any{"instanceId": "codex", "enabled": true, "installed": true, "status": "ready", "auth": map[string]string{"status": auth}, "models": entries})
		if err := os.WriteFile(filepath.Join(dir, "caches/codex.json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	wanted := domain.WorkerInventory{ID: "worker", Providers: []domain.WorkerProviderInventory{{InstanceID: "codex", Models: []string{"*"}, QuotaPoolID: "pool"}}}
	probe := HostInventoryProbe{DataDir: dir}
	observe := func(want []string, available bool) {
		t.Helper()
		got, err := probe.Observe(context.Background(), wanted)
		if err != nil {
			t.Fatal(err)
		}
		p := got.Providers[0]
		if !reflect.DeepEqual(p.Models, want) || p.Available != available {
			t.Fatalf("got %#v, want %v available=%v", p, want, available)
		}
	}
	write([]string{"old"}, "authenticated")
	observe([]string{"old"}, true)
	write([]string{"old", "new", "*", ""}, "authenticated")
	observe([]string{"old", "new"}, true)
	write([]string{"new"}, "unauthenticated")
	observe(nil, false)
	write([]string{"old", "new"}, "authenticated")
	wanted.Providers[0].Models = []string{"old"}
	observe([]string{"old"}, true)
	wanted.Providers[0].Models = nil
	observe(nil, false)
	if len(wanted.Providers[0].Models) != 0 {
		t.Fatal("probe changed authored authorization")
	}
}
