package backlogadmin

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
)

func TestReviewProjectsSlashModelsMetadata(t *testing.T) {
	store := openAdminTestStore(t)
	snapshot := viabilityWorkerSnapshot()
	snapshot.Sequence, snapshot.CoordinatorEpoch = 1, 1
	snapshot.Inventory.Providers[0].InstanceID = "opencode"
	models := []string{"deepseek/deepseek-flash", "vendor/team/model", "ollama/qwen3-coder:30b"}
	snapshot.Inventory.Providers[0].Models = models
	if err := store.SaveWorkerSnapshot(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return viabilityNow })
	service.SetRuntimeInfo(RuntimeInfo{Epoch: 1, MaxWorkerSnapshotAge: time.Minute})
	settings := viabilityCatalog(t)
	settings.ReviewRoutes = map[string]config.ReviewRouteMetadata{
		"opencode/deepseek/deepseek-flash": {ProviderFamily: "deepseek", Tier: "executor"},
		"opencode/vendor/team/model":       {ProviderFamily: "vendor", Tier: "critical"},
		"opencode/ollama/qwen3-coder:30b":  {ProviderFamily: "ollama", Tier: "economy"},
	}
	service.SetViability(settings)
	response, err := service.Query(context.Background(), Query{Version: Version, Kind: QueryProjects, Principal: Principal{ID: "operator"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Projects) != 1 || len(response.Projects[0].Workers) != 1 {
		t.Fatalf("projects: %+v", response.Projects)
	}
	routes := response.Projects[0].Workers[0].Routes
	if len(routes) != len(models) {
		t.Fatalf("routes: %+v", routes)
	}
	for _, route := range routes {
		metadata, ok := settings.ReviewRoutes[route.Instance+"/"+route.Model]
		if !ok || route.ProviderFamily != metadata.ProviderFamily || route.Tier != metadata.Tier {
			t.Fatalf("metadata not attached to exact route: %+v", route)
		}
	}
}
