package backlogadmin

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/config"
	"testing"
	"time"
)

func TestReviewCatalogCarriesAuthoritativeMetadata(t *testing.T) {
	store := openAdminTestStore(t)
	snapshot := viabilityWorkerSnapshot()
	snapshot.Sequence = 1
	snapshot.CoordinatorEpoch = 1
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
	settings.ReviewRoutes = map[string]config.ReviewRouteMetadata{"t3-primary/opus": {ProviderFamily: "claude", Tier: "executor"}}
	service.SetViability(settings)
	response, err := service.Query(context.Background(), Query{Version: Version, Kind: QueryProjects, Principal: Principal{ID: "operator"}})
	if err != nil {
		t.Fatal(err)
	}
	r := response.Projects[0].Workers[0].Routes[0]
	if r.ProviderFamily != "claude" || r.Tier != "executor" {
		t.Fatalf("metadata absent: %+v", r)
	}
	settings.ReviewRoutes = nil
	service.SetViability(settings)
	response, err = service.Query(context.Background(), Query{Version: Version, Kind: QueryProjects, Principal: Principal{ID: "operator"}})
	if err != nil {
		t.Fatal(err)
	}
	r = response.Projects[0].Workers[0].Routes[0]
	if r.ProviderFamily != "" || r.Tier != "" {
		t.Fatal("inferred classification for old catalog")
	}
}
