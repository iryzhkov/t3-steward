package backlogadmin

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// TestProjectsQueryListsTheCatalogWithItsEligibleWorkers is the query a client
// matches its checkout against and derives a route from: every configured
// project with its repository, default ref and setup profile, and for each one
// the workers that could take it with what they advertise.
func TestProjectsQueryListsTheCatalogWithItsEligibleWorkers(t *testing.T) {
	store := openAdminTestStore(t)
	homelabSnapshot := viabilityWorkerSnapshot()
	homelabSnapshot.Sequence, homelabSnapshot.CoordinatorEpoch = 1, 1
	if err := store.SaveWorkerSnapshot(context.Background(), homelabSnapshot); err != nil {
		t.Fatal(err)
	}
	// A second worker advertises a different project and must not be listed
	// as eligible for t3-steward.
	other := viabilityWorkerSnapshot()
	other.Sequence, other.CoordinatorEpoch = 1, 1
	other.WorkerID, other.Inventory.ID = "normandy", "normandy"
	other.Inventory.Projects = []domain.WorkerProjectInventory{{Name: "elsewhere", Available: true}}
	if err := store.SaveWorkerSnapshot(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return viabilityNow })
	service.SetRuntimeInfo(RuntimeInfo{Epoch: 1, MaxWorkerSnapshotAge: time.Minute})
	settings := viabilityCatalog(t)
	settings.ProjectWorkers = map[string][]string{"t3-steward": {"homelab", "laptop"}}
	service.SetViability(settings)

	response, err := service.Query(context.Background(), Query{
		Version: Version, Kind: QueryProjects, Principal: Principal{ID: "operator"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Kind != QueryProjects || len(response.Projects) != 1 {
		t.Fatalf("response = %+v", response)
	}
	project := response.Projects[0]
	if project.Name != "t3-steward" || project.Repository != "https://github.com/iryzhkov/t3-steward" ||
		project.DefaultRef != "main" || project.SetupProfile != "go" {
		t.Fatalf("project = %+v", project)
	}
	if len(project.Workers) != 2 {
		t.Fatalf("workers = %+v, want homelab and the configured-only laptop", project.Workers)
	}
	homelab, laptop := project.Workers[0], project.Workers[1]
	if homelab.Worker != "homelab" || !homelab.Configured || !homelab.Advertises {
		t.Fatalf("homelab = %+v", homelab)
	}
	// The snapshot is fresh and connected but the worker has no enrollment
	// record in this store, so it is observed and not ready.
	if homelab.Enrolled || homelab.Ready || homelab.State != "observed" || homelab.Health != "ready" {
		t.Fatalf("homelab readiness = %+v", homelab)
	}
	if len(homelab.Routes) != 1 || homelab.Routes[0] != (ProjectRoute{Instance: "t3-primary", Model: "opus", QuotaPool: "pool-1"}) {
		t.Fatalf("homelab routes = %+v", homelab.Routes)
	}
	if laptop.Worker != "laptop" || !laptop.Configured || laptop.Advertises || laptop.State != "unknown" || len(laptop.Routes) != 0 {
		t.Fatalf("laptop = %+v", laptop)
	}

	// The filter narrows to one project, and an absent one is an empty list
	// rather than an error: the caller reads the count.
	filtered, err := service.Query(context.Background(), Query{
		Version: Version, Kind: QueryProjects, Principal: Principal{ID: "operator"},
		Filter: Filter{Project: "absent"},
	})
	if err != nil || len(filtered.Projects) != 0 {
		t.Fatalf("filtered = %+v, %v", filtered.Projects, err)
	}
}

// A coordinator with no catalog refuses the query rather than answering with
// an empty fleet, exactly as the viability query does.
func TestProjectsQueryRefusesWithoutACatalog(t *testing.T) {
	service, err := New(openAdminTestStore(t), &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Query(context.Background(), Query{
		Version: Version, Kind: QueryProjects, Principal: Principal{ID: "operator"},
	})
	if err == nil || !strings.Contains(err.Error(), "no project catalog") {
		t.Fatalf("err = %v", err)
	}
}

// TestProjectsIsADeclaredReadView keeps the kind on the list authorization
// reads, so a remote client can ask it.
func TestProjectsIsADeclaredReadView(t *testing.T) {
	if !IsQueryKind(QueryProjects) {
		t.Fatal("projects is not a declared query kind")
	}
}
