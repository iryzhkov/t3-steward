package backlogadmin

import (
	"context"
	"reflect"
	"testing"
	"time"
)

// The workers view reports the provider authorization the coordinator was
// configured with, per worker, including the instances the load dropped.
// Nothing else can answer it: a quota pool has no worker dimension, and a
// worker inventory reports what exists on the host, not what the fleet
// authorized for it.
func TestWorkersQueryCarriesTheConfiguredProviderAuthorization(t *testing.T) {
	store := openAdminTestStore(t)
	seedAdminTestStore(t, store)
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return adminTestNow })
	service.SetRuntimeInfo(RuntimeInfo{Mode: "coordinator", Owner: "coordinator-1", Epoch: 1, Transport: "ssh",
		MaxWorkerSnapshotAge: time.Minute, MaxQuotaObservationAge: time.Minute})
	authorization := []WorkerProviderAuthorization{
		{Instance: "codex", QuotaPool: "codex-main", Models: []string{"gpt-5-codex"}},
		{Instance: "opencode", Dropped: "missing binding"},
	}
	service.SetWorkerAuthorization(map[string][]WorkerProviderAuthorization{
		"normandy": authorization,
		"absent":   {{Instance: "ollama", QuotaPool: "local"}},
	})
	response, err := service.Query(context.Background(), Query{
		Version: Version, Kind: QueryWorkers, Principal: Principal{ID: "operator"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Workers) != 1 {
		t.Fatalf("workers = %+v, want only the worker the coordinator knows", response.Workers)
	}
	if !reflect.DeepEqual(response.Workers[0].Providers, authorization) {
		t.Fatalf("providers = %+v, want %+v", response.Workers[0].Providers, authorization)
	}
	// The response is a copy: a reader cannot alter what the service reports.
	response.Workers[0].Providers[0].Instance = "mutated"
	again, err := service.Query(context.Background(), Query{
		Version: Version, Kind: QueryWorkers, Principal: Principal{ID: "operator"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if again.Workers[0].Providers[0].Instance != "codex" {
		t.Fatal("the workers view aliases the configured authorization")
	}
}

// A coordinator that sets none answers exactly as it did before, so a release
// that has not been given the authorization does not report an empty one as a
// fact about the fleet.
func TestWorkersQueryWithoutConfiguredAuthorizationIsUnchanged(t *testing.T) {
	store := openAdminTestStore(t)
	seedAdminTestStore(t, store)
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return adminTestNow })
	response, err := service.Query(context.Background(), Query{
		Version: Version, Kind: QueryWorkers, Principal: Principal{ID: "operator"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Workers) != 1 || response.Workers[0].Providers != nil {
		t.Fatalf("workers = %+v, want no invented authorization", response.Workers)
	}
}
