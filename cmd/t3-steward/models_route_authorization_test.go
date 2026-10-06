package main

import (
	"bytes"
	"regexp"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// R3: a worker may advertise a superset of the models the coordinator
// authorizes it for (domain.ModelsObserved accepts that), and models judged a
// route only by advertisement, so an instance authorized for opus that also
// advertised sonnet called t3-primary/sonnet available although
// validatePolicyCatalog refuses it. A route is now available only on a ready
// worker that both advertises the model and is authorized for it, and the text
// row, the JSON document and --available carry that one verdict.
func TestModelsRouteRequiresModelAuthorizationOnTheSameWorker(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	pinModelsNow(t, now)
	workers, quotas := availabilityFixture(now)
	workers[0].Providers = []backlogadmin.WorkerProviderAuthorization{{
		Instance: "t3-primary", QuotaPool: "pool-claude", Models: []string{"opus"},
	}}
	workers[0].Snapshot.Inventory.Providers[0].Models = []string{"opus", "sonnet"}
	policy := &routePolicy{Roles: []policyRole{{Name: "execute",
		Candidates: []policyCandidate{{Route: "t3-primary/sonnet"}}}}}
	if err := validatePolicyCatalog(policy, workers); err == nil {
		t.Fatal("fixture unexpectedly authorizes sonnet")
	}

	document := buildModelsDocument("", workers, quotas, nil, time.Hour)
	var out bytes.Buffer
	if err := renderModels(&out, document); err != nil {
		t.Fatal(err)
	}
	for route, status := range map[string]string{
		"t3-primary/opus":   `1/1 ready\s+available`,
		"t3-primary/sonnet": `none\s+not authorized: .*`,
	} {
		if !regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(route) + `\s.*\s` + status + `\s*$`).MatchString(out.String()) {
			t.Errorf("the row for %s does not end %q:\n%s", route, status, out.String())
		}
	}

	encoded := modelsDocumentKeys(t, mustModelsJSON(t, document))
	instance := encoded["instances"].([]any)[0].(map[string]any)
	codes := map[string]any{}
	for _, route := range instance["routes"].([]any) {
		entry := route.(map[string]any)
		codes[entry["model"].(string)] = entry["availability"].(map[string]any)["code"]
	}
	if codes["opus"] != modelsStatusAvailable || codes["sonnet"] != modelsAvailabilityModelNotAuthorized || len(codes) != 2 {
		t.Errorf("JSON route codes = %v, want opus available and sonnet %s", codes, modelsAvailabilityModelNotAuthorized)
	}
	worker := instance["workers"].([]any)[0].(map[string]any)
	if granted, _ := worker["authorizedModels"].([]any); len(granted) != 1 || granted[0] != "opus" {
		t.Errorf("JSON workers[].authorizedModels = %v, want [opus]", worker["authorizedModels"])
	}

	scoped := scopeModelsDocument(document, modelsScope{Available: true})
	if len(scoped.Instances) != 1 {
		t.Fatalf("--available kept %d instances, want t3-primary for its opus route", len(scoped.Instances))
	}
	if kept := scoped.Instances[0].Models; len(kept) != 1 || kept[0] != "opus" {
		t.Errorf("--available kept models %v, want only opus: sonnet is advertised and not authorized", kept)
	}

	// Authorized for sonnet alone, the instance has no available route: the
	// worker advertises opus and is not authorized for it.
	workers[0].Providers[0].Models = []string{"sonnet"}
	workers[0].Snapshot.Inventory.Providers[0].Models = []string{"opus"}
	onlyOpus := buildModelsDocument("", workers, quotas, nil, time.Hour)
	if got := modelsInstanceByName(t, onlyOpus, "t3-primary").Availability.Code; got != modelsAvailabilityModelNotAuthorized {
		t.Errorf("authorized for sonnet, advertising opus: instance code %q, want %q", got, modelsAvailabilityModelNotAuthorized)
	}
	if scoped := scopeModelsDocument(onlyOpus, modelsScope{Available: true}); len(scoped.Instances) != 0 {
		t.Errorf("authorized for sonnet, advertising opus: --available kept %+v", scoped.Instances)
	}

	// A sole wildcard authorizes every concrete model, as it does for dispatch.
	workers[0].Providers[0].Models = []string{"*"}
	workers[0].Snapshot.Inventory.Providers[0].Models = []string{"opus", "sonnet"}
	wildcard := scopeModelsDocument(buildModelsDocument("", workers, quotas, nil, time.Hour), modelsScope{Available: true})
	if len(wildcard.Instances) != 1 || len(wildcard.Instances[0].Models) != 2 {
		t.Errorf("a sole wildcard kept %+v, want opus and sonnet", wildcard.Instances)
	}
}

// The authorization and the advertisement must be on the same ready worker:
// one ready worker authorized for sonnet that does not advertise it, and
// another that advertises sonnet and is authorized only for opus, together run
// no sonnet route.
func TestModelsRouteAuthorizationAndAdvertisementMeetOnOneWorker(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	pinModelsNow(t, now)
	workers, quotas := availabilityFixture(now)
	workers[0].Providers = []backlogadmin.WorkerProviderAuthorization{{
		Instance: "t3-primary", QuotaPool: "pool-claude", Models: []string{"opus", "sonnet"},
	}}
	other := workers[0]
	other.Snapshot.WorkerID = "sonnet-host"
	other.Snapshot.QuotaObservations = nil
	other.Providers = []backlogadmin.WorkerProviderAuthorization{{
		Instance: "t3-primary", QuotaPool: "pool-claude", Models: []string{"opus"},
	}}
	other.Snapshot.Inventory.Providers = []domain.WorkerProviderInventory{{
		InstanceID: "t3-primary", QuotaPoolID: "pool-claude", Available: true, Models: []string{"opus", "sonnet"},
	}}
	workers = append(workers, other)

	document := buildModelsDocument("", workers, quotas, nil, time.Hour)
	instance := modelsInstanceByName(t, document, "t3-primary")
	if got := modelsRouteAvailability(instance, "sonnet").Code; got != modelsAvailabilityModelNotAuthorized {
		t.Errorf("sonnet route code %q, want %q", got, modelsAvailabilityModelNotAuthorized)
	}
	if got := modelsRouteAvailability(instance, "opus").Code; got != modelsStatusAvailable {
		t.Errorf("opus route code %q, want available", got)
	}

	// A worker the coordinator reports authorizations for, none of which names
	// this instance, is not authorized for any of its models.
	workers[0].Snapshot.Inventory.Providers[0].Models = []string{"opus"}
	workers[1].Providers[0].Instance = "t3-other"
	workers[0].Health = string(domain.WorkerHealthDegraded)
	unlisted := buildModelsDocument("", workers, quotas, nil, time.Hour)
	if got := modelsRouteAvailability(modelsInstanceByName(t, unlisted, "t3-primary"), "sonnet").Code; got != modelsAvailabilityModelNotAuthorized {
		t.Errorf("sonnet on a worker authorized for another instance only: code %q, want %q", got, modelsAvailabilityModelNotAuthorized)
	}
	if got := modelsRouteAvailability(modelsInstanceByName(t, unlisted, "t3-primary"), "opus").Code; got != modelsAvailabilityNoReadyWorker {
		t.Errorf("opus with its only authorized worker degraded: code %q, want %q", got, modelsAvailabilityNoReadyWorker)
	}
}

// Self-review: whether the coordinator reports the per-worker catalog is a
// fact about the coordinator, not about one worker. A ready worker with no
// authorization entry at all, on a coordinator that reports entries for other
// workers, is authorized for nothing, as validatePolicyCatalog reads it.
func TestModelsWorkerWithoutAuthorizationOnAReportingCoordinatorIsRefused(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	pinModelsNow(t, now)
	workers, quotas := availabilityFixture(now)
	workers[0].Providers = []backlogadmin.WorkerProviderAuthorization{{
		Instance: "t3-primary", QuotaPool: "pool-claude", Models: []string{"opus"},
	}}
	unlisted := workers[0]
	unlisted.Providers = nil
	unlisted.Snapshot.WorkerID = "unlisted"
	unlisted.Snapshot.QuotaObservations = nil
	unlisted.Snapshot.Inventory.Providers = []domain.WorkerProviderInventory{{
		InstanceID: "t3-primary", QuotaPoolID: "pool-claude", Available: true, Models: []string{"opus", "sonnet"},
	}}
	workers = append(workers, unlisted)
	policy := &routePolicy{Roles: []policyRole{{Name: "execute",
		Candidates: []policyCandidate{{Route: "t3-primary/sonnet"}}}}}
	if err := validatePolicyCatalog(policy, workers); err == nil {
		t.Fatal("fixture unexpectedly authorizes sonnet")
	}

	document := buildModelsDocument("", workers, quotas, nil, time.Hour)
	instance := modelsInstanceByName(t, document, "t3-primary")
	if got := modelsRouteAvailability(instance, "sonnet").Code; got != modelsAvailabilityModelNotAuthorized {
		t.Errorf("sonnet offered only by an unlisted worker: code %q, want %q", got, modelsAvailabilityModelNotAuthorized)
	}
	for _, worker := range instance.Workers {
		if worker.Worker == "unlisted" && (worker.Authorized == nil || *worker.Authorized) {
			t.Errorf("the unlisted worker's row says authorized=%v, want false", worker.Authorized)
		}
	}
	// The project filter leaving out the reporting worker does not turn the
	// coordinator into one that reports nothing.
	scoped := buildModelsDocument("", workers, quotas, map[string]bool{"unlisted": true}, time.Hour)
	if got := modelsRouteAvailability(modelsInstanceByName(t, scoped, "t3-primary"), "opus").Code; got != modelsAvailabilityModelNotAuthorized {
		t.Errorf("opus with only the unlisted worker eligible: code %q, want %q", got, modelsAvailabilityModelNotAuthorized)
	}
}

// A sole wildcard in one authorization entry keeps its meaning when the same
// instance has another entry for the worker; a dropped entry authorizes
// nothing.
func TestModelsWorkerAuthorizationEntriesAreReadApart(t *testing.T) {
	worker := modelsWorker{Authorized: new(bool), grants: [][]string{{"*"}, {"opus"}}, AuthorizedModels: []string{"*", "opus"}}
	*worker.Authorized = true
	if !modelsWorkerAuthorizes(worker, "sonnet") {
		t.Error("a sole wildcard entry merged with another entry no longer authorizes sonnet")
	}
	workers, _ := availabilityFixture(time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC))
	workers[0].Providers = []backlogadmin.WorkerProviderAuthorization{
		{Instance: "t3-primary", Models: []string{"sonnet"}, Dropped: "missing binding"},
		{Instance: "t3-primary", QuotaPool: "pool-claude", Models: []string{"opus"}},
	}
	document := buildModelsDocument("", workers, nil, nil, time.Hour)
	for _, row := range modelsInstanceByName(t, document, "t3-primary").Workers {
		if modelsWorkerAuthorizes(row, "sonnet") {
			t.Errorf("a dropped entry authorizes sonnet: %+v", row)
		}
	}
}
