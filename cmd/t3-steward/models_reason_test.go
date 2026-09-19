package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// modelsReasonFixture is one worker with an authorized route for each of the
// four ways a route fails, so that each reason is asserted from a fact that
// produces only that reason:
//
//	t3-primary  bound and advertised
//	codex       bound, installed on the worker and not available
//	ollama      bound, not installed on the worker at all
//	opencode    authorized by the fleet, dropped at load for want of a binding
//	gemini      authorized by the fleet with no desired models
func modelsReasonFixture() *modelsFixtureService {
	worker := backlogadmin.Worker{
		State: "observed", Enrolled: true, Health: string(domain.WorkerHealthReady),
		Snapshot: domain.WorkerSnapshot{
			WorkerID: "omarchy-pc", Connected: true,
			Inventory: domain.WorkerInventory{
				AcceptBacklog: true,
				Projects:      []domain.WorkerProjectInventory{{Name: "steward", Available: true}},
				Providers: []domain.WorkerProviderInventory{
					{InstanceID: "t3-primary", QuotaPoolID: "pool-claude", Available: true, Models: []string{"opus"}},
					{InstanceID: "codex", QuotaPoolID: "pool-codex", Available: false},
				},
			},
		},
		Providers: []backlogadmin.WorkerProviderAuthorization{
			{Instance: "codex", QuotaPool: "pool-codex", Models: []string{"gpt-5-codex"}},
			{Instance: "gemini", Dropped: config.DroppedProviderNoModels},
			{Instance: "ollama", QuotaPool: "pool-local", Models: []string{"llama"}},
			{Instance: "opencode", Dropped: config.DroppedProviderMissingBinding},
			{Instance: "t3-primary", QuotaPool: "pool-claude", Models: []string{"opus"}},
		},
	}
	pool := func(id, provider string, instances ...string) backlogadmin.Quota {
		return backlogadmin.Quota{Pool: domain.QuotaPool{
			ID: id, Provider: provider, ProviderInstanceIDs: instances,
			Admission: domain.AdmissionOpen, MaxConcurrent: 1,
		}}
	}
	return &modelsFixtureService{
		workers: []backlogadmin.Worker{worker},
		quotas: []backlogadmin.Quota{
			pool("pool-claude", "claude", "t3-primary"),
			pool("pool-codex", "codex", "codex"),
			pool("pool-local", "ollama", "ollama"),
		},
	}
}

func modelsReasonDocument(t *testing.T) modelsDocument {
	t.Helper()
	var out bytes.Buffer
	cli := modelsCLI{service: modelsReasonFixture(), principal: backlogadmin.Principal{ID: "test"}, stdout: &out}
	if err := cli.run(context.Background(), modelsScope{}, true); err != nil {
		t.Fatal(err)
	}
	var document modelsDocument
	if err := json.Unmarshal(out.Bytes(), &document); err != nil {
		t.Fatalf("models --json did not print one document: %v\n%s", err, out.String())
	}
	return document
}

// Each of the four reasons is reported for the instance and for the worker,
// because "nothing advertises it" was the whole of F-17: the fleet authorized
// opencode on two profiles, no worker offered it, and no command said whether
// the instance was missing, signed out, unbound or authorized for no model.
func TestModelsReportsWhyARouteIsNotAdvertised(t *testing.T) {
	document := modelsReasonDocument(t)
	for instance, want := range map[string]string{
		"t3-primary": "",
		"codex":      "unavailable",
		"ollama":     "not installed",
		"opencode":   "missing binding",
		"gemini":     "no models",
	} {
		item := modelsInstanceByName(t, document, instance)
		if item.Reason != want {
			t.Errorf("%s reason = %q, want %q", instance, item.Reason, want)
		}
		if len(item.Workers) != 1 || item.Workers[0].Worker != "omarchy-pc" {
			t.Fatalf("%s workers = %+v, want the one authorized worker", instance, item.Workers)
		}
		if got := item.Workers[0].Reason; got != want {
			t.Errorf("%s worker reason = %q, want %q", instance, got, want)
		}
		if advertised := item.Workers[0].Advertised; advertised != (want == "") {
			t.Errorf("%s worker advertised = %t", instance, advertised)
		}
		if authorized := item.Workers[0].Authorized; authorized == nil || !*authorized {
			t.Errorf("%s is authorized for omarchy-pc and the row does not say so", instance)
		}
	}
}

// An instance the coordinator dropped is in no quota pool, so it is invisible
// to the pool half of the join. It is listed anyway, from the worker's
// authorization, or the one state F-17 named would still be unreportable.
func TestModelsListsAnInstanceTheCoordinatorDropped(t *testing.T) {
	document := modelsReasonDocument(t)
	opencode := modelsInstanceByName(t, document, "opencode")
	if opencode.Advertised || !opencode.MissingBinding {
		t.Fatalf("opencode = %+v, want a missing binding and no advertisement", opencode)
	}
	if opencode.QuotaPool != "" || opencode.Phase != "" {
		t.Fatalf("opencode claims a pool it was never bound to: %+v", opencode)
	}
	primary := modelsInstanceByName(t, document, "t3-primary")
	if !primary.Authorized || !primary.Advertised || primary.MissingBinding || primary.Reason != "" {
		t.Fatalf("t3-primary = %+v, want a working route", primary)
	}
}

// The text form says the same thing: the status column carries the reason and
// the rows under the table name the worker each reason belongs to.
func TestModelsTextNamesTheReasonPerInstanceAndWorker(t *testing.T) {
	var out bytes.Buffer
	cli := modelsCLI{service: modelsReasonFixture(), principal: backlogadmin.Principal{ID: "test"}, stdout: &out}
	if err := cli.run(context.Background(), modelsScope{}, false); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{"t3-primary/opus", "available", "routes that cannot run:", "none"} {
		if !strings.Contains(text, want) {
			t.Errorf("models text does not carry %q:\n%s", want, text)
		}
	}
	for _, want := range [][]string{
		{"codex", "omarchy-pc", "unavailable"},
		{"ollama", "omarchy-pc", "not installed"},
		{"opencode", "omarchy-pc", "missing binding"},
		{"gemini", "omarchy-pc", "no models"},
	} {
		found := false
		for _, line := range strings.Split(text, "\n") {
			matches := true
			for _, field := range want {
				matches = matches && strings.Contains(line, field)
			}
			found = found || matches
		}
		if !found {
			t.Errorf("no line names %v:\n%s", want, text)
		}
	}
}

// A coordinator that reports no per-worker authorization at all -- a release
// older than this one, answering the same query -- still gets the table it got
// before, built from the quota pools and the inventories alone.
func TestModelsWithoutWorkerAuthorizationIsUnchanged(t *testing.T) {
	fixture := modelsReasonFixture()
	fixture.workers[0].Providers = nil
	var out bytes.Buffer
	cli := modelsCLI{service: fixture, principal: backlogadmin.Principal{ID: "test"}, stdout: &out}
	if err := cli.run(context.Background(), modelsScope{}, true); err != nil {
		t.Fatal(err)
	}
	var document modelsDocument
	if err := json.Unmarshal(out.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	if len(document.Instances) != 3 {
		t.Fatalf("instances = %+v, want only the three pooled instances", document.Instances)
	}
	for _, instance := range document.Instances {
		if instance.Reason != "" {
			t.Fatalf("%s reports a reason nothing could have told it: %q", instance.Instance, instance.Reason)
		}
		// The worker rows are the half this coordinator said nothing about, so
		// they must not answer for it. Printing "authorized": false here is the
		// mistake SetWorkerAuthorization avoids one level up: the coordinator
		// did not report that the route is unauthorized, it reported nothing.
		for _, worker := range instance.Workers {
			if worker.Authorized != nil {
				t.Fatalf("%s on %s claims authorized=%t against a coordinator that reported no authorization",
					instance.Instance, worker.Worker, *worker.Authorized)
			}
		}
	}
	// Every instance in this fixture is in a quota pool, so an "authorized":
	// false anywhere in the document can only be a worker row.
	if strings.Contains(out.String(), `"authorized": false`) {
		t.Fatalf("the document asserts an authorization the coordinator never reported:\n%s", out.String())
	}
	primary := modelsInstanceByName(t, document, "t3-primary")
	if !primary.Authorized || !primary.Advertised || len(primary.Workers) != 1 {
		t.Fatalf("t3-primary = %+v", primary)
	}
}

// modelsAdvertisedDropFixture is the live shape of opencode: the release
// authorizes the instance, the coordinator binds it to no pool and drops it,
// and the worker has it installed and is signed in to it. Its row then carries
// Advertised and a reason at the same time.
func modelsAdvertisedDropFixture() *modelsFixtureService {
	fixture := modelsReasonFixture()
	inventory := &fixture.workers[0].Snapshot.Inventory
	inventory.Providers = append(inventory.Providers, domain.WorkerProviderInventory{
		InstanceID: "opencode", QuotaPoolID: "opencode-free", Available: true,
		Models: []string{"glm-4.6"},
	})
	return fixture
}

// A worker can advertise a route the fleet does not let it run, so the second
// table is headed for what its rows mean and not for advertisement. Headed
// "not advertised by", it contradicted the table above it, which counts the
// same worker as one offering the route.
func TestModelsNamesARouteTheWorkerAdvertisesAndCannotRun(t *testing.T) {
	fixture := modelsAdvertisedDropFixture()
	var encoded bytes.Buffer
	cli := modelsCLI{service: fixture, principal: backlogadmin.Principal{ID: "test"}, stdout: &encoded}
	if err := cli.run(context.Background(), modelsScope{}, true); err != nil {
		t.Fatal(err)
	}
	var document modelsDocument
	if err := json.Unmarshal(encoded.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	opencode := modelsInstanceByName(t, document, "opencode")
	if len(opencode.Workers) != 1 {
		t.Fatalf("opencode workers = %+v, want the one worker", opencode.Workers)
	}
	if row := opencode.Workers[0]; !row.Advertised || row.Reason != config.DroppedProviderMissingBinding {
		t.Fatalf("opencode on %s = %+v, want advertised with a missing binding", row.Worker, row)
	}

	var rendered bytes.Buffer
	cli.stdout = &rendered
	if err := cli.run(context.Background(), modelsScope{}, false); err != nil {
		t.Fatal(err)
	}
	text := rendered.String()
	if strings.Contains(text, "not advertised by") {
		t.Errorf("the heading still says the listed workers do not advertise the route:\n%s", text)
	}
	if !strings.Contains(text, "routes that cannot run:") {
		t.Errorf("the second table has no heading for what its rows mean:\n%s", text)
	}
	found := false
	for _, line := range strings.Split(text, "\n") {
		found = found || (strings.Contains(line, "opencode") &&
			strings.Contains(line, "omarchy-pc") && strings.Contains(line, "missing binding"))
	}
	if !found {
		t.Errorf("no line names the advertised route the coordinator dropped:\n%s", text)
	}
	// The help describes the same rows, so it describes the same shape.
	if !strings.Contains(modelsUsage, "whose route cannot run") {
		t.Error("models --help still describes the second table as workers not offering the route")
	}
}

// The coordinator composes the per-worker authorization from the effective
// configuration: what a worker's catalog holds after the projection was
// applied, plus what that load dropped.
func TestCoordinatorWorkerAuthorizationJoinsTheCatalogAndTheDrops(t *testing.T) {
	cfg := quotaBindingCoordinator(t, false)
	authorization := coordinatorWorkerAuthorization(cfg)
	got := authorization["normandy"]
	want := []backlogadmin.WorkerProviderAuthorization{
		{Instance: "codex", QuotaPool: "codex-main", Models: []string{"test"}},
		{Instance: "opencode", Dropped: config.DroppedProviderMissingBinding},
	}
	if len(got) != len(want) {
		t.Fatalf("authorization = %+v, want %+v", got, want)
	}
	for i, entry := range got {
		if entry.Instance != want[i].Instance || entry.QuotaPool != want[i].QuotaPool ||
			entry.Dropped != want[i].Dropped || strings.Join(entry.Models, ",") != strings.Join(want[i].Models, ",") {
			t.Fatalf("authorization[%d] = %+v, want %+v", i, entry, want[i])
		}
	}
}
