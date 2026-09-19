package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// C-6. models filtered only by project and printed all thirty-six routes
// otherwise. --instance and --available are the two questions it could not be
// asked, and both of them say what to read: they are applied to the document
// before it is rendered or encoded, so a narrowed answer is the whole of a
// smaller question and never a window onto a larger one.
//
// The document is read as JSON rather than through its Go type on purpose:
// this file has to compile against the commit before the fix, so that its
// failure there is the missing behaviour and not a missing field.
func runModelsScoped(t *testing.T, args ...string) string {
	t.Helper()
	scope, asJSON, err := parseModelsArgs(args)
	if err != nil {
		t.Fatalf("t3-steward models %s was refused: %v", strings.Join(args, " "), err)
	}
	var out bytes.Buffer
	cli := modelsCLI{service: modelsFixture(), principal: backlogadmin.Principal{ID: "test"}, stdout: &out}
	if err := cli.run(context.Background(), scope, asJSON); err != nil {
		t.Fatalf("t3-steward models %s: %v", strings.Join(args, " "), err)
	}
	return out.String()
}

func modelsDocumentKeys(t *testing.T, encoded string) map[string]any {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal([]byte(encoded), &document); err != nil {
		t.Fatalf("models --json did not print one document: %v\n%s", err, encoded)
	}
	return document
}

func modelsInstanceNames(t *testing.T, document map[string]any) []string {
	t.Helper()
	instances, ok := document["instances"].([]any)
	if !ok {
		t.Fatalf("the document has no instances: %+v", document)
	}
	var names []string
	for _, item := range instances {
		instance, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("instance %+v is not an object", item)
		}
		names = append(names, instance["instance"].(string))
	}
	return names
}

func TestModelsScopesTheDocumentBeforeItIsPrinted(t *testing.T) {
	// t3-primary sorts last of the three instances, so an answer that held it
	// alone could not have come from capping the list.
	document := modelsDocumentKeys(t, runModelsScoped(t, "--instance", "t3-primary", "--json"))
	if names := modelsInstanceNames(t, document); len(names) != 1 || names[0] != "t3-primary" {
		t.Fatalf("--instance t3-primary returned %v", names)
	}
	// The scope is recorded with the sizes it narrowed, so the document says
	// which question it answers rather than looking like a fleet of one.
	if document["instance"] != "t3-primary" {
		t.Errorf("the document does not echo the instance it was scoped to: %+v", document)
	}
	if document["totalInstances"] != float64(3) || document["totalRoutes"] != float64(4) {
		t.Errorf("the document does not report the sizes before the scope: %+v", document)
	}

	// --available keeps the routes that can run now, which is the one
	// instance of the three whose status column says available.
	available := modelsDocumentKeys(t, runModelsScoped(t, "--available", "--json"))
	if names := modelsInstanceNames(t, available); len(names) != 1 || names[0] != "t3-primary" {
		t.Fatalf("--available returned %v, want the routes that can run", names)
	}
	if available["available"] != true || available["totalInstances"] != float64(3) {
		t.Errorf("the document does not report the scope and the size before it: %+v", available)
	}

	// The text form says it narrowed, gives the totals and says how to undo it.
	text := runModelsScoped(t, "--instance", "t3-primary")
	for _, want := range []string{"t3-primary/opus", "showing 2 of 4 routes", "1 of 3 provider instances", "--instance t3-primary is in force"} {
		if !strings.Contains(text, want) {
			t.Errorf("the narrowed table does not say %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "opencode/grok-code") {
		t.Errorf("--instance t3-primary printed another instance's route:\n%s", text)
	}

	// An empty narrowed answer is not an empty fleet.
	empty := runModelsScoped(t, "--instance", "typo")
	for _, want := range []string{"no provider route matches --instance typo", "3 instances"} {
		if !strings.Contains(empty, want) {
			t.Errorf("the empty narrowed answer does not say %q:\n%s", want, empty)
		}
	}

	// Unnarrowed, the document still reports its own size, so a reader never
	// has to know whether a filter was in force to trust the counts.
	whole := modelsDocumentKeys(t, runModelsScoped(t, "--json"))
	if whole["totalInstances"] != float64(3) || whole["totalRoutes"] != float64(4) {
		t.Errorf("the unnarrowed document does not report its size: %+v", whole)
	}
	if _, narrowed := whole["instance"]; narrowed {
		t.Errorf("the unnarrowed document claims a scope: %+v", whole)
	}
}

func TestParseModelsArgumentsRefusesAValuelessFilter(t *testing.T) {
	if _, _, err := parseModelsArgs([]string{"--instance"}); err == nil {
		t.Error("--instance without a value was accepted")
	}
	if _, _, err := parseModelsArgs([]string{"--available", "extra"}); err == nil {
		t.Error("a positional argument after --available was accepted")
	}
}

// modelsFleetFixture is the fleet the audit measured models against: thirty-six
// authorised routes over four provider instances, advertised by two workers.
// The behaviour above is asserted on the small fixture; this one exists so
// that the size the finding quotes is measured on a comparable answer.
func modelsFleetFixture() *modelsFixtureService {
	provider := map[string][]string{}
	var pools []backlogadmin.Quota
	for _, route := range catalogRoutes() {
		provider[route.Instance] = append(provider[route.Instance], route.Model)
	}
	var inventories []domain.WorkerProviderInventory
	for _, route := range catalogRoutes() {
		if len(provider[route.Instance]) == 0 {
			continue
		}
		models := provider[route.Instance]
		provider[route.Instance] = nil
		inventories = append(inventories, domain.WorkerProviderInventory{
			InstanceID: route.Instance, QuotaPoolID: route.QuotaPool, Available: true, Models: models,
		})
		pools = append(pools, backlogadmin.Quota{Pool: domain.QuotaPool{
			ID: route.QuotaPool, ProviderInstanceIDs: []string{route.Instance},
			Admission: domain.AdmissionOpen, MaxConcurrent: 2,
		}})
	}
	worker := func(id string) backlogadmin.Worker {
		return backlogadmin.Worker{
			State: "observed", Enrolled: true, Health: string(domain.WorkerHealthReady),
			Snapshot: domain.WorkerSnapshot{WorkerID: id, Connected: true, Inventory: domain.WorkerInventory{
				AcceptBacklog: true, Providers: inventories,
			}},
		}
	}
	return &modelsFixtureService{
		workers: []backlogadmin.Worker{worker("omarchy-pc"), worker("normandy")},
		quotas:  pools,
	}
}

// TestModelsMeasured records the sizes this finding is closed with. It asserts
// nothing: it exists so that "go test -run Measured -v" prints the same
// numbers on any commit, including the base.
func TestModelsMeasured(t *testing.T) {
	measure := func(args ...string) int {
		t.Helper()
		scope, asJSON, err := parseModelsArgs(args)
		if err != nil {
			t.Fatalf("t3-steward models %s: %v", strings.Join(args, " "), err)
		}
		var out bytes.Buffer
		cli := modelsCLI{service: modelsFleetFixture(), principal: backlogadmin.Principal{ID: "test"}, stdout: &out}
		if err := cli.run(context.Background(), scope, asJSON); err != nil {
			t.Fatalf("t3-steward models %s: %v", strings.Join(args, " "), err)
		}
		return out.Len()
	}
	t.Logf("models                       text %d bytes", measure())
	t.Logf("models --json                %d bytes", measure("--json"))
	// The narrowed forms only exist on one side of this fix, so they are
	// measured only where they parse.
	if _, _, err := parseModelsArgs([]string{"--instance", "t3-primary"}); err == nil {
		t.Logf("models --instance t3-primary text %d bytes", measure("--instance", "t3-primary"))
		t.Logf("models --available           text %d bytes", measure("--available"))
	}
}
