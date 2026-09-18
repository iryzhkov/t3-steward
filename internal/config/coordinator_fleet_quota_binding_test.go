package config

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

// bindingFleet is reviewFleet with a second provider instance that the
// operator file does not bind at all. It is the real case F-17 named: a
// provider registered by one release edit, where backlog_v2.workers.keep
// .providers carries only the instance somebody bound by hand.
func bindingFleet() CoordinatorFleet {
	fleet := reviewFleet()
	worker := fleet.Workers["keep"]
	worker.ProviderInstances = []string{"claude", "opencode"}
	worker.DesiredModels = map[string][]string{"claude": {"opus"}, "opencode": {"glm-4.6"}}
	worker.QuotaPools = []string{"claude-pool", "opencode-pool"}
	worker.QuotaBindings = map[string]string{"opencode": "opencode-pool"}
	fleet.Workers["keep"] = worker
	return fleet
}

// bindingConfig is reviewConfig with both quota pools defined, which is what
// an operator file that can charge either pool looks like. The pool
// definition (its provider and concurrency) stays operator configuration; the
// projection binds an instance to one of them.
func bindingConfig() Config {
	c := reviewConfig()
	c.BacklogV2.QuotaPools = map[string]V2QuotaPool{
		"claude-pool":   {Provider: "claude", MaxConcurrent: 1},
		"opencode-pool": {Provider: "opencode", MaxConcurrent: 1},
	}
	return c
}

// droppedProviderNames reports the worker, instance and reason of each drop,
// so an assertion does not have to repeat the remedy sentence word for word.
func droppedProviderNames(dropped []DroppedFleetProvider) []DroppedFleetProvider {
	names := make([]DroppedFleetProvider, 0, len(dropped))
	for _, item := range dropped {
		item.Remedy = ""
		names = append(names, item)
	}
	return names
}

// The projected binding authorizes an instance the coordinator's own file
// binds nowhere, which is what makes registering a provider one release edit.
func TestProjectedQuotaBindingAuthorizesAnInstanceTheOperatorFileDoesNotBind(t *testing.T) {
	c := bindingConfig()
	if err := c.ApplyCoordinatorFleet(bindingFleet()); err != nil {
		t.Fatalf("a projected quota binding was refused: %v", err)
	}
	providers := c.BacklogV2.Workers["keep"].Providers
	opencode, ok := providers["opencode"]
	if !ok {
		t.Fatalf("the projected binding did not authorize the instance: %+v", providers)
	}
	if opencode.QuotaPool != "opencode-pool" {
		t.Fatalf("opencode quota pool = %q, want the projected one", opencode.QuotaPool)
	}
	if !reflect.DeepEqual(opencode.Models, []string{"glm-4.6"}) {
		t.Fatalf("opencode models = %v, want the desired models", opencode.Models)
	}
	if providers["claude"].QuotaPool != "claude-pool" {
		t.Fatalf("the instance bound in the operator file changed: %+v", providers["claude"])
	}
	if dropped := c.DroppedFleetProviders(); len(dropped) != 0 {
		t.Fatalf("dropped = %+v, want nothing dropped", dropped)
	}
}

// The coordinator's own explicit binding still wins: the projection may grant
// authorization the operator file does not carry, never move a binding the
// operator wrote.
func TestExplicitQuotaBindingWinsOverTheProjectedOne(t *testing.T) {
	fleet := bindingFleet()
	worker := fleet.Workers["keep"]
	worker.QuotaBindings["claude"] = "opencode-pool"
	fleet.Workers["keep"] = worker
	c := bindingConfig()
	if err := c.ApplyCoordinatorFleet(fleet); err != nil {
		t.Fatal(err)
	}
	if pool := c.BacklogV2.Workers["keep"].Providers["claude"].QuotaPool; pool != "claude-pool" {
		t.Fatalf("claude quota pool = %q, want the explicit backlog_v2 binding to win", pool)
	}
}

// An instance with desired models and no binding from either source no longer
// fails the whole configuration: it is dropped for that worker and recorded,
// which is the S-1 pattern. Before this change the load returned "fleet worker
// "keep" provider "opencode" needs an explicit authorized quota binding" and
// with it took every worker, project and admin query on the coordinator down.
func TestInstanceWithNoQuotaBindingIsDroppedForThatWorkerAndRecorded(t *testing.T) {
	fleet := bindingFleet()
	worker := fleet.Workers["keep"]
	worker.QuotaBindings = nil
	fleet.Workers["keep"] = worker
	c := bindingConfig()
	if err := c.ApplyCoordinatorFleet(fleet); err != nil {
		t.Fatalf("an unbound provider instance failed the whole configuration: %v", err)
	}
	providers := c.BacklogV2.Workers["keep"].Providers
	if _, ok := providers["opencode"]; ok {
		t.Fatalf("the unbound instance was authorized anyway: %+v", providers)
	}
	if providers["claude"].QuotaPool != "claude-pool" {
		t.Fatalf("the rest of the worker did not load: %+v", providers)
	}
	if len(c.BacklogV2.Projects) != 1 {
		t.Fatalf("projects = %+v, want the rest of the configuration loaded", c.BacklogV2.Projects)
	}
	want := []DroppedFleetProvider{{Worker: "keep", Instance: "opencode", Reason: DroppedProviderMissingBinding}}
	if got := droppedProviderNames(c.DroppedFleetProviders()); !reflect.DeepEqual(got, want) {
		t.Fatalf("dropped = %+v, want %+v", got, want)
	}
	if remedy := c.DroppedFleetProviders()[0].Remedy; !strings.Contains(remedy, "upkeeper provider add") ||
		!strings.Contains(remedy, "backlog_v2.workers.keep.providers.opencode.quota_pool") {
		t.Fatalf("remedy = %q, want both ways to bind it", remedy)
	}
	// The list is a copy: a caller cannot alter what the configuration reports.
	c.DroppedFleetProviders()[0].Instance = "mutated"
	if c.DroppedFleetProviders()[0].Instance != "opencode" {
		t.Fatal("DroppedFleetProviders aliases internal state")
	}
}

// A projected binding to a pool the same projection does not authorize for
// that worker is no binding at all. It is dropped rather than accepted,
// because the pool list is what says which pools the worker may draw from.
func TestProjectedBindingToAnUnauthorizedPoolIsDropped(t *testing.T) {
	fleet := bindingFleet()
	worker := fleet.Workers["keep"]
	worker.QuotaBindings = map[string]string{"opencode": "unmapped"}
	fleet.Workers["keep"] = worker
	c := bindingConfig()
	if err := c.ApplyCoordinatorFleet(fleet); err != nil {
		t.Fatalf("an unauthorized projected pool failed the whole configuration: %v", err)
	}
	if _, ok := c.BacklogV2.Workers["keep"].Providers["opencode"]; ok {
		t.Fatal("a binding to a pool the worker does not join was accepted")
	}
	if got := c.DroppedFleetProviders(); len(got) != 1 || got[0].Reason != DroppedProviderMissingBinding {
		t.Fatalf("dropped = %+v, want one missing binding", got)
	}
}

// An explicit local binding to a pool the projection does not authorize is
// the operator's own file contradicting the fleet, not a missing binding, and
// still fails the whole configuration exactly as before.
func TestExplicitBindingToAnUnauthorizedPoolStillFailsTheConfiguration(t *testing.T) {
	fleet := bindingFleet()
	worker := fleet.Workers["keep"]
	worker.QuotaPools = []string{"opencode-pool"}
	fleet.Workers["keep"] = worker
	c := bindingConfig()
	err := c.ApplyCoordinatorFleet(fleet)
	if err == nil {
		t.Fatal("an explicit binding to an unauthorized pool was accepted")
	}
	if !strings.Contains(err.Error(), "claude") {
		t.Fatalf("the refusal does not name the instance: %v", err)
	}
}

// An instance the projection authorizes with no desired models grants no
// execution authorization, is skipped as before, and is recorded so that
// "t3-steward models" can say which of the four reasons applies.
func TestInstanceWithNoDesiredModelsIsSkippedAndRecorded(t *testing.T) {
	fleet := bindingFleet()
	worker := fleet.Workers["keep"]
	worker.DesiredModels["opencode"] = []string{}
	fleet.Workers["keep"] = worker
	c := bindingConfig()
	if err := c.ApplyCoordinatorFleet(fleet); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.BacklogV2.Workers["keep"].Providers["opencode"]; ok {
		t.Fatal("an instance with no desired models was authorized")
	}
	want := []DroppedFleetProvider{{Worker: "keep", Instance: "opencode", Reason: DroppedProviderNoModels}}
	if got := c.DroppedFleetProviders(); !reflect.DeepEqual(got, want) {
		t.Fatalf("dropped = %+v, want %+v", got, want)
	}
	if remedy := c.DroppedFleetProviders()[0].Remedy; remedy != "" {
		t.Fatalf("an instance nobody asked for models from carries a remedy: %q", remedy)
	}
}

// The pool itself stays operator configuration: backlog_v2.quota_pools
// defines its provider and concurrency. A binding to a pool this coordinator
// does not define is dropped with a remedy that names the file, rather than
// applied and then failing the whole configuration in validation.
func TestProjectedBindingToAPoolThisCoordinatorDoesNotDefineIsDropped(t *testing.T) {
	c := reviewConfig()
	c.BacklogV2.QuotaPools = map[string]V2QuotaPool{"claude-pool": {Provider: "claude", MaxConcurrent: 1}}
	if err := c.ApplyCoordinatorFleet(bindingFleet()); err != nil {
		t.Fatalf("a binding to an undefined pool failed the whole configuration: %v", err)
	}
	if _, ok := c.BacklogV2.Workers["keep"].Providers["opencode"]; ok {
		t.Fatal("an instance was bound to a quota pool this coordinator does not define")
	}
	dropped := c.DroppedFleetProviders()
	if len(dropped) != 1 || dropped[0].Reason != DroppedProviderMissingBinding {
		t.Fatalf("dropped = %+v, want one missing binding", dropped)
	}
	if !strings.Contains(dropped[0].Remedy, "backlog_v2.quota_pools") {
		t.Fatalf("remedy = %q, want it to name the file that defines the pool", dropped[0].Remedy)
	}
}

// A projection written before the field existed carries no quota_bindings key
// and loads exactly as it did: a mixed-version fleet is the normal state
// during a release, and this coordinator meets both documents.
func TestProjectionWithoutQuotaBindingsLoadsAsBefore(t *testing.T) {
	raw, err := json.Marshal(reviewFleet())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("quota_bindings")) {
		t.Fatalf("a projection that binds nothing encodes the key anyway: %s", raw)
	}
	decoded, err := DecodeCoordinatorFleet(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Workers["keep"].QuotaBindings != nil {
		t.Fatalf("bindings = %v, want none", decoded.Workers["keep"].QuotaBindings)
	}
	c := bindingConfig()
	if err := c.ApplyCoordinatorFleet(decoded); err != nil {
		t.Fatal(err)
	}
	if c.BacklogV2.Workers["keep"].Providers["claude"].QuotaPool != "claude-pool" {
		t.Fatal("the explicit binding stopped working without the new field")
	}
	if dropped := c.DroppedFleetProviders(); len(dropped) != 0 {
		t.Fatalf("dropped = %+v, want nothing dropped", dropped)
	}
	// The shipped projection fixture is such a document, and still decodes.
	fixture, err := os.ReadFile("../../testdata/fleet/projection-coordinator.json")
	if err != nil {
		t.Fatal(err)
	}
	older, err := DecodeCoordinatorFleet(fixture)
	if err != nil {
		t.Fatalf("the pre-binding projection fixture was refused: %v", err)
	}
	for name, worker := range older.Workers {
		if worker.QuotaBindings != nil {
			t.Fatalf("worker %q invented bindings: %v", name, worker.QuotaBindings)
		}
	}
}

// The field is closed like every other: a binding names an instance the worker
// runs and a pool it joins, or the projection is refused before it is applied.
func TestDecodeCoordinatorFleetValidatesQuotaBindings(t *testing.T) {
	for _, tc := range []struct {
		name     string
		bindings map[string]string
		want     string
	}{
		{"an instance the worker does not run", map[string]string{"gemini": "claude-pool"}, "gemini"},
		{"a pool the worker does not join", map[string]string{"opencode": "free"}, "free"},
		{"an empty pool", map[string]string{"opencode": ""}, "opencode"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fleet := bindingFleet()
			worker := fleet.Workers["keep"]
			worker.QuotaBindings = tc.bindings
			fleet.Workers["keep"] = worker
			raw, err := json.Marshal(fleet)
			if err != nil {
				t.Fatal(err)
			}
			_, err = DecodeCoordinatorFleet(raw)
			if err == nil {
				t.Fatal("a binding nobody can act on was accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("the refusal does not name %q: %v", tc.want, err)
			}
		})
	}
}

// The bytes UpKeeper renders for a bound worker decode into the binding this
// coordinator uses, with no key for a worker that binds nothing.
func TestUpKeeperCoordinatorProjectionQuotaBindingFixture(t *testing.T) {
	raw, err := os.ReadFile("../../testdata/fleet/projection-coordinator-quota-binding.json")
	if err != nil {
		t.Fatal(err)
	}
	fleet, err := DecodeCoordinatorFleet(raw)
	if err != nil {
		t.Fatalf("the rendered projection was refused: %v", err)
	}
	want := map[string]string{"opencode": "default"}
	if !reflect.DeepEqual(fleet.Workers["omarchy-pc"].QuotaBindings, want) {
		t.Fatalf("omarchy-pc bindings = %v, want %v", fleet.Workers["omarchy-pc"].QuotaBindings, want)
	}
	if fleet.Workers["homelab"].QuotaBindings != nil {
		t.Fatalf("homelab bindings = %v, want none", fleet.Workers["homelab"].QuotaBindings)
	}
}

// A projection that drops an instance changes the catalog and nothing else.
// The coordinator's reload check compares the configuration outside
// backlog_v2, so the dropped list must not leak into that comparison, or the
// first projection that added a binding would be refused as "host lifecycle
// settings require restart" -- which is what the defaulted project list did.
func TestLifecycleViewIgnoresDroppedFleetProviders(t *testing.T) {
	unbound := bindingFleet()
	worker := unbound.Workers["keep"]
	worker.QuotaBindings = nil
	unbound.Workers["keep"] = worker
	before := bindingConfig()
	if err := before.ApplyCoordinatorFleet(unbound); err != nil {
		t.Fatal(err)
	}
	after := bindingConfig()
	if err := after.ApplyCoordinatorFleet(bindingFleet()); err != nil {
		t.Fatal(err)
	}
	if len(before.DroppedFleetProviders()) != 1 || len(after.DroppedFleetProviders()) != 0 {
		t.Fatalf("fixture: dropped before=%+v after=%+v",
			before.DroppedFleetProviders(), after.DroppedFleetProviders())
	}
	b, a := before.LifecycleView(), after.LifecycleView()
	b.BacklogV2, a.BacklogV2 = BacklogV2{}, BacklogV2{}
	if !reflect.DeepEqual(b, a) {
		t.Fatal("a dropped provider instance changed the configuration outside backlog_v2")
	}
	if got := before.LifecycleView().DroppedFleetProviders(); len(got) != 0 {
		t.Fatalf("lifecycle view still carries dropped providers: %+v", got)
	}
	if got := before.DroppedFleetProviders(); len(got) != 1 {
		t.Fatalf("lifecycle view mutated its receiver: %+v", got)
	}
}
