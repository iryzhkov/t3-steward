package config

import (
	"encoding/json"
	"reflect"
	"testing"
)

func reviewFleet() CoordinatorFleet {
	return CoordinatorFleet{Kind: "steward-coordinator-catalog-input", SchemaVersion: 1, CoordinatorID: "coord",
		Workers: map[string]CoordinatorFleetWorker{"keep": {WorkerID: "keep", CPUClass: "high", ExecutorSlots: 2,
			Capabilities: []string{"git"}, ProviderInstances: []string{"claude"}, DesiredModels: map[string][]string{"claude": {"opus"}}, QuotaPools: []string{"claude-pool"}}},
		Projects: map[string]CoordinatorFleetProject{"keep": {Repository: "ssh://git@example.invalid/new.git", DefaultRef: "main", SetupProfile: "build", EligibleWorkers: []string{"keep"}}}}
}
func reviewConfig() Config {
	c := Config{}
	c.BacklogV2.Mode = "coordinator"
	c.BacklogV2.Coordinator.ID = "coord"
	worker := V2Worker{Address: "ssh-alias", Epoch: "worker-epoch", Credential: "secretref:f02-protocol/keep", AcceptBacklog: true,
		Executors: V2Executors{Slots: 1, CPUUnits: 3.5, MemoryMB: 1234, ScratchMB: 4567}, Containment: &V2Containment{Node: "/pinned/node"},
		Providers: map[string]V2Provider{"claude": {Models: []string{"sonnet"}, QuotaPool: "claude-pool"}}}
	c.BacklogV2.Workers = map[string]V2Worker{"keep": worker, "remove": worker}
	project := V2Project{Type: "git", Repository: "ssh://git@example.invalid/old.git", DefaultRef: "old", T3Project: "canonical-t3", SetupProfile: "old",
		Workers: []string{"keep"}, Credentials: []string{"secretref:repo/read"}, ResourceLocks: []string{"preserved-lock"}}
	c.BacklogV2.Projects = map[string]V2Project{"keep": project, "remove": project}
	return c
}
func TestReviewFleetMembershipRevokesOmitted(t *testing.T) {
	c := reviewConfig()
	if err := c.ApplyCoordinatorFleet(reviewFleet()); err != nil {
		t.Fatal(err)
	}
	if len(c.BacklogV2.Workers) != 1 || len(c.BacklogV2.Projects) != 1 {
		t.Fatal("omitted membership remains authorized")
	}
}
func TestReviewFleetMetadataPreserved(t *testing.T) {
	c := reviewConfig()
	before := reviewConfig()
	f := reviewFleet()
	if err := c.ApplyCoordinatorFleet(f); err != nil {
		t.Fatal(err)
	}
	w := c.BacklogV2.Workers["keep"]
	old := before.BacklogV2.Workers["keep"]
	if w.Address != old.Address || w.Epoch != old.Epoch || w.Credential != old.Credential ||
		!reflect.DeepEqual(w.Containment, old.Containment) || w.Executors.CPUUnits != 3.5 ||
		w.Executors.MemoryMB != 1234 || w.Executors.ScratchMB != 4567 {
		t.Fatalf("transport/containment/resource metadata changed: %+v", w)
	}
	p := c.BacklogV2.Projects["keep"]
	previous := before.BacklogV2.Projects["keep"]
	if p.Type != previous.Type || p.T3Project != previous.T3Project ||
		!reflect.DeepEqual(p.Credentials, previous.Credentials) || !reflect.DeepEqual(p.ResourceLocks, previous.ResourceLocks) {
		t.Fatal("project execution metadata changed")
	}
	if w.Providers["claude"].QuotaPool != "claude-pool" || !reflect.DeepEqual(w.Providers["claude"].Models, []string{"opus"}) {
		t.Fatal("quota binding or authorization incorrect")
	}
	// Neither input aliases the installed configuration after commit.
	f.Workers["keep"].DesiredModels["claude"][0] = "mutated"
	if c.BacklogV2.Workers["keep"].Providers["claude"].Models[0] != "opus" {
		t.Fatal("authored input aliases installed model slice")
	}
}
func TestReviewFleetUnknownBindingIsAtomic(t *testing.T) {
	for _, kind := range []string{"worker", "provider", "quota", "project", "eligible"} {
		t.Run(kind, func(t *testing.T) {
			c := reviewConfig()
			before, _ := json.Marshal(c)
			f := reviewFleet()
			switch kind {
			case "worker":
				w := f.Workers["keep"]
				w.WorkerID = "unknown"
				f.Workers["unknown"] = w
			case "provider":
				w := f.Workers["keep"]
				w.ProviderInstances = []string{"unknown"}
				w.DesiredModels = map[string][]string{"unknown": {"opus"}}
				f.Workers["keep"] = w
			case "quota":
				w := f.Workers["keep"]
				w.QuotaPools = []string{"unmapped"}
				f.Workers["keep"] = w
			case "project":
				f.Projects["unknown"] = f.Projects["keep"]
			case "eligible":
				p := f.Projects["keep"]
				p.EligibleWorkers = []string{"unknown"}
				f.Projects["keep"] = p
			}
			if err := c.ApplyCoordinatorFleet(f); err == nil {
				t.Fatal("unknown binding accepted")
			}
			after, _ := json.Marshal(c)
			if string(before) != string(after) {
				t.Fatal("failed application partly mutated authorization")
			}
		})
	}
}
func TestReviewFleetEmptyAuthorization(t *testing.T) {
	for _, kind := range []string{"models", "providers", "workers", "projects"} {
		t.Run(kind, func(t *testing.T) {
			f := reviewFleet()
			switch kind {
			case "models":
				w := f.Workers["keep"]
				w.DesiredModels["claude"] = []string{}
				f.Workers["keep"] = w
			case "providers":
				w := f.Workers["keep"]
				w.ProviderInstances = []string{}
				w.DesiredModels = map[string][]string{}
				w.QuotaPools = []string{}
				f.Workers["keep"] = w
			case "workers":
				f.Workers = map[string]CoordinatorFleetWorker{}
				f.Projects = map[string]CoordinatorFleetProject{}
			case "projects":
				f.Projects = map[string]CoordinatorFleetProject{}
			}
			raw, _ := json.Marshal(f)
			decoded, err := DecodeCoordinatorFleet(raw)
			if err != nil {
				t.Fatal(err)
			}
			c := reviewConfig()
			if err := c.ApplyCoordinatorFleet(decoded); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "models":
				if len(c.BacklogV2.Workers["keep"].Providers["claude"].Models) != 0 {
					t.Fatal("models survived")
				}
			case "providers":
				if len(c.BacklogV2.Workers["keep"].Providers) != 0 {
					t.Fatal("providers survived")
				}
			case "workers":
				if len(c.BacklogV2.Workers) != 0 {
					t.Fatal("workers survived")
				}
			case "projects":
				if len(c.BacklogV2.Projects) != 0 {
					t.Fatal("projects survived")
				}
			}
		})
	}
}
func TestReviewFleetRejectsNilAndEmptyEligibility(t *testing.T) {
	for _, kind := range []string{"workers", "projects", "eligible"} {
		t.Run(kind, func(t *testing.T) {
			f := reviewFleet()
			switch kind {
			case "workers":
				f.Workers = nil
			case "projects":
				f.Projects = nil
			case "eligible":
				p := f.Projects["keep"]
				p.EligibleWorkers = []string{}
				f.Projects["keep"] = p
			}
			raw, _ := json.Marshal(f)
			decoded, err := DecodeCoordinatorFleet(raw)
			if err == nil {
				c := reviewConfig()
				err = c.ApplyCoordinatorFleet(decoded)
			}
			if err == nil {
				t.Fatal("ambiguous/missing authorization accepted")
			}
		})
	}
}
