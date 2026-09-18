package config

import (
	"reflect"
	"testing"
)

// A projection that adds a project with no local binding changes the catalog
// and nothing else. The coordinator's reload check compares the configuration
// outside backlog_v2 to tell a catalog change from a lifecycle change, so the
// list of defaulted projects must not leak into that comparison: on
// 2026-09-17 the first such projection was refused as "host lifecycle settings
// require restart" and the project stayed unknown until a restart.
func TestLifecycleViewIgnoresDerivedFleetState(t *testing.T) {
	before := reviewConfig()
	if err := before.ApplyCoordinatorFleet(reviewFleet()); err != nil {
		t.Fatal(err)
	}
	after := reviewConfig()
	f := reviewFleet()
	f.Projects["unbound"] = CoordinatorFleetProject{
		Repository: "ssh://git@example.invalid/unbound.git", DefaultRef: "main",
		SetupProfile: "build", EligibleWorkers: []string{"keep"},
	}
	if err := after.ApplyCoordinatorFleet(f); err != nil {
		t.Fatal(err)
	}
	if len(after.DefaultedFleetProjects()) != 1 || len(before.DefaultedFleetProjects()) != 0 {
		t.Fatalf("fixture: defaulted before=%v after=%v", before.DefaultedFleetProjects(), after.DefaultedFleetProjects())
	}
	b, a := before.LifecycleView(), after.LifecycleView()
	b.BacklogV2, a.BacklogV2 = BacklogV2{}, BacklogV2{}
	if !reflect.DeepEqual(b, a) {
		t.Fatal("a defaulted project changed the configuration outside backlog_v2")
	}
	if got := after.LifecycleView().DefaultedFleetProjects(); len(got) != 0 {
		t.Fatalf("lifecycle view still carries defaulted projects: %v", got)
	}
	if got := after.DefaultedFleetProjects(); len(got) != 1 {
		t.Fatalf("lifecycle view mutated its receiver: %v", got)
	}
}
