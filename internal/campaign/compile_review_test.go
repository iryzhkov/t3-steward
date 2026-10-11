package campaign

import (
	"errors"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
)

func TestCompileReviewResourcesAndPlacement(t *testing.T) {
	for _, tc := range []struct {
		name, front, implementHost, reviewHost, preset string
	}{
		{"defaults", "", "", "", "review"},
		{"shared", "placement: {hosts: [shared], requires: [go]}\n", "shared", "shared", "review"},
		{"implement only", "placement: {implement: {hosts: [builder], requires: [go]}}\nresources: {implement: {preset: build}}\n", "builder", "", "review"},
		{"separate", "placement: {implement: {hosts: [builder]}, review: {hosts: [reader], requires: [internet]}}\n", "builder", "reader", "review"},
		{"explicit review resources", "resources: {review: {preset: light, cpu_units: 2}}\n", "", "", "light"},
		{"empty review resources", "resources: {review: {}}\n", "", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := ParseCompilePlan("plan.md", []byte(compileRoutingHead+tc.front+compileRoutingUnits))
			if err != nil {
				t.Fatal(err)
			}
			m := compiledManifest(t, plan.Units[0])
			implement, review := m.Tasks["implement"], m.Tasks["review"]
			if strings.Join(implement.Placement.Hosts, ",") != tc.implementHost ||
				strings.Join(review.Placement.Hosts, ",") != tc.reviewHost || review.Resources.Preset != tc.preset {
				t.Fatalf("implement=%+v review=%+v", implement, review)
			}
			if tc.name == "implement only" && (len(review.Placement.Requires) != 0 || implement.Resources.Preset != "build") {
				t.Fatalf("review inherited implementation constraints: %+v", m)
			}
			if tc.name == "separate" && strings.Join(review.Placement.Requires, ",") != "internet" {
				t.Fatal(review.Placement)
			}
			if tc.name == "explicit review resources" && *review.Resources.CPUUnits != 2 {
				t.Fatal(review.Resources)
			}
			if tc.preset == "review" && (review.Resources.MinCPUClass != backlog.CPUClassLow ||
				*review.Resources.CPUUnits != 1 || *review.Resources.MemoryMB != 1500 || *review.Resources.ScratchMB != 1024) {
				t.Fatal(review.Resources)
			}
		})
	}
}

func TestCompileMixedPlacementRefusedAtPlanLine(t *testing.T) {
	for _, front := range []string{
		"placement: {hosts: [builder], review: {hosts: [reader]}}\n",
		"placement: {requires: [], implement: {}}\n",
		"placement: {hosts: [], review: null}\n",
	} {
		_, err := ParseCompilePlan("plan.md", []byte(compileRoutingHead+front+compileRoutingUnits))
		var refusal *CompilePlanError
		if !errors.As(err, &refusal) || refusal.Line != 5 || !strings.Contains(err.Error(), "mixes") {
			t.Fatalf("error=%v", err)
		}
	}
}
