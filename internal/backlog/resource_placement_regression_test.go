package backlog

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A fleet-managed worker configures executor slots only. A preset is an
// expected live need for telemetry floors and ranking, never a reservation of
// configured CPU, memory or scratch capacity, so a slot-only worker must keep
// receiving preset tasks exactly as it did before resource-aware placement.
func TestResourcePlacementSlotOnlyWorkerTakesPresetTasks(t *testing.T) {
	for _, preset := range []string{ResourcePresetLight, ResourcePresetBuild} {
		t.Run(preset, func(t *testing.T) {
			resources := ManifestResources{Preset: preset}
			expandResourcePreset(&resources)
			task := placementTask()
			task.ResourceDemand = resourceDemandFor(resources)
			if task.ResourceDemand.CPUUnits != 0 || task.ResourceDemand.MemoryMB != 0 || task.ResourceDemand.ScratchMB != 0 {
				t.Fatalf("preset %s reserves configured capacity: %+v", preset, task.ResourceDemand)
			}
			worker := resourceWorker("fleet")
			worker.Allocatable = domain.AllocatableCapacity{ExecutorSlots: 4}
			s, err := SelectWorker(placementRequest(task), []domain.WorkerInventory{worker})
			if err != nil {
				t.Fatal(err)
			}
			if s.Decision.SelectedWorkerID != "fleet" || len(s.Decision.Rejections) != 0 {
				t.Fatalf("slot-only worker refused preset %s: %+v", preset, s.Decision)
			}
		})
	}
}

// The preset's expected needs still drive live safety floors: a build task is
// refused by a worker whose live memory cannot hold build's 4096 MB plus the
// reserve, while a light task still fits there.
func TestResourcePlacementPresetNeedsDriveLiveFloors(t *testing.T) {
	for _, tc := range []struct {
		preset   string
		selected string
	}{{ResourcePresetBuild, ""}, {ResourcePresetLight, "fleet"}} {
		t.Run(tc.preset, func(t *testing.T) {
			resources := ManifestResources{Preset: tc.preset}
			expandResourcePreset(&resources)
			task := placementTask()
			task.ResourceDemand = resourceDemandFor(resources)
			worker := resourceWorker("fleet")
			worker.Allocatable = domain.AllocatableCapacity{ExecutorSlots: 4}
			worker.Telemetry.MemoryAvailableMB = resourcePtr(int64(3072))
			s, err := SelectWorker(placementRequest(task), []domain.WorkerInventory{worker})
			if err != nil {
				t.Fatal(err)
			}
			if s.Decision.SelectedWorkerID != tc.selected {
				t.Fatalf("preset %s: %+v", tc.preset, s.Decision)
			}
		})
	}
}

// Unsized tasks are the default task shape. Within one planning cycle each
// proposal must reserve a nominal per-attempt cost, so a burst spreads across
// workers instead of filling the one that looked idlest at the snapshot.
func TestResourcePlacementPlanBurstUnsized(t *testing.T) {
	a, b := resourceWorker("a"), resourceWorker("b")
	for _, w := range []*domain.WorkerInventory{&a, &b} {
		w.ObservedAt = plannerTestTime
		w.Telemetry.ObservedAt = plannerTestTime
	}
	b.Telemetry.Load1 = resourcePtr(2.0)
	b.Telemetry.Load5 = resourcePtr(2.0)
	tasks := []domain.Task{testTask("t1"), testTask("t2"), testTask("t3"), testTask("t4")}
	plan, err := BuildPlan(plannerInput(tasks, []domain.WorkerInventory{a, b}))
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, proposal := range plan.Proposals {
		counts[proposal.WorkerID]++
	}
	if len(plan.Proposals) != 4 || counts["a"] != 2 || counts["b"] != 2 {
		t.Fatalf("unsized burst did not spread: %v", counts)
	}
}

// A worker clock a little ahead of the coordinator must not make fresh
// telemetry stale; readings dated beyond the freshness bound still are.
func TestResourcePlacementToleratesWorkerClockSkew(t *testing.T) {
	worker := resourceWorker("a")
	worker.Telemetry.ObservedAt = placementTestTime.Add(30 * time.Second)
	s, err := SelectWorker(placementRequest(placementTask()), []domain.WorkerInventory{worker})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Decision.ResourceEvaluations[0].State; got != "known" {
		t.Fatalf("skewed telemetry state = %q", got)
	}
	worker.Telemetry.ObservedAt = placementTestTime.Add(domain.DefaultResourcePlacementPolicy().TelemetryMaxAge + time.Second)
	s, err = SelectWorker(placementRequest(placementTask()), []domain.WorkerInventory{worker})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Decision.ResourceEvaluations[0].State; got != "stale" {
		t.Fatalf("far-future telemetry state = %q", got)
	}
}
