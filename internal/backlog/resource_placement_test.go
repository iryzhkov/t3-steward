package backlog

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
	"time"
)

func resourcePtr[T any](v T) *T { return &v }

func resourceWorker(id string) domain.WorkerInventory {
	w := placementWorker(id, "internet")
	w.CPUClass = domain.CPUClassHigh
	w.Allocatable = domain.AllocatableCapacity{ExecutorSlots: 16, CPUUnits: 32, MemoryMB: 65536, ScratchMB: 131072}
	w.Telemetry = &domain.WorkerTelemetry{
		ObservedAt: placementTestTime, CPUCount: resourcePtr(8), Load1: resourcePtr(1.0), Load5: resourcePtr(1.0),
		MemoryAvailableMB: resourcePtr(int64(16384)), SwapUsedMB: resourcePtr(int64(0)),
		WorkspaceFreeMB: resourcePtr(int64(65536)), TempFreeMB: resourcePtr(int64(65536)), RunningAttempts: resourcePtr(0),
	}
	return w
}

func TestResourcePlacementFilters(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		change     func(*domain.WorkerTelemetry)
	}{
		{"memory", "resource-memory", func(v *domain.WorkerTelemetry) { v.MemoryAvailableMB = resourcePtr(int64(2048)) }},
		{"swap", "resource-swap", func(v *domain.WorkerTelemetry) { v.SwapUsedMB = resourcePtr(int64(8192)) }},
		{"workspace", "resource-workspace-disk", func(v *domain.WorkerTelemetry) { v.WorkspaceFreeMB = resourcePtr(int64(1024)) }},
		{"temp", "resource-temp-disk", func(v *domain.WorkerTelemetry) { v.TempFreeMB = resourcePtr(int64(1024)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad, good := resourceWorker("a"), resourceWorker("b")
			tc.change(bad.Telemetry)
			task := placementTask()
			task.ResourceDemand = domain.ResourceDemand{CPUUnits: 2, MemoryMB: 4096, ScratchMB: 8192}
			s, err := SelectWorker(placementRequest(task), []domain.WorkerInventory{bad, good})
			if err != nil {
				t.Fatal(err)
			}
			if s.Decision.SelectedWorkerID != "b" {
				t.Fatalf("selection=%+v", s.Decision)
			}
			found := false
			for _, r := range s.Decision.Rejections {
				if r.WorkerID == "a" && r.Code == tc.code {
					found = true
				}
			}
			if !found {
				t.Fatalf("missing %s: %+v", tc.code, s.Decision)
			}
		})
	}
}

func TestResourcePlacementRanksBeforeLegacyScore(t *testing.T) {
	busy, idle := resourceWorker("a"), resourceWorker("b")
	busy.Telemetry.Load1 = resourcePtr(10.0)
	busy.Telemetry.Load5 = resourcePtr(10.0)
	busy.CPUClass = domain.CPUClassLow
	task := placementTask()
	s, err := SelectWorker(placementRequest(task), []domain.WorkerInventory{busy, idle})
	if err != nil || s.Decision.SelectedWorkerID != "b" {
		t.Fatalf("selection=%+v err=%v", s, err)
	}
	if len(s.Decision.ResourceEvaluations) != 2 {
		t.Fatalf("missing evidence: %+v", s)
	}
}

func TestResourcePlacementUnknownAndStale(t *testing.T) {
	for _, state := range []string{"unknown", "stale", "partial"} {
		t.Run(state, func(t *testing.T) {
			old, known := resourceWorker("a"), resourceWorker("b")
			switch state {
			case "unknown":
				old.Telemetry = nil
			case "stale":
				old.Telemetry.ObservedAt = placementTestTime.Add(-3 * time.Minute)
			case "partial":
				old.Telemetry.Load1 = nil
			}
			s, err := SelectWorker(placementRequest(placementTask()), []domain.WorkerInventory{old, known})
			if err != nil || s.Decision.SelectedWorkerID != "b" {
				t.Fatalf("selection=%+v err=%v", s, err)
			}
			for _, e := range s.Decision.ResourceEvaluations {
				if e.WorkerID == "a" && e.State != state {
					t.Fatalf("state=%s want %s", e.State, state)
				}
			}
			s, err = SelectWorker(placementRequest(placementTask()), []domain.WorkerInventory{old})
			if err != nil || s.Decision.SelectedWorkerID != "a" {
				t.Fatalf("legacy worker starved: %+v err=%v", s, err)
			}
		})
	}
}

func TestResourcePlacementBurstReservation(t *testing.T) {
	a, b := resourceWorker("a"), resourceWorker("b")
	input := []domain.WorkerInventory{a, b}
	task := placementTask()
	task.ResourceDemand = domain.ResourceDemand{CPUUnits: 4, MemoryMB: 4096, ScratchMB: 8192}
	first, err := SelectWorker(placementRequest(task), input)
	if err != nil {
		t.Fatal(err)
	}
	adjusted := reservePlacementResources(input, first.Decision.SelectedWorkerID, task.ResourceDemand)
	second, err := SelectWorker(placementRequest(task), adjusted)
	if err != nil || first.Decision.SelectedWorkerID != "a" || second.Decision.SelectedWorkerID != "b" {
		t.Fatalf("first=%+v second=%+v err=%v", first, second, err)
	}
	if *input[0].Telemetry.Load1 != 1 || *input[0].Telemetry.MemoryAvailableMB != 16384 {
		t.Fatal("mutated immutable input")
	}
}

func TestResourcePlacementConfiguredWeightsAndFreshness(t *testing.T) {
	a, b := resourceWorker("a"), resourceWorker("b")
	a.Telemetry.Load1 = resourcePtr(0.0)
	a.Telemetry.Load5 = resourcePtr(0.0)
	a.Telemetry.MemoryAvailableMB = resourcePtr(int64(4096))
	b.Telemetry.MemoryAvailableMB = resourcePtr(int64(32768))
	request := placementRequest(placementTask())
	request.ResourcePolicy = domain.DefaultResourcePlacementPolicy()
	request.ResourcePolicy.CPUWeight = 0
	s, err := SelectWorker(request, []domain.WorkerInventory{a, b})
	if err != nil || s.Decision.SelectedWorkerID != "b" {
		t.Fatalf("memory weighting: %+v err=%v", s, err)
	}
	request.ResourcePolicy.CPUWeight = 1
	request.ResourcePolicy.MemoryWeight = 0
	s, err = SelectWorker(request, []domain.WorkerInventory{a, b})
	if err != nil || s.Decision.SelectedWorkerID != "a" {
		t.Fatalf("cpu weighting: %+v err=%v", s, err)
	}
	a.Telemetry.ObservedAt = placementTestTime.Add(-30 * time.Second)
	a.Telemetry.SwapUsedMB = resourcePtr(int64(99999))
	request.ResourcePolicy.TelemetryMaxAge = 10 * time.Second
	s, err = SelectWorker(request, []domain.WorkerInventory{a})
	if err != nil || s.Decision.SelectedWorkerID != "a" || len(s.Decision.Rejections) != 0 {
		t.Fatalf("stale values filtered: %+v err=%v", s, err)
	}
	// Measurements at the bound remain fresh and hard thresholds include equality.
	a.Telemetry.ObservedAt = placementTestTime.Add(-10 * time.Second)
	a.Telemetry.SwapUsedMB = resourcePtr(request.ResourcePolicy.MaxSwapUsedMB)
	s, err = SelectWorker(request, []domain.WorkerInventory{a})
	if err != nil || s.Decision.SelectedWorkerID != "a" || s.Decision.ResourceEvaluations[0].State != "known" {
		t.Fatalf("boundary: %+v err=%v", s, err)
	}
}

func TestResourcePlacementPlanBurst(t *testing.T) {
	a, b := resourceWorker("a"), resourceWorker("b")
	a.ObservedAt = plannerTestTime
	b.ObservedAt = plannerTestTime
	a.Telemetry.ObservedAt = plannerTestTime
	b.Telemetry.ObservedAt = plannerTestTime
	alpha, beta := testTask("alpha"), testTask("beta")
	alpha.ResourceDemand = domain.ResourceDemand{CPUUnits: 4, MemoryMB: 4096, ScratchMB: 8192}
	beta.ResourceDemand = alpha.ResourceDemand
	input := plannerInput([]domain.Task{alpha, beta}, []domain.WorkerInventory{a, b})
	plan, err := BuildPlan(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Proposals) != 2 || plan.Proposals[0].WorkerID != "a" || plan.Proposals[1].WorkerID != "b" {
		t.Fatalf("burst=%+v", plan)
	}
	if *input.Workers[0].Telemetry.Load1 != 1 {
		t.Fatal("plan modified original telemetry")
	}
	// Resource reservations must also enforce hard floors in the same pass.
	a.Telemetry.MemoryAvailableMB = resourcePtr(int64(6000))
	input = plannerInput([]domain.Task{alpha, beta}, []domain.WorkerInventory{a})
	plan, err = BuildPlan(input)
	if err != nil || len(plan.Proposals) != 1 {
		t.Fatalf("memory overcommit: %+v err=%v", plan, err)
	}
}

func TestResourcePresetSizes(t *testing.T) {
	policy := domain.DefaultResourcePlacementPolicy()
	for _, tc := range []struct {
		preset       string
		cpu          float64
		memory, disk int
	}{{"build", 4, 6000, 8192}, {"light", .5, 1000, 512}, {"", 1, 1024, 0}} {
		r := ManifestResources{Preset: tc.preset}
		expandResourcePreset(&r)
		if (r.CPUUnits != nil) != (tc.preset != "") {
			t.Fatalf("preset %q expanded sizes %+v", tc.preset, r)
		}
		needs := expectedResourceNeeds(domain.Task{ResourceDemand: resourceDemandFor(r), ResourcePreset: r.Preset}, policy)
		if needs.CPUUnits != tc.cpu || needs.MemoryMB != tc.memory || needs.ScratchMB != tc.disk {
			t.Fatalf("preset %q needs = %+v", tc.preset, needs)
		}
	}
	r := ManifestResources{Preset: "build", CPUUnits: resourcePtr(.5), MemoryMB: resourcePtr(512)}
	expandResourcePreset(&r)
	needs := expectedResourceNeeds(domain.Task{ResourceDemand: resourceDemandFor(r), ResourcePreset: r.Preset}, policy)
	if needs.CPUUnits != .5 || needs.MemoryMB != 512 || needs.ScratchMB != 8192 {
		t.Fatalf("explicit needs not preferred per dimension: %+v", needs)
	}
}
