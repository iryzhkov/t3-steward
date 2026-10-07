package backlog

import (
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A preset is one table of sizes: the expansion an author reads back in the
// plan, the reservation demand on a worker that declares capacity and the live
// need the telemetry floors use are the same numbers.
func TestResourcePresetsExpandSizes(t *testing.T) {
	policy := domain.DefaultResourcePlacementPolicy()
	for _, tc := range []struct {
		preset          string
		cpu             float64
		memory, scratch int
	}{{ResourcePresetBuild, 4, 6000, 8192}, {ResourcePresetLight, .5, 1000, 512}} {
		t.Run(tc.preset, func(t *testing.T) {
			r := ManifestResources{Preset: tc.preset}
			expandResourcePreset(&r)
			if r.CPUUnits == nil || *r.CPUUnits != tc.cpu || r.MemoryMB == nil || *r.MemoryMB != tc.memory ||
				r.ScratchMB == nil || *r.ScratchMB != tc.scratch {
				t.Fatalf("preset %s expanded to %+v", tc.preset, r)
			}
			want := domain.ResourceDemand{CPUUnits: tc.cpu, MemoryMB: tc.memory, ScratchMB: tc.scratch}
			demand := resourceDemandFor(r)
			if demand.CPUUnits != want.CPUUnits || demand.MemoryMB != want.MemoryMB || demand.ScratchMB != want.ScratchMB {
				t.Fatalf("preset %s demand = %+v", tc.preset, demand)
			}
			// A task stored before presets were sized carries only the preset
			// name; its live need is still the same table.
			if needs := expectedResourceNeeds(domain.Task{ResourcePreset: tc.preset}, policy); needs != want {
				t.Fatalf("preset %s live need = %+v, want %+v", tc.preset, needs, want)
			}
		})
	}
	explicit := ManifestResources{Preset: ResourcePresetBuild, CPUUnits: resourcePtr(1.5), MemoryMB: resourcePtr(2048)}
	expandResourcePreset(&explicit)
	if *explicit.CPUUnits != 1.5 || *explicit.MemoryMB != 2048 || explicit.ScratchMB == nil || *explicit.ScratchMB != 8192 {
		t.Fatalf("explicit fields must win per dimension: %+v", explicit)
	}
}

// A task's preset sizes fill only what neither the task nor its workflow
// declared: a workflow's explicit size still wins over a task preset, and a
// task preset replaces the workflow preset's sizes.
func TestResourcePresetSizesFollowInheritance(t *testing.T) {
	manifest := mustParseManifest(t, `
version: 2
name: sizing
environment:
  project: t3-steward
resources:
  preset: light
tasks:
  inherits:
    prompt_file: prompts/inspect.md
  build:
    prompt_file: prompts/inspect.md
    resources: {preset: build}
  explicit:
    prompt_file: prompts/inspect.md
    resources: {preset: build, cpu_units: 2}
`)
	for name, want := range map[string]domain.ResourceDemand{
		"inherits": {MinCPUClass: domain.CPUClassLow, CPUUnits: .5, MemoryMB: 1000, ScratchMB: 512},
		"build":    {MinCPUClass: domain.CPUClassMedium, PreferredCPUClass: domain.CPUClassHigh, CPUUnits: 4, MemoryMB: 6000, ScratchMB: 8192},
		"explicit": {MinCPUClass: domain.CPUClassMedium, PreferredCPUClass: domain.CPUClassHigh, CPUUnits: 2, MemoryMB: 6000, ScratchMB: 8192},
	} {
		if got := resourceDemandFor(manifest.Tasks[name].Resources); got != want {
			t.Errorf("task %s demand = %+v, want %+v", name, got, want)
		}
	}
	if got := resourceDemandFor(manifest.Resources); got.CPUUnits != .5 || got.MemoryMB != 1000 {
		t.Errorf("workflow resources = %+v", got)
	}

	explicitWorkflow := mustParseManifest(t, `
version: 2
name: sizing
environment:
  project: t3-steward
resources:
  memory_mb: 3000
tasks:
  build:
    prompt_file: prompts/inspect.md
    resources: {preset: build}
`)
	if got := resourceDemandFor(explicitWorkflow.Tasks["build"].Resources); got.MemoryMB != 3000 || got.CPUUnits != 4 {
		t.Errorf("explicit workflow memory lost to the task preset: %+v", got)
	}
}

func buildTask(id string) domain.Task {
	resources := ManifestResources{Preset: ResourcePresetBuild}
	expandResourcePreset(&resources)
	task := placementTask()
	task.ID, task.Name = id, id
	task.ResourceDemand = resourceDemandFor(resources)
	task.ResourcePreset = ResourcePresetBuild
	return task
}

// A worker that declares neither cpu nor memory capacity must stay usable for
// sized work, or sized presets would make every slot-only worker ineligible
// for every build. A worker that declares cpu units is checked on them.
func TestSizedDemandOnUnsizedWorkerCountsOneSlot(t *testing.T) {
	build := buildTask("build")
	unsized := placementWorker("fleet", "internet")
	unsized.CPUClass = domain.CPUClassHigh
	unsized.Allocatable = domain.AllocatableCapacity{ExecutorSlots: 2}
	result := mustMatchWorkers(t, build, []domain.WorkerInventory{unsized})
	if len(result.EligibleWorkerIDs) != 1 {
		t.Fatalf("unsized worker refused a build: %+v", result.Evaluations)
	}
	evaluation := evaluationFor(t, result, "fleet")
	if len(evaluation.Notes) != 1 || !strings.Contains(evaluation.Notes[0], `worker "fleet" declares no cpu/memory capacity`) ||
		!strings.Contains(evaluation.Notes[0], "counted as one slot") {
		t.Fatalf("evaluation notes = %#v", evaluation.Notes)
	}

	// The reservation on the unsized worker consumes one slot and nothing else
	// it could refuse: the second build takes the second slot, a third has none.
	registry, err := NewExecutorRegistry([]domain.ExecutorPool{{WorkerID: "fleet", Name: "default", CPUClass: domain.CPUClassHigh, Allocatable: unsized.Allocatable}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"one", "two"} {
		if shortfalls := registry.Fits("fleet", build.ResourceDemand); len(shortfalls) != 0 {
			t.Fatalf("build %d on unsized worker: %+v", i, shortfalls)
		}
		if _, err := registry.Reserve(ReservationRequest{ReservationID: id, WorkerID: "fleet", Kind: ReservationKindAttempt, Demand: build.ResourceDemand, ProviderAdmissionHeld: true}); err != nil {
			t.Fatalf("reserve build %d: %v", i, err)
		}
	}
	if snapshot := registry.Snapshot("fleet"); snapshot.Reserved.ExecutorSlots != 2 {
		t.Fatalf("reserved = %+v, want two slots", snapshot.Reserved)
	}
	if shortfalls := registry.Fits("fleet", build.ResourceDemand); len(shortfalls) != 1 || shortfalls[0].Dimension != CapacityDimensionSlots {
		t.Fatalf("third build shortfalls = %+v, want slots only", shortfalls)
	}

	// A worker with cpu_units: 9 takes two builds and refuses a third on cpu
	// units, while unsized work still fits as long as slots remain.
	sized := domain.AllocatableCapacity{ExecutorSlots: 6, CPUUnits: 9}
	registry, err = NewExecutorRegistry([]domain.ExecutorPool{{WorkerID: "agent", Name: "default", CPUClass: domain.CPUClassHigh, Allocatable: sized}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"one", "two"} {
		if _, err := registry.Reserve(ReservationRequest{ReservationID: id, WorkerID: "agent", Kind: ReservationKindAttempt, Demand: build.ResourceDemand, ProviderAdmissionHeld: true}); err != nil {
			t.Fatalf("reserve build %s: %v", id, err)
		}
	}
	shortfalls := registry.Fits("agent", build.ResourceDemand)
	if len(shortfalls) != 1 || shortfalls[0].Dimension != CapacityDimensionCPUUnits || shortfalls[0].Available != 1 || shortfalls[0].Required != 4 {
		t.Fatalf("third build shortfalls = %+v, want a cpu-units shortfall of 1 against 4", shortfalls)
	}
	if shortfalls := registry.Fits("agent", domain.ResourceDemand{}); len(shortfalls) != 0 {
		t.Fatalf("unsized task refused with free slots: %+v", shortfalls)
	}
	worker := placementWorker("agent", "internet")
	worker.CPUClass = domain.CPUClassHigh
	worker.Allocatable = sized
	worker.Reserved = domain.ReservedCapacity{ExecutorSlots: 2, CPUUnits: 8, MemoryMB: 12000, ScratchMB: 16384}
	if got := evaluationCodes(t, mustMatchWorkers(t, build, []domain.WorkerInventory{worker}), "agent"); len(got) != 1 || got[0] != ExclusionCapacityExhausted {
		t.Fatalf("third build on cpu_units 9 exclusions = %#v", got)
	}
	if evaluation := evaluationFor(t, mustMatchWorkers(t, build, []domain.WorkerInventory{worker}), "agent"); len(evaluation.Notes) != 0 {
		t.Fatalf("sized worker carries the unsized note: %#v", evaluation.Notes)
	}
	if got := mustMatchWorkers(t, placementTask(), []domain.WorkerInventory{worker}); len(got.EligibleWorkerIDs) != 1 {
		t.Fatalf("unsized task refused: %+v", got.Evaluations)
	}
}

// The pressure ceiling stops new build-class work on a host whose load is
// already far beyond its cpus, and nothing else.
func TestBuildLoadCeilingExcludesOnlyBuildWork(t *testing.T) {
	busy := func() domain.WorkerInventory {
		w := resourceWorker("agent-a")
		w.Telemetry.CPUCount = resourcePtr(10)
		w.Telemetry.Load1 = resourcePtr(20.0)
		w.Telemetry.Load5 = resourcePtr(18.0)
		return w
	}
	light := placementTask()
	lightResources := ManifestResources{Preset: ResourcePresetLight}
	expandResourcePreset(&lightResources)
	light.ResourceDemand, light.ResourcePreset = resourceDemandFor(lightResources), ResourcePresetLight

	s, err := SelectWorker(placementRequest(buildTask("build")), []domain.WorkerInventory{busy()})
	if err != nil {
		t.Fatal(err)
	}
	if s.Decision.SelectedWorkerID != "" || len(s.Decision.Rejections) != 1 || s.Decision.Rejections[0].Code != ExclusionResourceCPULoad {
		t.Fatalf("build at load 20 on 10 cpus: %+v", s.Decision)
	}
	for _, want := range []string{"load 20", "10 cpus", "ceiling 1.5 per cpu"} {
		if !strings.Contains(s.Decision.Rejections[0].Detail, want) {
			t.Fatalf("detail %q does not name %q", s.Decision.Rejections[0].Detail, want)
		}
	}
	if s, err = SelectWorker(placementRequest(light), []domain.WorkerInventory{busy()}); err != nil || s.Decision.SelectedWorkerID != "agent-a" {
		t.Fatalf("light task at load 20: %+v err=%v", s.Decision, err)
	}
	// Unsized work is unaffected too.
	if s, err = SelectWorker(placementRequest(placementTask()), []domain.WorkerInventory{busy()}); err != nil || s.Decision.SelectedWorkerID != "agent-a" {
		t.Fatalf("unsized task at load 20: %+v err=%v", s.Decision, err)
	}
	stale := busy()
	stale.Telemetry.ObservedAt = placementTestTime.Add(-10 * time.Minute)
	if s, err = SelectWorker(placementRequest(buildTask("build")), []domain.WorkerInventory{stale}); err != nil || s.Decision.SelectedWorkerID != "agent-a" {
		t.Fatalf("stale telemetry excluded a build: %+v err=%v", s.Decision, err)
	}
	partial := busy()
	partial.Telemetry.CPUCount = nil
	if s, err = SelectWorker(placementRequest(buildTask("build")), []domain.WorkerInventory{partial}); err != nil || s.Decision.SelectedWorkerID != "agent-a" {
		t.Fatalf("partial telemetry excluded a build: %+v err=%v", s.Decision, err)
	}
	// At the ceiling exactly the build is still admitted.
	edge := busy()
	edge.Telemetry.Load1, edge.Telemetry.Load5 = resourcePtr(15.0), resourcePtr(15.0)
	if s, err = SelectWorker(placementRequest(buildTask("build")), []domain.WorkerInventory{edge}); err != nil || s.Decision.SelectedWorkerID != "agent-a" {
		t.Fatalf("build at the ceiling: %+v err=%v", s.Decision, err)
	}
	disabled := placementRequest(buildTask("build"))
	disabled.ResourcePolicy = domain.DefaultResourcePlacementPolicy()
	disabled.ResourcePolicy.BuildMaxLoadPerCPU = 0
	if s, err = SelectWorker(disabled, []domain.WorkerInventory{busy()}); err != nil || s.Decision.SelectedWorkerID != "agent-a" {
		t.Fatalf("disabled ceiling still excluded: %+v err=%v", s.Decision, err)
	}
}

// The ceiling acts only on complete telemetry: a fresh snapshot that reports
// cpus and load but misses any other field is partial, and partial telemetry
// never excludes build work on load.
func TestBuildLoadCeilingIgnoresPartialTelemetry(t *testing.T) {
	for name, clear := range map[string]func(*domain.WorkerTelemetry){
		"memory available": func(v *domain.WorkerTelemetry) { v.MemoryAvailableMB = nil },
		"swap used":        func(v *domain.WorkerTelemetry) { v.SwapUsedMB = nil },
		"workspace free":   func(v *domain.WorkerTelemetry) { v.WorkspaceFreeMB = nil },
		"temp free":        func(v *domain.WorkerTelemetry) { v.TempFreeMB = nil },
		"running attempts": func(v *domain.WorkerTelemetry) { v.RunningAttempts = nil },
	} {
		t.Run(name, func(t *testing.T) {
			w := resourceWorker("agent-a")
			w.Telemetry.CPUCount = resourcePtr(10)
			w.Telemetry.Load1 = resourcePtr(20.0)
			w.Telemetry.Load5 = resourcePtr(20.0)
			clear(w.Telemetry)
			evaluation, exclusions := liveResourceEvaluation(placementRequest(buildTask("build")), w)
			if evaluation.State != "partial" {
				t.Fatalf("state = %s, want partial", evaluation.State)
			}
			for _, e := range exclusions {
				if e.Code == ExclusionResourceCPULoad {
					t.Fatalf("partial telemetry excluded build: %s", e.Detail)
				}
			}
		})
	}
}

// Swap on zram is compressed memory, not paging to disk; only disk swap is a
// pressure signal when the worker reports the split.
func TestZramSwapDoesNotExclude(t *testing.T) {
	swapCodes := func(t *testing.T, w domain.WorkerInventory) []string {
		t.Helper()
		result := mustMatchWorkers(t, buildTask("build"), []domain.WorkerInventory{w})
		var codes []string
		for _, code := range evaluationCodes(t, result, w.ID) {
			if code == ExclusionResourceSwap {
				codes = append(codes, code)
			}
		}
		return codes
	}
	zram := resourceWorker("omarchy-pc")
	zram.Telemetry.SwapUsedMB = resourcePtr(int64(4300))
	zram.Telemetry.ZramSwapUsedMB = resourcePtr(int64(4300))
	if got := swapCodes(t, zram); len(got) != 0 {
		t.Fatalf("4.2 GB of zram swap excluded the worker: %v", got)
	}
	disk := resourceWorker("disk")
	disk.Telemetry.SwapUsedMB = resourcePtr(int64(5120))
	disk.Telemetry.ZramSwapUsedMB = resourcePtr(int64(0))
	if got := swapCodes(t, disk); len(got) != 1 {
		t.Fatalf("5 GB of disk swap did not exclude: %v", got)
	}
	mixed := resourceWorker("mixed")
	mixed.Telemetry.SwapUsedMB = resourcePtr(int64(9000))
	mixed.Telemetry.ZramSwapUsedMB = resourcePtr(int64(4000))
	if got := swapCodes(t, mixed); len(got) != 1 {
		t.Fatalf("5 GB of disk swap beside zram did not exclude: %v", got)
	}
	// An older worker reports no split, and all of its swap counts as before.
	older := resourceWorker("older")
	older.Telemetry.SwapUsedMB = resourcePtr(int64(4300))
	if got := swapCodes(t, older); len(got) != 1 {
		t.Fatalf("unsplit swap no longer counts: %v", got)
	}
	// The operator may count zram again.
	request := placementRequest(buildTask("build"))
	request.ResourcePolicy = domain.DefaultResourcePlacementPolicy()
	request.ResourcePolicy.SwapIgnoreZram = false
	result, err := MatchWorkers(request, []domain.WorkerInventory{zram})
	if err != nil {
		t.Fatal(err)
	}
	if got := evaluationCodes(t, result, "omarchy-pc"); len(got) != 1 || got[0] != ExclusionResourceSwap {
		t.Fatalf("zram counted on request still ignored: %v", got)
	}
}
