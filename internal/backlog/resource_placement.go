package backlog

import (
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"math"
	"sort"
)

// Live-telemetry exclusion codes. They are temporary resource pressure, never
// a statement about configured capacity.
const (
	ExclusionResourceMemory        = "resource-memory"
	ExclusionResourceSwap          = "resource-swap"
	ExclusionResourceWorkspaceDisk = "resource-workspace-disk"
	ExclusionResourceTempDisk      = "resource-temp-disk"
)

// expectedResourceNeeds is the live use one attempt is expected to add to a
// worker. Explicit sizes win per dimension; a task ingested since presets were
// sized carries its preset's sizes as explicit ones. Otherwise the declared
// preset selects its sizes from presetSizes, whatever CPU classes the task
// overrode, because a class says nothing about memory or scratch. Any other
// task, including one that declares a preset's classes without the preset,
// uses the policy's nominal unsized needs, so a burst of unsized tasks still
// spreads and a bare class floor is not mistaken for a build.
func expectedResourceNeeds(task domain.Task, p domain.ResourcePlacementPolicy) domain.ResourceDemand {
	demand := task.ResourceDemand
	needs := domain.ResourceDemand{CPUUnits: p.UnsizedTaskCPUUnits, MemoryMB: p.UnsizedTaskMemoryMB, ScratchMB: p.UnsizedTaskScratchMB}
	if sizes, ok := presetSizes[task.ResourcePreset]; ok {
		needs = sizes
	}
	if demand.CPUUnits > 0 {
		needs.CPUUnits = demand.CPUUnits
	}
	if demand.MemoryMB > 0 {
		needs.MemoryMB = demand.MemoryMB
	}
	if demand.ScratchMB > 0 {
		needs.ScratchMB = demand.ScratchMB
	}
	return needs
}

func liveResourceEvaluation(request WorkerPlacementRequest, worker domain.WorkerInventory) (domain.ResourceEvaluation, []WorkerExclusion) {
	p := request.ResourcePolicy.WithDefaults()
	e := domain.ResourceEvaluation{WorkerID: worker.ID, Telemetry: worker.Telemetry.Clone(), State: "unknown"}
	v := e.Telemetry
	if v == nil {
		return e, nil
	}
	// A worker clock may lead the coordinator's; tolerate a lead up to the same
	// bound as the age, as snapshot freshness tolerates future timestamps.
	age := request.Now.Sub(v.ObservedAt)
	if v.ObservedAt.IsZero() || age < -p.TelemetryMaxAge || age > p.TelemetryMaxAge {
		e.State = "stale"
		return e, nil
	}
	validSize := func(x *int64) bool { return x != nil && *x >= 0 }
	validLoad := func(x *float64) bool { return x != nil && *x >= 0 && !math.IsNaN(*x) && !math.IsInf(*x, 0) }
	knownCPU := v.CPUCount != nil && *v.CPUCount > 0 && validLoad(v.Load1) && validLoad(v.Load5)
	e.State = "partial"
	if knownCPU && validSize(v.MemoryAvailableMB) && validSize(v.SwapUsedMB) && validSize(v.WorkspaceFreeMB) && validSize(v.TempFreeMB) && v.RunningAttempts != nil && *v.RunningAttempts >= 0 {
		e.State = "known"
	}
	var exclusions []WorkerExclusion
	addFloor := func(value *int64, need int, reserve int64, code, dimension string) {
		if validSize(value) && float64(*value) < float64(need)+float64(reserve) {
			exclusions = append(exclusions, WorkerExclusion{Code: code, Detail: fmt.Sprintf("%s available %d MB is below task need %d MB plus reserve %d MB", dimension, *value, need, reserve)})
		}
	}
	demand := expectedResourceNeeds(request.Task, p)
	addFloor(v.MemoryAvailableMB, demand.MemoryMB, p.MemoryReserveMB, ExclusionResourceMemory, "memory")
	addFloor(v.WorkspaceFreeMB, demand.ScratchMB, p.DiskReserveMB, ExclusionResourceWorkspaceDisk, "workspace disk")
	addFloor(v.TempFreeMB, demand.ScratchMB, p.DiskReserveMB, ExclusionResourceTempDisk, "temp disk")
	if swap, kind := countedSwapMB(v, p); swap > p.MaxSwapUsedMB {
		exclusions = append(exclusions, WorkerExclusion{Code: ExclusionResourceSwap, Detail: fmt.Sprintf("%s used %d MB exceeds limit %d MB", kind, swap, p.MaxSwapUsedMB)})
	}
	if knownCPU {
		cores := float64(*v.CPUCount)
		e.CPUHeadroom = math.Max(-1, math.Min(1, (cores-math.Max(*v.Load1, *v.Load5)-demand.CPUUnits)/cores))
		if exclusion, over := buildLoadCeiling(*v.CPUCount, math.Max(*v.Load1, *v.Load5), demand, p); over {
			exclusions = append(exclusions, exclusion)
		}
	}
	if validSize(v.MemoryAvailableMB) {
		available := float64(*v.MemoryAvailableMB)
		required := float64(demand.MemoryMB) + float64(p.MemoryReserveMB)
		if available+required > 0 {
			e.MemoryHeadroom = (available - required) / (available + required)
		}
	}
	e.Score = p.CPUWeight*e.CPUHeadroom + p.MemoryWeight*e.MemoryHeadroom
	return e, exclusions
}

// ResourceRankLess orders resource evidence before the existing worker score.
// Partial and absent observations share the conservative fallback tier.
func ResourceRankLess(a, b domain.ResourceEvaluation) bool {
	if (a.State == "known") != (b.State == "known") {
		return a.State == "known"
	}
	return a.State == "known" && a.Score > b.Score
}

func placementBetter(decision domain.PlacementDecision, candidate, incumbent string) bool {
	var a, b domain.ResourceEvaluation
	for _, e := range decision.ResourceEvaluations {
		if e.WorkerID == candidate {
			a = e
		}
		if e.WorkerID == incumbent {
			b = e
		}
	}
	if ResourceRankLess(a, b) {
		return true
	}
	if ResourceRankLess(b, a) {
		return false
	}
	return scoreFor(decision.Scores, candidate).Total > scoreFor(decision.Scores, incumbent).Total
}

func rankResourceEvaluations(decision *domain.PlacementDecision) {
	ids := make([]string, 0, len(decision.Scores))
	for _, s := range decision.Scores {
		ids = append(ids, s.WorkerID)
	}
	sort.SliceStable(ids, func(i, j int) bool { return placementBetter(*decision, ids[i], ids[j]) })
	for i, id := range ids {
		for j := range decision.ResourceEvaluations {
			if decision.ResourceEvaluations[j].WorkerID == id {
				decision.ResourceEvaluations[j].Rank = i + 1
			}
		}
	}
}

// reservePlacementResources adds planned expected needs to a private snapshot.
// Live metrics already account for running tasks; only newly proposed work is
// added.
func reservePlacementResources(workers []domain.WorkerInventory, workerID string, demand domain.ResourceDemand) []domain.WorkerInventory {
	result := append([]domain.WorkerInventory(nil), workers...)
	for i := range result {
		if result[i].ID != workerID || result[i].Telemetry == nil {
			continue
		}
		v := result[i].Telemetry.Clone()
		result[i].Telemetry = v
		if v.Load1 != nil {
			*v.Load1 += demand.CPUUnits
		}
		if v.Load5 != nil {
			*v.Load5 += demand.CPUUnits
		}
		deduct := func(x *int64, need int) {
			if x != nil {
				*x -= int64(need)
				if *x < 0 {
					*x = 0
				}
			}
		}
		deduct(v.MemoryAvailableMB, demand.MemoryMB)
		deduct(v.WorkspaceFreeMB, demand.ScratchMB)
		deduct(v.TempFreeMB, demand.ScratchMB)
		if v.RunningAttempts != nil {
			*v.RunningAttempts++
		}
	}
	return result
}
