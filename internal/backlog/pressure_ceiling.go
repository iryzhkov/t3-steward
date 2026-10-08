package backlog

import (
	"fmt"
	"strconv"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// ExclusionResourceCPULoad is the build load ceiling: temporary pressure, like
// the other live-telemetry exclusions, and never a statement about capacity.
const ExclusionResourceCPULoad = "resource-cpu-load"

// buildClassCPUUnits is the expected cpu need from which a task counts as
// build-class work for the load ceiling. A light task and an unsized task
// expect less and are never held back by it.
const buildClassCPUUnits = 2

// buildLoadCeiling stops new build-class work on a worker whose observed load
// per cpu is above the policy ceiling. It applies only to fresh, complete cpu
// evidence, which the caller has established, and it never touches running
// work: it decides placement, not preemption. Load is the larger of the one
// and five minute averages, the same figure the cpu headroom score uses.
func buildLoadCeiling(cpus int, load float64, need domain.ResourceDemand, p domain.ResourcePlacementPolicy) (WorkerExclusion, bool) {
	if p.BuildMaxLoadPerCPU <= 0 || cpus <= 0 || need.CPUUnits < buildClassCPUUnits {
		return WorkerExclusion{}, false
	}
	if load/float64(cpus) <= p.BuildMaxLoadPerCPU {
		return WorkerExclusion{}, false
	}
	return WorkerExclusion{
		Code: ExclusionResourceCPULoad,
		Detail: fmt.Sprintf("load %s over %d cpus exceeds build ceiling %s per cpu",
			strconv.FormatFloat(load, 'f', -1, 64), cpus, strconv.FormatFloat(p.BuildMaxLoadPerCPU, 'f', -1, 64)),
	}, true
}

// countedSwapMB is the swap that counts against MaxSwapUsedMB, and how the
// detail names it. Swap on zram is compressed memory rather than paging to
// disk, so when the policy ignores it and the worker reports the split, only
// the rest counts. A worker that does not report the split keeps the whole
// figure, as before the split existed. Unknown swap counts as none.
func countedSwapMB(v *domain.WorkerTelemetry, p domain.ResourcePlacementPolicy) (int64, string) {
	if v.SwapUsedMB == nil || *v.SwapUsedMB < 0 {
		return 0, ""
	}
	used := *v.SwapUsedMB
	if !p.SwapIgnoreZram || v.ZramSwapUsedMB == nil || *v.ZramSwapUsedMB < 0 {
		return used, "swap"
	}
	return max(0, used-*v.ZramSwapUsedMB), "disk swap"
}
