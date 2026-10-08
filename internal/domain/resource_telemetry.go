package domain

import (
	"fmt"
	"math"
	"time"
)

// WorkerTelemetry is optional live resource evidence. Nil measurements are
// unknown, never zero. It is independent of operator capacity and reservations.
type WorkerTelemetry struct {
	ObservedAt        time.Time `json:"observed_at"`
	CPUCount          *int      `json:"cpu_count,omitempty"`
	Load1             *float64  `json:"load_1,omitempty"`
	Load5             *float64  `json:"load_5,omitempty"`
	MemoryAvailableMB *int64    `json:"memory_available_mb,omitempty"`
	SwapUsedMB        *int64    `json:"swap_used_mb,omitempty"`
	// ZramSwapUsedMB is the part of SwapUsedMB held on zram devices, which is
	// compressed memory rather than paging to disk. It is reported only to a
	// coordinator that asks for it; nil means the split is unknown.
	ZramSwapUsedMB  *int64 `json:"zram_swap_used_mb,omitempty"`
	WorkspaceFreeMB *int64 `json:"workspace_free_mb,omitempty"`
	TempFreeMB      *int64 `json:"temp_free_mb,omitempty"`
	RunningAttempts *int   `json:"running_attempts,omitempty"`
}

// Clone makes decision evidence and planning adjustments independent of input.
func (v *WorkerTelemetry) Clone() *WorkerTelemetry {
	if v == nil {
		return nil
	}
	c := *v
	c.CPUCount = copyMeasurement(v.CPUCount)
	c.Load1 = copyMeasurement(v.Load1)
	c.Load5 = copyMeasurement(v.Load5)
	c.MemoryAvailableMB = copyMeasurement(v.MemoryAvailableMB)
	c.SwapUsedMB = copyMeasurement(v.SwapUsedMB)
	c.ZramSwapUsedMB = copyMeasurement(v.ZramSwapUsedMB)
	c.WorkspaceFreeMB = copyMeasurement(v.WorkspaceFreeMB)
	c.TempFreeMB = copyMeasurement(v.TempFreeMB)
	c.RunningAttempts = copyMeasurement(v.RunningAttempts)
	return &c
}
func copyMeasurement[T any](v *T) *T {
	if v == nil {
		return nil
	}
	c := *v
	return &c
}

// ResourcePlacementPolicy holds the coordinator's live-telemetry thresholds and
// weights. The UnsizedTask fields are the nominal live use expected of one
// attempt that declares no size and no preset class; they feed safety floors,
// ranking and in-cycle burst reservation, and never reserve configured capacity.
type ResourcePlacementPolicy struct {
	TelemetryMaxAge      time.Duration
	MemoryReserveMB      int64
	DiskReserveMB        int64
	MaxSwapUsedMB        int64
	CPUWeight            float64
	MemoryWeight         float64
	UnsizedTaskCPUUnits  float64
	UnsizedTaskMemoryMB  int
	UnsizedTaskScratchMB int
	// BuildMaxLoadPerCPU is the soft ceiling on new build-class work: a
	// worker whose observed load per cpu is above it takes no new task that
	// needs two or more cpu units. Zero disables the ceiling.
	BuildMaxLoadPerCPU float64
	// SwapIgnoreZram counts only disk swap against MaxSwapUsedMB when the
	// worker reports how much of its swap is on zram.
	SwapIgnoreZram bool
}

func DefaultResourcePlacementPolicy() ResourcePlacementPolicy {
	return ResourcePlacementPolicy{
		TelemetryMaxAge: 2 * time.Minute, MemoryReserveMB: 1024, DiskReserveMB: 2048, MaxSwapUsedMB: 4096, CPUWeight: 1, MemoryWeight: 1,
		UnsizedTaskCPUUnits: 1, UnsizedTaskMemoryMB: 1024,
		BuildMaxLoadPerCPU: 1.5, SwapIgnoreZram: true,
	}
}

// WithDefaults preserves explicit zero thresholds in a configured policy.
func (p ResourcePlacementPolicy) WithDefaults() ResourcePlacementPolicy {
	if p == (ResourcePlacementPolicy{}) {
		return DefaultResourcePlacementPolicy()
	}
	return p
}
func (p ResourcePlacementPolicy) Validate() error {
	if p.TelemetryMaxAge <= 0 {
		return fmt.Errorf("resource placement telemetry_max_age must be positive")
	}
	if p.MemoryReserveMB < 0 || p.DiskReserveMB < 0 || p.MaxSwapUsedMB < 0 {
		return fmt.Errorf("resource placement thresholds must not be negative")
	}
	if math.IsNaN(p.CPUWeight) || math.IsInf(p.CPUWeight, 0) || math.IsNaN(p.MemoryWeight) || math.IsInf(p.MemoryWeight, 0) || p.CPUWeight < 0 || p.MemoryWeight < 0 || p.CPUWeight+p.MemoryWeight <= 0 || math.IsInf(p.CPUWeight+p.MemoryWeight, 0) {
		return fmt.Errorf("resource placement weights must be finite and nonnegative, with at least one positive")
	}
	if math.IsNaN(p.UnsizedTaskCPUUnits) || math.IsInf(p.UnsizedTaskCPUUnits, 0) || p.UnsizedTaskCPUUnits < 0 || p.UnsizedTaskMemoryMB < 0 || p.UnsizedTaskScratchMB < 0 {
		return fmt.Errorf("resource placement unsized task needs must be finite and nonnegative")
	}
	if math.IsNaN(p.BuildMaxLoadPerCPU) || math.IsInf(p.BuildMaxLoadPerCPU, 0) || p.BuildMaxLoadPerCPU < 0 {
		return fmt.Errorf("resource placement build_max_load_per_cpu must be finite and nonnegative")
	}
	return nil
}

// ResourceEvaluation records the exact adjusted telemetry used for placement.
type ResourceEvaluation struct {
	WorkerID       string           `json:"workerId"`
	Telemetry      *WorkerTelemetry `json:"telemetry,omitempty"`
	State          string           `json:"state"`
	CPUHeadroom    float64          `json:"cpuHeadroom"`
	MemoryHeadroom float64          `json:"memoryHeadroom"`
	Score          float64          `json:"score"`
	Rank           int              `json:"rank,omitempty"`
}
