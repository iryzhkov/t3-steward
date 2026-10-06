package config

import "github.com/iryzhkov/t3-steward/internal/domain"

// V2ResourcePlacement bounds worker placement using live resource telemetry.
// Pointers distinguish an omitted field from an explicitly configured zero.
type V2ResourcePlacement struct {
	TelemetryMaxAge *Duration `yaml:"telemetry_max_age"`
	MemoryReserveMB *int64    `yaml:"memory_reserve_mb"`
	DiskReserveMB   *int64    `yaml:"disk_reserve_mb"`
	MaxSwapUsedMB   *int64    `yaml:"max_swap_used_mb"`
	CPUWeight       *float64  `yaml:"cpu_weight"`
	MemoryWeight    *float64  `yaml:"memory_weight"`
	// The nominal live use of one attempt that declares neither a size nor a
	// preset class, used for safety floors, ranking and burst reservation.
	UnsizedTaskCPUUnits  *float64 `yaml:"unsized_task_cpu_units"`
	UnsizedTaskMemoryMB  *int     `yaml:"unsized_task_memory_mb"`
	UnsizedTaskScratchMB *int     `yaml:"unsized_task_scratch_mb"`
}

// Policy resolves omitted values to the coordinator's safe defaults.
func (c V2ResourcePlacement) Policy() domain.ResourcePlacementPolicy {
	policy := domain.DefaultResourcePlacementPolicy()
	if c.TelemetryMaxAge != nil {
		policy.TelemetryMaxAge = c.TelemetryMaxAge.D()
	}
	if c.MemoryReserveMB != nil {
		policy.MemoryReserveMB = *c.MemoryReserveMB
	}
	if c.DiskReserveMB != nil {
		policy.DiskReserveMB = *c.DiskReserveMB
	}
	if c.MaxSwapUsedMB != nil {
		policy.MaxSwapUsedMB = *c.MaxSwapUsedMB
	}
	if c.CPUWeight != nil {
		policy.CPUWeight = *c.CPUWeight
	}
	if c.MemoryWeight != nil {
		policy.MemoryWeight = *c.MemoryWeight
	}
	if c.UnsizedTaskCPUUnits != nil {
		policy.UnsizedTaskCPUUnits = *c.UnsizedTaskCPUUnits
	}
	if c.UnsizedTaskMemoryMB != nil {
		policy.UnsizedTaskMemoryMB = *c.UnsizedTaskMemoryMB
	}
	if c.UnsizedTaskScratchMB != nil {
		policy.UnsizedTaskScratchMB = *c.UnsizedTaskScratchMB
	}
	return policy
}
