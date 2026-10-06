package config

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestResourcePlacementDefaults(t *testing.T) {
	policy := Default().BacklogV2.Coordinator.ResourcePlacement.Policy()
	if policy.TelemetryMaxAge != 2*time.Minute || policy.MemoryReserveMB != 1024 ||
		policy.DiskReserveMB != 2048 || policy.MaxSwapUsedMB != 4096 ||
		policy.CPUWeight != 1 || policy.MemoryWeight != 1 ||
		policy.UnsizedTaskCPUUnits != 1 || policy.UnsizedTaskMemoryMB != 1024 || policy.UnsizedTaskScratchMB != 0 {
		t.Fatalf("resource placement defaults = %+v", policy)
	}
}

func TestResourcePlacementConfiguredZerosAndOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	contents := "backlog_v2:\n  coordinator:\n    resource_placement:\n      telemetry_max_age: 45s\n      memory_reserve_mb: 0\n      disk_reserve_mb: 17\n      max_swap_used_mb: 0\n      cpu_weight: 0\n      memory_weight: 2\n      unsized_task_cpu_units: 0.5\n      unsized_task_memory_mb: 0\n      unsized_task_scratch_mb: 64\n"
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	policy := cfg.BacklogV2.Coordinator.ResourcePlacement.Policy()
	if policy.TelemetryMaxAge != 45*time.Second || policy.MemoryReserveMB != 0 ||
		policy.DiskReserveMB != 17 || policy.MaxSwapUsedMB != 0 ||
		policy.CPUWeight != 0 || policy.MemoryWeight != 2 ||
		policy.UnsizedTaskCPUUnits != 0.5 || policy.UnsizedTaskMemoryMB != 0 || policy.UnsizedTaskScratchMB != 64 {
		t.Fatalf("configured policy = %+v", policy)
	}
}

func TestResourcePlacementValidation(t *testing.T) {
	duration := func(v time.Duration) *Duration { d := Duration(v); return &d }
	integer := func(v int64) *int64 { return &v }
	weight := func(v float64) *float64 { return &v }
	size := func(v int) *int { return &v }
	cases := map[string]V2ResourcePlacement{
		"zero age":        {TelemetryMaxAge: duration(0)},
		"negative age":    {TelemetryMaxAge: duration(-time.Second)},
		"memory":          {MemoryReserveMB: integer(-1)},
		"disk":            {DiskReserveMB: integer(-1)},
		"swap":            {MaxSwapUsedMB: integer(-1)},
		"cpu":             {CPUWeight: weight(-1)},
		"memory weight":   {MemoryWeight: weight(-1)},
		"nan":             {CPUWeight: weight(math.NaN())},
		"infinity":        {MemoryWeight: weight(math.Inf(1))},
		"no weights":      {CPUWeight: weight(0), MemoryWeight: weight(0)},
		"weight overflow": {CPUWeight: weight(math.MaxFloat64), MemoryWeight: weight(math.MaxFloat64)},
		"unsized cpu":     {UnsizedTaskCPUUnits: weight(-1)},
		"unsized cpu nan": {UnsizedTaskCPUUnits: weight(math.NaN())},
		"unsized memory":  {UnsizedTaskMemoryMB: size(-1)},
		"unsized scratch": {UnsizedTaskScratchMB: size(-1)},
	}
	for name, policy := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := Default()
			cfg.BacklogV2.Coordinator.ResourcePlacement = policy
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "backlog_v2.coordinator.resource_placement") {
				t.Fatalf("invalid resource policy error = %v", err)
			}
		})
	}
}

func TestResourcePlacementPartialConfigurationKeepsDefaults(t *testing.T) {
	v := int64(0)
	policy := (V2ResourcePlacement{MemoryReserveMB: &v}).Policy()
	if policy.MemoryReserveMB != 0 || policy.DiskReserveMB != 2048 || policy.CPUWeight != 1 {
		t.Fatalf("partial policy = %+v", policy)
	}
}
