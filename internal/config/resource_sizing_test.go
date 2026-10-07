package config

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildLoadCeilingAndZramSettings(t *testing.T) {
	policy := Default().BacklogV2.Coordinator.ResourcePlacement.Policy()
	if policy.BuildMaxLoadPerCPU != 1.5 || !policy.SwapIgnoreZram {
		t.Fatalf("defaults = %+v", policy)
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	contents := "backlog_v2:\n  coordinator:\n    resource_placement:\n      build_max_load_per_cpu: 0\n      swap_ignore_zram: false\n"
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	policy = cfg.BacklogV2.Coordinator.ResourcePlacement.Policy()
	if policy.BuildMaxLoadPerCPU != 0 || policy.SwapIgnoreZram || policy.MaxSwapUsedMB != 4096 {
		t.Fatalf("configured policy = %+v", policy)
	}
	for name, value := range map[string]float64{"negative": -1, "nan": math.NaN(), "infinity": math.Inf(1)} {
		t.Run(name, func(t *testing.T) {
			cfg := Default()
			cfg.BacklogV2.Coordinator.ResourcePlacement = V2ResourcePlacement{BuildMaxLoadPerCPU: &value}
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "build_max_load_per_cpu") {
				t.Fatalf("invalid ceiling error = %v", err)
			}
		})
	}
}
