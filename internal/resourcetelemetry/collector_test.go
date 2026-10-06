package resourcetelemetry

import (
	"errors"
	"testing"
	"time"
)

func TestCollectLinux(t *testing.T) {
	now := time.Date(2026, 10, 6, 1, 2, 3, 0, time.UTC)
	paths := []string{}
	c := Collector{
		GOOS: "linux", NumCPU: func() int { return 8 }, Now: func() time.Time { return now },
		ReadFile: func(path string) ([]byte, error) {
			switch path {
			case "/proc/loadavg":
				return []byte("2.5 3.75 4.0 1/100 1\n"), nil
			case "/proc/meminfo":
				return []byte("MemAvailable: 8192000 kB\nSwapTotal: 2097152 kB\nSwapFree: 1048576 kB\n"), nil
			}
			return nil, errors.New("missing")
		},
		FreeBytes: func(path string) (uint64, error) {
			paths = append(paths, path)
			if path == "/work" {
				return 12 * 1024 * 1024, nil
			}
			return 7 * 1024 * 1024, nil
		},
	}
	got := c.Collect("/work", "/temp", 3)
	if got.ObservedAt != now || *got.CPUCount != 8 || *got.Load1 != 2.5 || *got.Load5 != 3.75 || *got.MemoryAvailableMB != 8000 || *got.SwapUsedMB != 1024 || *got.WorkspaceFreeMB != 12 || *got.TempFreeMB != 7 || *got.RunningAttempts != 3 {
		t.Fatalf("telemetry = %+v", got)
	}
	if len(paths) != 2 || paths[0] != "/work" || paths[1] != "/temp" {
		t.Fatalf("paths = %v", paths)
	}
}

func TestUnavailableAndMalformedRemainUnknown(t *testing.T) {
	for _, platform := range []string{"linux", "other"} {
		c := Collector{GOOS: platform, NumCPU: func() int { return 2 }, ReadFile: func(string) ([]byte, error) {
			return []byte("MemAvailable: nope kB\nSwapTotal: 0 kB\nSwapFree: 1 kB\n"), nil
		}, FreeBytes: func(string) (uint64, error) { return 0, errors.New("unavailable") }}
		got := c.Collect("/missing", "/temp", 0)
		if got.Load1 != nil || got.Load5 != nil || got.MemoryAvailableMB != nil || got.SwapUsedMB != nil || got.WorkspaceFreeMB != nil || got.TempFreeMB != nil || got.CPUCount == nil || got.RunningAttempts == nil || *got.RunningAttempts != 0 {
			t.Fatalf("%s telemetry = %+v", platform, got)
		}
	}
}

func TestLoadAndMemoryInvalidValues(t *testing.T) {
	for _, input := range []string{"NaN 1 0", "Inf 1 0", "-1 2 0", "1"} {
		c := Collector{GOOS: "linux", ReadFile: func(path string) ([]byte, error) {
			if path == "/proc/loadavg" {
				return []byte(input), nil
			}
			return []byte("MemAvailable: -1 kB\nSwapTotal: 1 MB\nSwapFree: 0 kB\n"), nil
		}}
		got := c.Collect("", "", 0)
		if got.Load1 != nil || got.MemoryAvailableMB != nil || got.SwapUsedMB != nil {
			t.Fatalf("%q telemetry = %+v", input, got)
		}
	}
}
