// Package resourcetelemetry collects best-effort host observations. Unavailable
// measurements stay nil, so placement can distinguish unknown from zero.
package resourcetelemetry

import (
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type Collector struct {
	GOOS      string
	ReadFile  func(string) ([]byte, error)
	FreeBytes func(string) (uint64, error)
	NumCPU    func() int
	Now       func() time.Time
}

func New() Collector {
	return Collector{GOOS: runtime.GOOS, ReadFile: os.ReadFile, FreeBytes: platformFreeBytes, NumCPU: runtime.NumCPU, Now: time.Now}
}

func (c Collector) Collect(workspace, temp string, running int) domain.WorkerTelemetry {
	now := time.Now()
	if c.Now != nil {
		now = c.Now()
	}
	got := domain.WorkerTelemetry{ObservedAt: now.UTC()}
	if c.NumCPU != nil {
		if n := c.NumCPU(); n > 0 {
			got.CPUCount = &n
		}
	}
	if running >= 0 {
		got.RunningAttempts = &running
	}
	if c.GOOS == "linux" && c.ReadFile != nil {
		if data, err := c.ReadFile("/proc/loadavg"); err == nil {
			fields := strings.Fields(string(data))
			if len(fields) >= 2 {
				got.Load1 = parseLoad(fields[0])
				got.Load5 = parseLoad(fields[1])
			}
		}
		if data, err := c.ReadFile("/proc/meminfo"); err == nil {
			values := map[string]int64{}
			for _, line := range strings.Split(string(data), "\n") {
				f := strings.Fields(line)
				if len(f) != 3 || f[2] != "kB" {
					continue
				}
				n, err := strconv.ParseInt(f[1], 10, 64)
				if err == nil && n >= 0 {
					values[strings.TrimSuffix(f[0], ":")] = n
				}
			}
			if n, ok := values["MemAvailable"]; ok {
				mb := n / 1024
				got.MemoryAvailableMB = &mb
			}
			total, okTotal := values["SwapTotal"]
			free, okFree := values["SwapFree"]
			if okTotal && okFree && total >= free {
				mb := (total - free) / 1024
				got.SwapUsedMB = &mb
			}
		}
	}
	if c.FreeBytes != nil {
		got.WorkspaceFreeMB = freeMB(c.FreeBytes, workspace)
		got.TempFreeMB = freeMB(c.FreeBytes, temp)
	}
	return got
}

func parseLoad(s string) *float64 {
	n, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(n) || math.IsInf(n, 0) || n < 0 {
		return nil
	}
	return &n
}
func freeMB(read func(string) (uint64, error), path string) *int64 {
	if path == "" {
		return nil
	}
	n, err := read(path)
	if err != nil || n/(1024*1024) > math.MaxInt64 {
		return nil
	}
	mb := int64(n / (1024 * 1024))
	return &mb
}
