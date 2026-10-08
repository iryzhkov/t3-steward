package resourcetelemetry

import (
	"errors"
	"testing"
)

func zramCollector(swaps string, err error) Collector {
	return Collector{GOOS: "linux", ReadFile: func(path string) ([]byte, error) {
		switch path {
		case "/proc/meminfo":
			return []byte("MemAvailable: 8192000 kB\nSwapTotal: 16777216 kB\nSwapFree: 8000000 kB\n"), nil
		case "/proc/swaps":
			return []byte(swaps), err
		}
		return nil, errors.New("missing")
	}}
}

func TestCollectSplitsZramSwap(t *testing.T) {
	got := zramCollector("Filename\t\t\t\tType\t\tSize\t\tUsed\t\tPriority\n"+
		"/dev/zram0                              partition\t8388604\t\t4300800\t\t100\n"+
		"/dev/zram1                              partition\t8388604\t\t1024\t\t100\n"+
		"/swapfile                               file\t\t4194300\t\t2048\t\t-2\n", nil).Collect("", "", 0)
	if got.SwapUsedMB == nil || *got.SwapUsedMB != 8571 {
		t.Fatalf("total swap = %v", got.SwapUsedMB)
	}
	if got.ZramSwapUsedMB == nil || *got.ZramSwapUsedMB != 4201 {
		t.Fatalf("zram swap = %v, want 4201", got.ZramSwapUsedMB)
	}
	// A host with no swap device at all reports zero zram, which is known.
	if got := zramCollector("Filename\tType\tSize\tUsed\tPriority\n", nil).Collect("", "", 0); got.ZramSwapUsedMB == nil || *got.ZramSwapUsedMB != 0 {
		t.Fatalf("empty swaps zram = %v", got.ZramSwapUsedMB)
	}
	// Unreadable or malformed evidence stays unknown.
	if got := zramCollector("", errors.New("denied")).Collect("", "", 0); got.ZramSwapUsedMB != nil {
		t.Fatalf("unreadable swaps zram = %v", *got.ZramSwapUsedMB)
	}
	if got := zramCollector("Filename Type Size Used Priority\n/dev/zram0 partition 10 lots 100\n", nil).Collect("", "", 0); got.ZramSwapUsedMB != nil {
		t.Fatalf("malformed swaps zram = %v", *got.ZramSwapUsedMB)
	}
}
