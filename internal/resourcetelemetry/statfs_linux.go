//go:build linux

package resourcetelemetry

import (
	"fmt"
	"golang.org/x/sys/unix"
	"math"
)

func platformFreeBytes(path string) (uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, err
	}
	if stat.Bsize <= 0 || stat.Bavail > math.MaxUint64/uint64(stat.Bsize) {
		return 0, fmt.Errorf("invalid filesystem space")
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}
