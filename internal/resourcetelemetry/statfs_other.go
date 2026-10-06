//go:build !linux

package resourcetelemetry

import "errors"

func platformFreeBytes(string) (uint64, error) {
	return 0, errors.New("filesystem space unavailable on this platform")
}
