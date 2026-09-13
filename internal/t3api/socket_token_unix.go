//go:build linux || darwin

package t3api

import (
	"os"
	"syscall"
)

// A provider can replace token with a FIFO. Nonblocking open lets the caller
// inspect and reject special files instead of hanging the worker.
func openTokenFile(root *os.Root) (*os.File, error) {
	return root.OpenFile("token", os.O_RDONLY|syscall.O_NONBLOCK, 0)
}
