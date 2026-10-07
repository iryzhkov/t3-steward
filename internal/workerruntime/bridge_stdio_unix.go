//go:build unix

package workerruntime

import (
	"os"
	"syscall"
)

// InterruptibleFile returns a File for an inherited descriptor, such as
// standard input, whose Close releases a Read or Write blocked on it. An
// inherited descriptor is normally in blocking mode, where Close waits for
// the blocked call instead; switching it to non-blocking mode lets the
// runtime poller own it. The returned File owns fd.
func InterruptibleFile(fd uintptr, name string) (*os.File, error) {
	if err := syscall.SetNonblock(int(fd), true); err != nil {
		return nil, err
	}
	return os.NewFile(fd, name), nil
}
