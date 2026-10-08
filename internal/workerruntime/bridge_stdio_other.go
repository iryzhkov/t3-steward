//go:build !unix

package workerruntime

import "os"

// InterruptibleFile returns a File for fd. Where descriptors cannot be
// switched to non-blocking mode, Close may not release a blocked call.
func InterruptibleFile(fd uintptr, name string) (*os.File, error) {
	return os.NewFile(fd, name), nil
}
