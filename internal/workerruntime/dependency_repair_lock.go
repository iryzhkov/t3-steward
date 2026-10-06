package workerruntime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// A kernel lock serializes repair for one published workspace across callers
// and replacement worker processes, and releases automatically after a crash.
// Keep it beside the checkout, outside the task-editable metadata directory.
func lockDependencyRepair(ctx context.Context, workspace string) (*os.File, error) {
	path := filepath.Join(filepath.Dir(workspace), ".dependency-repair.lock")
	fd, err := syscall.Open(path, syscall.O_RDWR|syscall.O_CREAT|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	fail := func(err error) (*os.File, error) { _ = file.Close(); return nil, err }
	info, err := file.Stat()
	if err != nil {
		return fail(err)
	}
	if !info.Mode().IsRegular() {
		return fail(errors.New("dependency repair lock is not a regular file"))
	}
	for {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		err = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return fail(err)
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fail(ctx.Err())
		case <-timer.C:
		}
	}
}
