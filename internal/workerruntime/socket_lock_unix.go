//go:build unix

package workerruntime

import (
	"errors"
	"os"
	"syscall"
)

// LockWorkerSocket holds exclusive runtime custody for the socket's lifetime.
// Kernel locks disappear after a crash; only the next owner removes its stale socket.
func LockWorkerSocket(path string) (*os.File, error) {
	fd, err := syscall.Open(path+".lock", syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path+".lock")
	fail := func(err error) (*os.File, error) { f.Close(); return nil, err }
	info, err := f.Stat()
	if err != nil {
		return fail(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Getuid()) || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return fail(errors.New("invalid worker ownership lock"))
	}
	if err = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fail(errors.New("worker runtime already owns socket"))
	}
	info, err = os.Lstat(path)
	if err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return fail(errors.New("refusing to replace non-socket worker path"))
		}
		if err = os.Remove(path); err != nil {
			return fail(err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fail(err)
	}
	return f, nil
}
