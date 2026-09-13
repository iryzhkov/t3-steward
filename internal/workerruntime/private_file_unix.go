//go:build unix

package workerruntime

import (
	"errors"
	"io"
	"os"
	"syscall"
)

func readPrivateFile(path string, limit int64) ([]byte, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Mode().Perm() != 0600 {
		return nil, errors.New("private file mode")
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Getuid()) || !os.SameFile(before, info) || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() < 1 || info.Size() > limit {
		return nil, errors.New("private file ownership, identity or size")
	}
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(raw)) > limit {
		return nil, errors.New("private file read failed")
	}
	return raw, nil
}
