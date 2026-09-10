package backlog

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const fileLockRetryInterval = 10 * time.Millisecond

type fileLock struct {
	file *os.File
}

func acquireFileLock(ctx context.Context, root, key string) (*fileLock, error) {
	if root == "" {
		return nil, errors.New("file lock root is required")
	}
	if key == "" {
		return nil, errors.New("file lock key is required")
	}
	lockRoot := filepath.Join(root, ".locks")
	if err := os.MkdirAll(lockRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create lock directory: %w", err)
	}
	sum := sha256.Sum256([]byte(key))
	path := filepath.Join(lockRoot, fmt.Sprintf("%x.lock", sum))
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open file lock: %w", err)
	}
	for {
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return &fileLock{file: file}, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			_ = file.Close()
			return nil, fmt.Errorf("acquire file lock: %w", err)
		}
		timer := time.NewTimer(fileLockRetryInterval)
		select {
		case <-ctx.Done():
			_ = timer.Stop()
			_ = file.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (l *fileLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	unlockErr := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	closeErr := l.file.Close()
	l.file = nil
	return errors.Join(unlockErr, closeErr)
}
