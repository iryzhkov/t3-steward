package backlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// stageOwnedTempDir resolves only a directory allocated and cleaned up by this
// test. Production roots still pass the strict Lstat fence without resolution.
func stageOwnedTempDir(t *testing.T) string {
	t.Helper()
	path, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return path
}

type stageLockTestWorker struct {
	ctx                  context.Context
	ready, release, done chan struct{}
	once                 sync.Once
	lock                 *fileLock
	err                  error
}

func startStageLockTestWorker(t *testing.T, namespace, key, phase string) *stageLockTestWorker {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	w := &stageLockTestWorker{ctx: ctx, ready: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		w.lock, w.err = childStageLockWithBoundary(ctx, namespace, key, func(p string) error {
			if p == phase {
				close(w.ready)
				select {
				case <-w.release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	}()
	t.Cleanup(func() {
		cancel()
		w.unblock()
		<-w.done
		if w.lock != nil {
			_ = w.lock.Close()
		}
	})
	return w
}

func (w *stageLockTestWorker) unblock() { w.once.Do(func() { close(w.release) }) }
func (w *stageLockTestWorker) waitBoundary() error {
	return waitStageTestBoundary(w.ctx, w.ready, w.done, func() error { return w.err })
}
func (w *stageLockTestWorker) result() (*fileLock, error) {
	select {
	case <-w.done:
		return w.lock, w.err
	case <-w.ctx.Done():
		return nil, w.ctx.Err()
	}
}

func waitStageTestBoundary(ctx context.Context, ready, done <-chan struct{}, failure func() error) error {
	select {
	case <-ready:
		return nil
	case <-done:
		// A completed worker may have crossed the boundary before completion.
		select {
		case <-ready:
			return nil
		default:
		}
		err := failure()
		if err == nil {
			err = errors.New("worker exited without reaching boundary")
		}
		return fmt.Errorf("staging test boundary not reached: %w", err)
	case <-ctx.Done():
		return fmt.Errorf("staging test boundary not reached: %w", ctx.Err())
	}
}

func TestReviewChildStagingOwnedTempAlias(t *testing.T) {
	physical := stageOwnedTempDir(t)
	alias := filepath.Join(stageOwnedTempDir(t), "temp-alias")
	if err := os.Symlink(physical, alias); err != nil {
		t.Fatal(err)
	}
	t.Run("alias-parent", func(t *testing.T) {
		t.Setenv("TMPDIR", alias)
		t.Setenv("GOTMPDIR", alias)
		raw := t.TempDir()
		resolved, err := filepath.EvalSymlinks(raw)
		if err != nil {
			t.Fatal(err)
		}
		if raw == resolved || !strings.HasPrefix(resolved, physical+string(filepath.Separator)) {
			t.Fatalf("alias mapping raw=%q physical=%q parent=%q", raw, resolved, physical)
		}
		a, err := os.Stat(raw)
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.Stat(resolved)
		if err != nil || !os.SameFile(a, b) {
			t.Fatal("alias identity", err)
		}
		if err := childStageRootFence(raw); err == nil {
			t.Fatal("production accepted alias ancestor")
		}
		_, owner, req := newStageOwner(t)
		if _, err := owner.StageDeclared(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		for _, field := range []string{"submission", "artifact"} {
			admission := owner.admission
			if field == "submission" {
				admission.Artifacts.SubmissionRoot = raw
			} else {
				admission.Artifacts.Root = raw
			}
			if _, err := NewDeclaredReviewStaging(admission, owner.store, time.Now); err == nil {
				t.Fatalf("configured %s alias accepted", field)
			}
		}
	})
}

func TestReviewChildStagingBoundaryWorkerFailure(t *testing.T) {
	// The real strict API fails before its boundary when the namespace is absent.
	worker := startStageLockTestWorker(t, filepath.Join(stageOwnedTempDir(t), "missing", "stages"), "same", "lock-private-held")
	waitErr := worker.waitBoundary()
	worker.unblock()
	lock, err := worker.result()
	if lock != nil || err == nil || !errors.Is(waitErr, err) {
		t.Fatalf("missing boundary lost worker error: wait=%v result=%v lock=%v", waitErr, err, lock)
	}

}

func TestReviewChildStagingBoundaryDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := waitStageTestBoundary(ctx, make(chan struct{}), make(chan struct{}), func() error { return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("missing boundary was not bounded", err)
	}
}
