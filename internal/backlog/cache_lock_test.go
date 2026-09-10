package backlog

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestLocalRepositoryCacheSerializesConcurrentPreparationAndCleansStaleStage(t *testing.T) {
	repository := newGitFixture(t)
	cacheRoot := t.TempDir()
	cache := LocalRepositoryCache{Root: cacheRoot}
	sum := sha256.Sum256([]byte(repository))
	stale := filepath.Join(cacheRoot, fmt.Sprintf(".clone-%x-orphan", sum))
	if err := os.MkdirAll(filepath.Join(stale, "repository.git"), 0o755); err != nil {
		t.Fatalf("create stale cache stage: %v", err)
	}

	const workers = 6
	start := make(chan struct{})
	results := make(chan CachedRepository, workers)
	errs := make(chan error, workers)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			result, err := cache.Prepare(context.Background(), repository, nil)
			results <- result
			errs <- err
		}()
	}
	close(start)
	group.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent cache preparation: %v", err)
		}
	}
	reused := 0
	path := ""
	for result := range results {
		if path == "" {
			path = result.Path
		} else if result.Path != path {
			t.Fatalf("cache paths differ: %q != %q", result.Path, path)
		}
		if result.Reused {
			reused++
		}
	}
	if reused != workers-1 {
		t.Fatalf("reused preparations = %d, want %d", reused, workers-1)
	}
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale cache stage remains: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("published cache missing: %v", err)
	}
}

func TestFileLockWaitHonorsContext(t *testing.T) {
	root := t.TempDir()
	first, err := acquireFileLock(context.Background(), root, "same-resource")
	if err != nil {
		t.Fatalf("acquire first lock: %v", err)
	}
	defer first.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, err := acquireFileLock(ctx, root, "same-resource"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("contended lock error = %v, want deadline", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("context cancellation did not promptly stop lock wait")
	}
}
