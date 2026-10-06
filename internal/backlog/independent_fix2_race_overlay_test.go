package backlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func TestIndependentFix2FirstPublicationRace(t *testing.T) {
	t.Parallel()
	base := stageOwnedTempDir(t)
	runLockRaceIterations(t, 2000, func(n int) error {
		ns, err := os.MkdirTemp(base, "race-")
		if err != nil {
			return err
		}
		if err = os.Mkdir(filepath.Join(ns, ".locks"), 0700); err != nil {
			return err
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		errs := make([]error, 2)
		infos := make([]os.FileInfo, 2)
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				lock, e := childStageLock(context.Background(), ns, "same")
				errs[i] = e
				if e == nil {
					infos[i], errs[i] = lock.file.Stat()
					lock.Close()
				}
			}(i)
		}
		close(start)
		wg.Wait()
		if errs[0] != nil || errs[1] != nil {
			return fmt.Errorf("initial identical cooperating calls failed at iteration %d: first=%v second=%v", n, errs[0], errs[1])
		}
		if !os.SameFile(infos[0], infos[1]) {
			return fmt.Errorf("different lock inodes at iteration %d", n)
		}
		return nil
	})
}

// runLockRaceIterations runs iterations 0 through count-1 and fails the test
// with the error of any iteration that returns one. Each iteration is a
// self-contained race between two cooperating callers that start together; it
// spends most of its time waiting for the lock and directory syncs the
// production code makes, so iterations run on several goroutines at once
// instead of one after another. No iteration is skipped and each is checked
// exactly as a sequential loop would check it. After a failure no new
// iteration starts.
func runLockRaceIterations(t *testing.T, count int, iteration func(n int) error) {
	t.Helper()
	const workers = 8
	var next atomic.Int64
	var failed atomic.Bool
	failures := make(chan error, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !failed.Load() {
				n := int(next.Add(1) - 1)
				if n >= count {
					return
				}
				if err := iteration(n); err != nil {
					failed.Store(true)
					failures <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}
}
