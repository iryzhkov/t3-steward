package backlog

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestIndependentFix2FirstPublicationRace(t *testing.T) {
	base := stageOwnedTempDir(t)
	for n := 0; n < 2000; n++ {
		ns, err := os.MkdirTemp(base, "race-")
		if err != nil {
			t.Fatal(err)
		}
		if err = os.Mkdir(filepath.Join(ns, ".locks"), 0700); err != nil {
			t.Fatal(err)
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
			t.Fatalf("initial identical cooperating calls failed at iteration %d: first=%v second=%v", n, errs[0], errs[1])
		}
		if !os.SameFile(infos[0], infos[1]) {
			t.Fatal("different lock inodes")
		}
	}
}
