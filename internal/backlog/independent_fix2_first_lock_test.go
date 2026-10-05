package backlog

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestIndependentFix2SimultaneousFirstLock(t *testing.T) {
	namespace := filepath.Join(t.TempDir(), "stages")
	if err := os.Mkdir(namespace, 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2000; i++ {
		key := fmtKey(i)
		start := make(chan struct{})
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		for j := 0; j < 2; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				l, e := childStageLock(context.Background(), namespace, key)
				if e == nil {
					l.Close()
				}
				errs <- e
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		for e := range errs {
			if e != nil {
				t.Fatalf("simultaneous identical first custody iteration=%d: %v", i, e)
			}
		}
	}
}

func TestIndependentFix2SimultaneousStage(t *testing.T) {
	for i := 0; i < 100; i++ {
		t.Run(fmtKey(i), func(t *testing.T) {
			_, o, req := newStageOwner(t)
			other, e := NewDeclaredReviewStaging(o.admission, o.store, o.now)
			if e != nil {
				t.Fatal(e)
			}
			start := make(chan struct{})
			errs := make(chan error, 2)
			for _, s := range []*DeclaredReviewStaging{o, other} {
				go func(s *DeclaredReviewStaging) { <-start; _, e := s.StageDeclared(context.Background(), req); errs <- e }(s)
			}
			close(start)
			e1, e2 := <-errs, <-errs
			if e1 != nil || e2 != nil {
				t.Fatalf("simultaneous identical staging iteration=%d first=%v second=%v", i, e1, e2)
			}
		})
		if t.Failed() {
			return
		}
	}
}

func fmtKey(i int) string {
	const digits = "0123456789"
	s := ""
	for {
		s = string(digits[i%10]) + s
		i /= 10
		if i == 0 {
			return "key-" + s
		}
	}
}
