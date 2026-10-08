package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// A raw-path reader whose read completes before K further collections of the
// same task are published never observes a missing directory or a partial file.
// K is resultRetainedGenerations; the grace window additionally protects brief
// reads during faster bursts. The public directory is never missing during
// nonempty publication, and its final message is one collection's whole file.
func TestReadersAlwaysFindACompleteCollectionWhileANewOneIsPublished(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "run-1", "task")
	publish := func(n int) {
		t.Helper()
		staged, err := stageResultDirectory(directory)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(staged, "final-message.md"), []byte(fmt.Sprintf("collection %d", n)), 0o600); err != nil {
			t.Fatal(err)
		}
		if left, err := replaceResultDirectory(staged, directory); err != nil || len(left) != 0 {
			t.Fatalf("publish %d: left %v, err %v", n, left, err)
		}
	}
	publish(0)
	done := make(chan struct{})
	gaps := make(chan error, 1)
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			if _, err := os.ReadFile(filepath.Join(directory, "final-message.md")); err != nil {
				gaps <- err
				return
			}
		}
	}()
	for n := 1; n <= 500; n++ {
		publish(n)
	}
	close(done)
	readers.Wait()
	select {
	case err := <-gaps:
		t.Fatalf("a reader found no complete collection during publication: %v", err)
	default:
	}
	if got := readFile(t, directory, "final-message.md"); got != "collection 500" {
		t.Fatalf("final message = %q, want the last collection", got)
	}
}

// A collection interrupted by a crash leaves its hidden staging directory, or
// the earlier collection it had just exchanged out, beside the task's
// directory. The next collection removes such a leftover once it is old enough
// that no running collection can still own it, and leaves a recent one, which
// may belong to a collection still fetching.
func TestTheNextCollectionRemovesWhatAnInterruptedOneLeftBehind(t *testing.T) {
	runDirectory := filepath.Join(t.TempDir(), "run-1")
	directory := filepath.Join(runDirectory, "task")
	interrupted := func(age time.Duration) string {
		t.Helper()
		staged, err := stageResultDirectory(directory)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(staged, "old.txt"), []byte("old"), 0o600); err != nil {
			t.Fatal(err)
		}
		when := time.Now().Add(-age)
		if err := os.Chtimes(staged, when, when); err != nil {
			t.Fatal(err)
		}
		return staged
	}
	stale := interrupted(2 * interruptedCollectionAge)
	recent := interrupted(time.Minute)
	staged, err := stageResultDirectory(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staged, "final-message.md"), []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	if left, err := replaceResultDirectory(staged, directory); err != nil || len(left) != 0 {
		t.Fatalf("left %v, err %v", left, err)
	}
	if _, err := os.Lstat(stale); !os.IsNotExist(err) {
		t.Fatalf("the interrupted collection's leftover survived: %v", err)
	}
	if _, err := os.Lstat(recent); err != nil {
		t.Fatalf("a recent staging directory, possibly still in use, was removed: %v", err)
	}
	if got := readFile(t, directory, "final-message.md"); got != "new" {
		t.Fatalf("final message = %q", got)
	}
}

// An empty collection intentionally removes the previous public directory.
func TestEmptyCollectionStillRemovesPreviousWithoutExchange(t *testing.T) {
	swapDirectories = func(string, string) error { return errExchangeUnsupported }
	t.Cleanup(func() { swapDirectories = exchangeDirectories })
	f := newTaskResultFixture(t)
	base := t.TempDir()
	task := f.detail.Tasks[0]
	if _, err := f.cli().collect(context.Background(), f.detail, task, base, true); err != nil {
		t.Fatal(err)
	}
	f.detail.Artifacts = nil
	if _, err := f.cli().collect(context.Background(), f.detail, task, base, true); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(base)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("empty collection left public or staged directories: %v", entries)
	}
}

// Unsupported exchange must refuse a nonempty replacement before moving the
// old collection. Exercise collect so its deferred staging cleanup is covered.
func TestACollectionIsRefusedWhereDirectoriesCannotBeExchanged(t *testing.T) {
	testUnsupportedCollectionPreservesPrevious(t, func(string, string) error { return errExchangeUnsupported })
}

func testUnsupportedCollectionPreservesPrevious(t *testing.T, exchange func(string, string) error) {
	t.Helper()
	swapDirectories = exchange
	t.Cleanup(func() { swapDirectories = exchangeDirectories })
	f := newTaskResultFixture(t)
	base := t.TempDir()
	task := f.detail.Tasks[0]
	directory := filepath.Join(base, task.Task.Name)
	if _, err := f.cli().collect(context.Background(), f.detail, task, base, true); err != nil {
		t.Fatalf("atomic first publication: %v", err)
	}
	before := make(map[string]string)
	for _, path := range []string{"final-message.md", "reports/report.md"} {
		before[path] = readFile(t, directory, filepath.FromSlash(path))
	}
	// Also retain a file the new attempt would otherwise remove.
	if err := os.WriteFile(filepath.Join(directory, "old-only.txt"), []byte("old bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	before["old-only.txt"] = "old bytes"
	for id := range f.content {
		f.content[id] = "replacement bytes"
	}
	done := make(chan struct{})
	started := make(chan struct{})
	failures := make(chan error, 1)
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		check := func() error {
			for path, want := range before {
				got, err := os.ReadFile(filepath.Join(directory, filepath.FromSlash(path)))
				if err != nil {
					return err
				}
				if string(got) != want {
					return fmt.Errorf("%s changed to %q", path, got)
				}
			}
			return nil
		}
		first := true
		for {
			if err := check(); err != nil {
				failures <- err
				if first {
					close(started)
				}
				return
			}
			if first {
				close(started)
				first = false
			}
			select {
			case <-done:
				return
			default:
			}
		}
	}()
	<-started
	var stopped sync.Once
	stop := func() { stopped.Do(func() { close(done); readers.Wait() }) }
	defer stop()
	for n := 0; n < 100; n++ {
		_, err := f.cli().collect(context.Background(), f.detail, task, base, true)
		if !errors.Is(err, errExchangeUnsupported) {
			t.Fatalf("replacement %d must refuse unsupported exchange, got %v", n, err)
		}
		for path, want := range before {
			if got := readFile(t, directory, filepath.FromSlash(path)); got != want {
				t.Fatalf("%s = %q, want %q", path, got, want)
			}
		}
		entries, err := os.ReadDir(base)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 || entries[0].Name() != task.Task.Name {
			t.Fatalf("staging survived refusal: %v", entries)
		}
	}
	stop()
	select {
	case err := <-failures:
		t.Fatalf("reader lost previous complete collection: %v", err)
	default:
	}
}

// If an exchange reports unsupported before a concurrent collection creates
// the public path, the attempted first-publication rename must refuse safely.
func TestUnsupportedExchangePreservesAConcurrentFirstCollection(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "task")
	staged, err := stageResultDirectory(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(staged)
	if err := os.WriteFile(filepath.Join(staged, "new.txt"), []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	swapDirectories = func(_, to string) error {
		if err := os.Mkdir(to, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(to, "old.txt"), []byte("old"), 0o600); err != nil {
			t.Fatal(err)
		}
		return errExchangeUnsupported
	}
	t.Cleanup(func() { swapDirectories = exchangeDirectories })
	if _, err := replaceResultDirectory(staged, directory); !errors.Is(err, errExchangeUnsupported) {
		t.Fatalf("want refusal, got %v", err)
	}
	if got := readFile(t, directory, "old.txt"); got != "old" {
		t.Fatalf("previous bytes = %q", got)
	}
	if _, err := os.Stat(filepath.Join(directory, "new.txt")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("new file published: %v", err)
	}
}
