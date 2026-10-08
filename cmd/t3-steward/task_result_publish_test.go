package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// A reader of a task's result directory always finds a complete collection
// while a new one is published: the public directory is never missing, and its
// final message is always one collection's whole file. Moving the old
// directory aside and then renaming the new one in left a moment with no
// directory at all, and a crash in that moment left none until the next
// collection.
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

// On a filesystem that cannot exchange two directories, a new collection still
// replaces the earlier one completely, with nothing left beside it.
func TestACollectionReplacesTheEarlierOneWhereDirectoriesCannotBeExchanged(t *testing.T) {
	swapDirectories = func(string, string) error { return errExchangeUnsupported }
	t.Cleanup(func() { swapDirectories = exchangeDirectories })
	runDirectory := filepath.Join(t.TempDir(), "run-1")
	directory := filepath.Join(runDirectory, "task")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "old.txt"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
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
	if got := readFile(t, directory, "final-message.md"); got != "new" {
		t.Fatalf("final message = %q", got)
	}
	if _, err := os.Stat(filepath.Join(directory, "old.txt")); !os.IsNotExist(err) {
		t.Fatalf("the earlier collection's file survived: %v", err)
	}
	entries, err := os.ReadDir(runDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("run directory holds %d entries, want only the task directory", len(entries))
	}
}
