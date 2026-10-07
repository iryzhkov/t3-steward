//go:build unix

package workerruntime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// snapshotOutcome runs SnapshotWorkInProgress in the background and waits at
// most wait for it. A snapshot still blocked on the FIFO at path by then is
// released by opening and closing a writer, awaited, and reported as blocked,
// so no test leaves a goroutine waiting on a pipe.
func snapshotOutcome(t *testing.T, f wipFixture, ctx context.Context, path string, wait time.Duration) (summary string, err error, blocked bool) {
	t.Helper()
	type outcome struct {
		summary string
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		summary, err := f.driver.SnapshotWorkInProgress(ctx, f.pkg, f.workspace)
		done <- outcome{summary, err}
	}()
	select {
	case got := <-done:
		return got.summary, got.err, false
	case <-time.After(wait):
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case got := <-done:
			return got.summary, got.err, true
		default:
		}
		// A nonblocking writer opens only while a reader waits; until then
		// the open fails with ENXIO and is tried again.
		if writer, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0); err == nil {
			writer.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot did not finish after releasing %s", path)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A task can replace any file under .git with a named pipe and leave no
// writer. Opening one for reading waits for a writer, and os.Root does not
// change that, so the snapshot, which runs on the worker host while a failure
// waits to publish, must refuse such a file without opening it into a wait,
// whatever its caller's context. This is review round 1's reproduction, with
// the other metadata files the snapshot reads.
func TestSnapshotRefusesSpecialFilesInGitMetadataWithoutBlocking(t *testing.T) {
	for _, tc := range []struct {
		name string
		// prepare readies the repository so that the snapshot reads name.
		prepare func(t *testing.T, f wipFixture)
	}{
		{name: "HEAD"},
		{name: "info/exclude"},
		{name: "index"},
		{name: "shallow"},
		{name: "refs/heads/main"},
		{name: "packed-refs", prepare: func(t *testing.T, f wipFixture) {
			runGitForTest(t, f.workspace, "pack-refs", "--all")
			if _, err := os.Lstat(filepath.Join(f.workspace, ".git", "refs", "heads", "main")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("main is still a loose ref: %v", err)
			}
		}},
	} {
		for _, deadline := range []bool{false, true} {
			t.Run(strings.ReplaceAll(tc.name, "/", "_")+map[bool]string{false: "/no deadline", true: "/deadline"}[deadline], func(t *testing.T) {
				f := newWIPFixture(t, commitOutputs)
				writeTestFile(t, filepath.Join(f.workspace, "a.txt"), "changed\n")
				if tc.prepare != nil {
					tc.prepare(t, f)
				}
				path := filepath.Join(f.workspace, ".git", filepath.FromSlash(tc.name))
				if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
					t.Fatal(err)
				}
				if err := syscall.Mkfifo(path, 0o600); err != nil {
					t.Fatal(err)
				}
				ctx := context.Background()
				if deadline {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
					defer cancel()
				}
				_, err, blocked := snapshotOutcome(t, f, ctx, path, 2*time.Second)
				if blocked {
					t.Fatalf("snapshot blocked opening .git/%s; a task-controlled FIFO holds up failure collection", tc.name)
				}
				if err == nil {
					t.Fatalf("snapshot accepted a FIFO at .git/%s", tc.name)
				}
				if _, statErr := os.Stat(filepath.Join(f.driver.workspacePath(f.pkg), WorkInProgressBundleFile)); !errors.Is(statErr, os.ErrNotExist) {
					t.Fatalf("a bundle was retained from a refused repository: %v", statErr)
				}
			})
		}
	}
}

// The base pin under .t3 is read by the snapshot too. A FIFO there costs the
// bundle its base exclusion, never the snapshot's return.
func TestSnapshotDoesNotBlockOnASpecialBasePin(t *testing.T) {
	f := newWIPFixture(t, commitOutputs)
	writeTestFile(t, filepath.Join(f.workspace, "a.txt"), "changed\n")
	path := filepath.Join(f.workspace, ".t3", "base-commit")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	summary, err, blocked := snapshotOutcome(t, f, context.Background(), path, 2*time.Second)
	if blocked {
		t.Fatal("snapshot blocked reading a FIFO base pin")
	}
	if err != nil || !strings.HasPrefix(summary, "wip.bundle retained") {
		t.Fatalf("summary = %q, err = %v", summary, err)
	}
}

// The snapshot's Git reads the work tree, and Git itself waits on a FIFO
// named .gitignore or .gitattributes, or on a nested repository's HEAD; a
// task may still be creating these while the snapshot runs. Git is stopped
// when the snapshot's own time is up, even when its caller's context has no
// deadline, such as the reconcile pass holding the worker's lock.
func TestSnapshotStopsGitThatWaitsOnTheWorkTree(t *testing.T) {
	for _, name := range []string{".gitignore", ".gitattributes", "nested/.git/HEAD"} {
		t.Run(strings.ReplaceAll(name, "/", "_"), func(t *testing.T) {
			f := newWIPFixture(t, commitOutputs)
			f.driver.Config.SnapshotTimeout = 300 * time.Millisecond
			writeTestFile(t, filepath.Join(f.workspace, "a.txt"), "changed\n")
			path := filepath.Join(f.workspace, filepath.FromSlash(name))
			if strings.HasPrefix(name, "nested/") {
				for _, dir := range []string{"objects", "refs"} {
					if err := os.MkdirAll(filepath.Join(f.workspace, "nested", ".git", dir), 0o700); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := syscall.Mkfifo(path, 0o600); err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			_, err, blocked := snapshotOutcome(t, f, context.Background(), path, 3*time.Second)
			if blocked {
				t.Fatalf("snapshot waited on %s past its own time", name)
			}
			if err == nil || !strings.Contains(err.Error(), "did not finish within") {
				t.Fatalf("err = %v, want the snapshot stopped at its time limit", err)
			}
			if elapsed := time.Since(started); elapsed > 2*time.Second {
				t.Fatalf("snapshot took %s to stop", elapsed)
			}
		})
	}
}

// The open behind every metadata read refuses a file that became a FIFO after
// it was checked, from the opened descriptor, without waiting for a writer.
func TestOpenRootFileRefusesAFIFOFromTheDescriptor(t *testing.T) {
	dir := t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(dir, "pipe"), 0o600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	done := make(chan error, 1)
	go func() {
		f, _, err := openRootNonblocking(root, "pipe", false)
		if f != nil {
			f.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, errFileType) {
			t.Fatalf("err = %v, want errFileType", err)
		}
	case <-time.After(2 * time.Second):
		if writer, err := os.OpenFile(filepath.Join(dir, "pipe"), os.O_WRONLY|syscall.O_NONBLOCK, 0); err == nil {
			writer.Close()
		}
		<-done
		t.Fatal("opening a FIFO waited for a writer")
	}
	if _, _, err := openRootNonblocking(root, "pipe", true); !errors.Is(err, errFileType) {
		t.Fatalf("directory open of a FIFO: err = %v", err)
	}
}
