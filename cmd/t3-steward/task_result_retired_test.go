package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// Synthetic retirement timestamps avoid sleeps while exercising the real
// on-disk naming and reclamation rules.
func retiredResultFixture(t *testing.T, directory string, when time.Time, content string) string {
	t.Helper()
	path := resultAgeName(filepath.Join(filepath.Dir(directory), retiredResultPrefix(directory)+fmt.Sprintf("%020d-fixture", when.UnixNano())), when)
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if content != "" {
		if err := os.WriteFile(filepath.Join(path, "final-message.md"), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return path
}

func publishResultFixture(t *testing.T, directory, content string) {
	t.Helper()
	staged, err := stageResultDirectory(directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(staged) })
	if err := os.WriteFile(filepath.Join(staged, "final-message.md"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if left, err := publishResultDirectory(staged, directory); err != nil || len(left) != 0 {
		t.Fatalf("publish: left %v, err %v", left, err)
	}
}

// All siblings must be identifiable retirements, never abandoned staging.
// Once the grace window passes, only the newest K may remain.
func assertResultRetentionAfterGrace(t *testing.T, directory string, want int) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Dir(directory))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		if entry.Name() == filepath.Base(directory) {
			continue
		}
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), retiredResultPrefix(directory)) {
			t.Fatalf("unexpected sibling %q", entry.Name())
		}
		names = append(names, entry.Name())
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	if len(names) < want {
		t.Fatalf("only %d retired generations, want at least %d", len(names), want)
	}
	if left := removeInterruptedCollections("", directory, time.Now().Add(2*interruptedCollectionAge)); len(left) != 0 {
		t.Fatalf("sweep leftovers: %v", left)
	}
	entries, err = os.ReadDir(filepath.Dir(directory))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != want+1 {
		t.Fatalf("after grace: %d siblings, want public plus %d retired", len(entries), want)
	}
	for i, name := range names {
		_, err := os.Stat(filepath.Join(filepath.Dir(directory), name))
		if i < want && err != nil {
			t.Fatalf("newest generation %q lost: %v", name, err)
		}
		if i >= want && !os.IsNotExist(err) {
			t.Fatalf("old generation %q survived: %v", name, err)
		}
	}
}

func TestRapidResultPublishesRetainGraceWindowThenBoundByK(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "task")
	const publications = 2*resultRetainedGenerations + 3
	for n := 0; n < publications; n++ {
		publishResultFixture(t, directory, fmt.Sprintf("whole collection %d", n))
	}
	// The newest K always survive, regardless of publication speed. The grace
	// gate may retain additional generations; advancing sweep time is deterministic.
	assertResultRetentionAfterGrace(t, directory, resultRetainedGenerations)
	if got := readFile(t, directory, "final-message.md"); got != fmt.Sprintf("whole collection %d", publications-1) {
		t.Fatalf("public collection changed: %q", got)
	}
}

func TestResultRetirementReclaimedByPublishAndSweep(t *testing.T) {
	for _, sweep := range []bool{false, true} {
		t.Run(fmt.Sprintf("sweep=%v", sweep), func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "task")
			now := time.Now()
			var paths []string
			for n := 0; n < resultRetainedGenerations+3; n++ {
				paths = append(paths, retiredResultFixture(t, directory, now.Add(-2*resultRetirementGrace+time.Duration(n)), "old whole file"))
			}
			if sweep {
				if left := removeInterruptedCollections("", directory, now); len(left) != 0 {
					t.Fatalf("sweep: %v", left)
				}
			} else {
				publishResultFixture(t, directory, "new whole file")
			}
			for n, path := range paths {
				_, err := os.Stat(path)
				if n < 3 && !os.IsNotExist(err) {
					t.Fatalf("eligible old generation survived: %s, %v", path, err)
				}
				if n >= 3 && err != nil {
					t.Fatalf("newest K generation lost: %s, %v", path, err)
				}
			}
		})
	}
}

func TestResultRetirementRequiresBothSafetyGates(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "task")
	now := time.Now()
	old := retiredResultFixture(t, directory, now.Add(-2*resultRetirementGrace), "old")
	recent := retiredResultFixture(t, directory, now.Add(-resultRetirementGrace/2), "recent")
	for n := 0; n < resultRetainedGenerations; n++ {
		retiredResultFixture(t, directory, now.Add(time.Duration(n)), "newest")
	}
	// An old collection mtime is irrelevant: retirement, not production, starts
	// the grace period. The interrupted sweep must not bypass the retirement gate.
	ancient := now.Add(-2 * interruptedCollectionAge)
	if err := os.Chtimes(recent, ancient, ancient); err != nil {
		t.Fatal(err)
	}
	if left := removeInterruptedCollections("", directory, now); len(left) != 0 {
		t.Fatal(left)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatalf("eligible generation survived: %v", err)
	}
	if got := readFile(t, recent, "final-message.md"); got != "recent" {
		t.Fatalf("grace generation changed: %q", got)
	}
	if left := removeInterruptedCollections("", directory, now.Add(resultRetirementGrace)); len(left) != 0 {
		t.Fatal(left)
	}
	if _, err := os.Stat(recent); !os.IsNotExist(err) {
		t.Fatalf("generation outside K survived grace expiry: %v", err)
	}
}

func TestResultReaderHoldingOldDirectoryAcrossPublishReadsWholeFile(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "task")
	want := strings.Repeat("old whole collection\n", 1024)
	publishResultFixture(t, directory, want)
	old, err := os.Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	publishResultFixture(t, directory, "new whole collection")
	// Resolve the child only AFTER publication through the old directory handle.
	// Immediate RemoveAll would unlink the child and make this fail with ENOENT.
	fd, err := unix.Openat(int(old.Fd()), "final-message.md", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	file := os.NewFile(uintptr(fd), "old final message")
	defer file.Close()
	got, err := io.ReadAll(file)
	if err != nil || string(got) != want {
		t.Fatalf("old read: %d bytes, err %v", len(got), err)
	}
	if got := readFile(t, directory, "final-message.md"); got != "new whole collection" {
		t.Fatalf("public file: %q", got)
	}
}

// Publication is complete even when the retirement rename fails. In particular,
// collect's deferred cleanup must not delete the exchanged-out generation.
func TestResultCollectionKeepsRetirementWhenItsRenameFails(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root renames in read-only directories")
	}
	f := newTaskResultFixture(t)
	base := t.TempDir()
	task := f.detail.Tasks[0]
	if _, err := f.cli().collect(context.Background(), f.detail, task, base, true); err != nil {
		t.Fatal(err)
	}
	swapDirectories = func(from, to string) error {
		if err := exchangeDirectories(from, to); err != nil {
			return err
		}
		// Exchange succeeds, but the following retirement rename is refused.
		return os.Chmod(base, 0o500)
	}
	t.Cleanup(func() {
		swapDirectories = exchangeDirectories
		_ = os.Chmod(base, 0o700)
	})
	for id := range f.content {
		f.content[id] = "new whole file"
	}
	f.stderr.Reset()
	if _, err := f.cli().collect(context.Background(), f.detail, task, base, true); err != nil {
		t.Fatalf("completed publication refused: %v", err)
	}
	if got := readFile(t, base, task.Task.Name, "final-message.md"); got != "new whole file" {
		t.Fatalf("public file: %q", got)
	}
	entries, err := os.ReadDir(base)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("previous generation not retained: %v", entries)
	}
	for _, entry := range entries {
		if entry.Name() == task.Task.Name {
			continue
		}
		old := filepath.Join(base, entry.Name())
		if got := readFile(t, old, "final-message.md"); got != "the job" {
			t.Fatalf("previous generation: %q", got)
		}
		if !strings.Contains(f.stderr.String(), old) {
			t.Fatalf("unretired leftover not reported: %q", f.stderr.String())
		}
		// A subsequent sweep, including after grace and interrupted-staging
		// age expire, must treat a failed rename as a newest-K retirement.
		if err := os.Chmod(base, 0o700); err != nil {
			t.Fatal(err)
		}
		recoveryTime := time.Now().Add(2 * interruptedCollectionAge)
		if left := removeInterruptedCollections("", filepath.Join(base, task.Task.Name), recoveryTime); len(left) != 0 {
			t.Fatal(left)
		}
		if got := readFile(t, resultAgeName(old, recoveryTime), "final-message.md"); got != "the job" {
			t.Fatalf("failed-rename retirement lost to sweep: %q", got)
		}
	}
}

func TestResultRetirementsAreDistinctFromStagingAndOtherTasks(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "task")
	other := filepath.Join(filepath.Dir(directory), "task-other")
	now := time.Now()
	staged, err := stageResultDirectory(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staged, "in-progress"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	otherRetired := retiredResultFixture(t, other, now.Add(-2*resultRetirementGrace), "other")
	for n := 0; n < resultRetainedGenerations+1; n++ {
		retiredResultFixture(t, directory, now.Add(-2*resultRetirementGrace+time.Duration(n)), "retired")
	}
	publishResultFixture(t, directory, "public")
	if got := readFile(t, staged, "in-progress"); got != "partial" {
		t.Fatalf("in-progress staging changed: %q", got)
	}
	if got := readFile(t, otherRetired, "final-message.md"); got != "other" {
		t.Fatalf("other task retirement changed: %q", got)
	}
	if got := readFile(t, directory, "final-message.md"); got != "public" {
		t.Fatalf("retirement mistaken for public collection: %q", got)
	}
}
