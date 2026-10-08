package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// Each boundary advances an injected clock; no scheduler delays or retries.
func TestResultRetirementGraceStartsAfterExchangeOrRecovery(t *testing.T) {
	for _, mode := range []string{"publish", "crash", "rename-failure"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Now()
			resultRetirementNow = func() time.Time { return now }
			t.Cleanup(func() {
				resultRetirementNow = time.Now
				swapDirectories = exchangeDirectories
				renameResultRetirement = os.Rename
			})
			directory := filepath.Join(t.TempDir(), "task")
			publishResultFixture(t, directory, "old whole file")
			old, err := os.Open(directory)
			if err != nil {
				t.Fatal(err)
			}
			defer old.Close()

			if mode == "crash" {
				// The child reserves an ancient order then exits just after exchange.
				output, err := resultPublicationChild(t, directory, "after-exchange").CombinedOutput()
				if err != nil {
					t.Fatalf("crash: %v: %s", err, output)
				}
				now = now.Add(3 * interruptedCollectionAge)
			} else {
				swapDirectories = func(from, to string) error {
					// Pause beyond grace BEFORE the real exchange.
					now = now.Add(2 * resultRetirementGrace)
					return exchangeDirectories(from, to)
				}
				if mode == "rename-failure" {
					renameResultRetirement = func(string, string) error { return fmt.Errorf("injected rename failure") }
				}
				staged, err := stageResultDirectory(directory)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(staged, "final-message.md"), []byte("new whole file"), 0600); err != nil {
					t.Fatal(err)
				}
				left, err := publishResultDirectory(staged, directory)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "publish" && len(left) != 0 {
					t.Fatal(left)
				}
				if mode == "rename-failure" && len(left) == 0 {
					t.Fatal("rename failure not reported")
				}
				swapDirectories = exchangeDirectories
				// Recovery happens much later than exchange; preparation age is unusable.
				if mode == "rename-failure" {
					now = now.Add(3 * interruptedCollectionAge)
				}
			}
			renameResultRetirement = os.Rename
			// Model a sweep descheduled after sampling its time but before locking.
			if left := removeInterruptedCollections("", directory, now.Add(-3*interruptedCollectionAge)); len(left) != 0 {
				t.Fatal(left)
			}
			for n := 0; n < resultRetainedGenerations; n++ {
				publishResultFixture(t, directory, "burst whole file")
			}
			readHeldResult(t, old, "old whole file")
			// Just before the full grace expires, a sweep must still preserve it.
			if left := removeInterruptedCollections("", directory, now.Add(resultRetirementGrace-time.Nanosecond)); len(left) != 0 {
				t.Fatal(left)
			}
			readHeldResult(t, old, "old whole file")
			// At expiry it is outside newest K and must be reclaimed.
			if left := removeInterruptedCollections("", directory, now.Add(resultRetirementGrace)); len(left) != 0 {
				t.Fatal(left)
			}
			fd, err := unix.Openat(int(old.Fd()), "final-message.md", unix.O_RDONLY|unix.O_CLOEXEC, 0)
			if err == nil {
				unix.Close(fd)
			}
			if !os.IsNotExist(err) {
				t.Fatalf("expired generation child open: %v", err)
			}
		})
	}
}
