package backlog

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

func TestIndependentFix3LoserCancelPreservesHeldWinner(t *testing.T) {
	ns := filepath.Join(stageOwnedTempDir(t), "stages")
	if e := os.Mkdir(ns, 0700); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var winner *fileLock
	var winning os.FileInfo
	var candidate string
	waited := false
	loser, err := childStageLockWithBoundary(ctx, ns, "same", func(phase string) error {
		switch phase {
		case "lock-private-held":
			entries, e := os.ReadDir(filepath.Join(ns, ".locks"))
			if e != nil {
				return e
			}
			if len(entries) != 1 {
				t.Fatalf("private entries: %v", entries)
			}
			candidate = filepath.Join(ns, ".locks", entries[0].Name())
			winner, e = childStageLock(ctx, ns, "same")
			if e != nil {
				return e
			}
			winning, e = winner.file.Stat()
			return e
		case "lock-waiting":
			waited = true
			if _, e := os.Lstat(candidate); !errors.Is(e, os.ErrNotExist) {
				t.Fatalf("loser candidate not cleaned before wait: %v", e)
			}
			cancel()
		}
		return nil
	})
	if winner == nil {
		t.Fatal("winner never acquired", err)
	}
	defer winner.Close()
	if loser != nil {
		loser.Close()
		t.Fatal("canceled loser acquired")
	}
	if !waited || !errors.Is(err, context.Canceled) {
		t.Fatal("wrong cancellation", waited, err)
	}
	current, e := os.Lstat(filepath.Join(ns, ".locks", "same.lock"))
	if e != nil || !os.SameFile(winning, current) {
		t.Fatal("winner replaced", e)
	}
	probe, e := os.OpenFile(filepath.Join(ns, ".locks", "same.lock"), os.O_RDWR, 0)
	if e != nil {
		t.Fatal(e)
	}
	e = syscall.Flock(int(probe.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	probe.Close()
	if !errors.Is(e, syscall.EWOULDBLOCK) {
		t.Fatal("loser unlocked winner", e)
	}
	evidence := stageFix1Evidence(t, ns)
	if e = winner.Close(); e != nil {
		t.Fatal(e)
	}
	next, e := childStageLock(context.Background(), ns, "same")
	if e != nil {
		t.Fatal(e)
	}
	defer next.Close()
	info, e := next.file.Stat()
	if e != nil || !os.SameFile(winning, info) || evidence != stageFix1Evidence(t, ns) {
		t.Fatal("reopen changed custody", e)
	}
}

func TestIndependentFix3PrivateReplacementPreserved(t *testing.T) {
	for _, kind := range []string{"candidate", "directory"} {
		t.Run(kind, func(t *testing.T) {
			ns := filepath.Join(stageOwnedTempDir(t), "stages")
			if e := os.Mkdir(ns, 0700); e != nil {
				t.Fatal(e)
			}
			var evidence string
			var moved string
			lock, err := childStageLockWithBoundary(context.Background(), ns, "same", func(phase string) error {
				if phase != "lock-private-held" {
					return nil
				}
				locks := filepath.Join(ns, ".locks")
				entries, e := os.ReadDir(locks)
				if e != nil {
					return e
				}
				if len(entries) != 1 {
					t.Fatalf("entries: %v", entries)
				}
				candidate := filepath.Join(locks, entries[0].Name())
				if kind == "candidate" {
					moved = candidate + ".saved"
					if e = os.Rename(candidate, moved); e != nil {
						return e
					}
					if e = os.WriteFile(candidate, []byte("replacement evidence"), 0600); e != nil {
						return e
					}
				} else {
					moved = filepath.Join(ns, "saved-locks", entries[0].Name())
					if e = os.Rename(locks, filepath.Join(ns, "saved-locks")); e != nil {
						return e
					}
					if e = os.Mkdir(locks, 0700); e != nil {
						return e
					}
					if e = os.WriteFile(candidate, nil, 0600); e != nil {
						return e
					}
				}
				evidence = stageFix1Evidence(t, ns)
				return nil
			})
			if lock != nil {
				lock.Close()
				t.Fatal("replacement accepted")
			}
			if err == nil || !strings.Contains(err.Error(), "identity refused") {
				t.Fatal("wrong refusal", err)
			}
			if evidence == "" || evidence != stageFix1Evidence(t, ns) {
				t.Fatal("replacement or moved evidence deleted")
			}
			// The abandoned descriptor must be closed even on cleanup refusal.
			f, e := os.OpenFile(moved, os.O_RDWR, 0)
			if e != nil {
				t.Fatal(e)
			}
			defer f.Close()
			if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
				t.Fatal("private descriptor leaked locked", e)
			}
			if _, e = os.Lstat(filepath.Join(ns, ".locks", "same.lock")); !errors.Is(e, os.ErrNotExist) {
				t.Fatal("bad publication", e)
			}
		})
	}
}

func TestIndependentFix3PublishedCancellationRemainsOwnerless(t *testing.T) {
	ns := filepath.Join(stageOwnedTempDir(t), "stages")
	if e := os.Mkdir(ns, 0700); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var original os.FileInfo
	lock, err := childStageLockWithBoundary(ctx, ns, "same", func(phase string) error {
		if phase == "lock-published-held" {
			var e error
			original, e = os.Lstat(filepath.Join(ns, ".locks", "same.lock"))
			if e != nil {
				return e
			}
			cancel()
		}
		return nil
	})
	if lock != nil {
		lock.Close()
		t.Fatal("cancel accepted")
	}
	if !errors.Is(err, context.Canceled) || original == nil {
		t.Fatal("wrong cancellation", err)
	}
	before := stageFix1Evidence(t, ns)
	lock, err = childStageLock(context.Background(), ns, "same")
	if lock != nil {
		lock.Close()
		t.Fatal("ownerless adopted")
	}
	if err == nil || !strings.Contains(err.Error(), "existing owner missing") {
		t.Fatal("wrong replay refusal", err)
	}
	current, e := os.Lstat(filepath.Join(ns, ".locks", "same.lock"))
	if e != nil || !os.SameFile(original, current) || before != stageFix1Evidence(t, ns) {
		t.Fatal("released custody changed", e)
	}
}
