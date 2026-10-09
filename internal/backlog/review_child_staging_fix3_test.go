package backlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/testtiming"
)

func TestReviewChildStagingFix3PublishedBeforeOwner(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(map[bool]string{false: "identical", true: "conflicting"}[conflict], func(t *testing.T) {
			f, o, req := newStageOwner(t)
			locked, release := make(chan struct{}), make(chan struct{})
			o.fault = func(phase string) error {
				if phase == "lock-published-held" {
					close(locked)
					<-release
				}
				return nil
			}
			type outcome struct {
				got DeclaredChildStageResult
				err error
			}
			firstCh := make(chan outcome, 1)
			go func() { got, err := o.StageDeclared(context.Background(), req); firstCh <- outcome{got, err} }()
			<-locked
			namespace := filepath.Join(o.admission.Artifacts.SubmissionRoot, childStageNamespace)
			entries, err := os.ReadDir(filepath.Join(namespace, ".locks"))
			if err != nil || len(entries) != 1 {
				t.Fatal(err, entries)
			}
			lockPath := filepath.Join(namespace, ".locks", entries[0].Name())
			ownerPath := filepath.Join(namespace, strings.TrimSuffix(entries[0].Name(), ".lock"))
			if _, err := os.Lstat(ownerPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("owner already visible at publication barrier", err)
			}
			fd, err := syscall.Open(lockPath, syscall.O_RDWR|syscall.O_NOFOLLOW, 0)
			if err != nil {
				t.Fatal(err)
			}
			probeErr := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
			syscall.Close(fd)
			if !errors.Is(probeErr, syscall.EWOULDBLOCK) {
				t.Fatal("creator did not own visible inode", probeErr)
			}
			inode, err := os.Lstat(lockPath)
			if err != nil {
				t.Fatal(err)
			}
			// Actual exclusive flock, before the first intent exists. Timeout cannot
			// create or replace the lock, owner, metadata or SQL.
			evidence := stageFix1Evidence(t, namespace)
			before := independentDeclaredTables(t, stagingSQL(t, f))
			fresh, err := NewDeclaredReviewStaging(o.admission, o.store, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_, err = fresh.StageDeclared(ctx, req)
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("first owner not excluded", err)
			}
			if evidence != stageFix1Evidence(t, namespace) || before != independentDeclaredTables(t, stagingSQL(t, f)) {
				t.Fatal("waiting caller mutated partial evidence")
			}
			other := req
			if conflict {
				other.Deadline = other.Deadline.Add(time.Minute)
			}
			waiting := make(chan struct{})
			secondLocked, continueSecond := make(chan struct{}), make(chan struct{})
			var once sync.Once
			fresh.fault = func(phase string) error {
				if phase == "lock-waiting" {
					once.Do(func() { close(waiting) })
				}
				if phase == "owner-locked" {
					close(secondLocked)
					<-continueSecond
				}
				return nil
			}
			secondCh := make(chan outcome, 1)
			go func() { got, err := fresh.StageDeclared(context.Background(), other); secondCh <- outcome{got, err} }()
			select {
			case <-waiting:
			case early := <-secondCh:
				t.Fatal("caller did not wait at real publication boundary", early.err)
			case <-time.After(testtiming.Bound(5 * time.Second)):
				t.Fatal("caller never reached held lock")
			}
			if evidence != stageFix1Evidence(t, namespace) || before != independentDeclaredTables(t, stagingSQL(t, f)) {
				t.Fatal("waiting caller mutated published custody")
			}
			close(release)
			first := <-firstCh
			if first.err != nil {
				t.Fatal(first.err)
			}
			<-secondLocked
			winnerEvidence := stageFix1Evidence(t, namespace)
			winnerSQL := independentDeclaredTables(t, stagingSQL(t, f))
			close(continueSecond)
			second := <-secondCh
			fresh.fault = nil
			if winnerEvidence != stageFix1Evidence(t, namespace) || winnerSQL != independentDeclaredTables(t, stagingSQL(t, f)) {
				t.Fatal("second caller mutated complete winner FS or logical SQL/native audit")
			}
			if conflict {
				if second.err == nil || !strings.Contains(second.err.Error(), "conflict") {
					t.Fatal("conflicting loser accepted or wrong refusal", second.err)
				}
			} else if second.err != nil || !reflect.DeepEqual(first.got, second.got) {
				t.Fatal("identical caller lost exact winner", second.err)
			}
			current, err := os.Lstat(lockPath)
			if err != nil || !os.SameFile(inode, current) {
				t.Fatal("concurrent lock inode replaced", err)
			}
			evidence = stageFix1Evidence(t, namespace)
			before = independentDeclaredTables(t, stagingSQL(t, f))
			got, err := fresh.StageDeclared(context.Background(), req)
			if err != nil || !reflect.DeepEqual(first.got, got) || got.Receipt.Deadline != req.Deadline {
				t.Fatal("winner or deadline changed", err)
			}
			if evidence != stageFix1Evidence(t, namespace) || before != independentDeclaredTables(t, stagingSQL(t, f)) {
				t.Fatal("exact winner replay mutated evidence")
			}
		})
	}
}

func TestReviewChildStagingFix3PrivatePublication(t *testing.T) {
	t.Run("private-contenders", func(t *testing.T) {
		ns := filepath.Join(stageOwnedTempDir(t), "stages")
		if e := os.Mkdir(ns, 0700); e != nil {
			t.Fatal(e)
		}
		worker := startStageLockTestWorker(t, ns, "same", "lock-private-held")
		if e := worker.waitBoundary(); e != nil {
			t.Fatal(e)
		}
		locks := filepath.Join(ns, ".locks")
		entries, e := os.ReadDir(locks)
		if e != nil || len(entries) != 1 || !strings.HasPrefix(entries[0].Name(), ".private-lock-") {
			t.Fatal("private candidate absent", entries, e)
		}
		privatePath := filepath.Join(locks, entries[0].Name())
		info, e := os.Lstat(privatePath)
		if e != nil || info.Mode() != 0600 || info.Size() != 0 {
			t.Fatal("private mode/size", e)
		}
		if _, e = os.Lstat(filepath.Join(locks, "same.lock")); !errors.Is(e, os.ErrNotExist) {
			t.Fatal("published before lock", e)
		}
		winner, e := childStageLock(context.Background(), ns, "same")
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { _ = winner.Close() })
		winningInfo, e := winner.file.Stat()
		if e != nil {
			t.Fatal(e)
		}
		if e = winner.Close(); e != nil {
			t.Fatal(e)
		}
		worker.unblock()
		loser, e := worker.result()
		if e != nil {
			t.Fatal(e)
		}
		defer loser.Close()
		got, e := loser.file.Stat()
		if e != nil || !os.SameFile(got, winningInfo) {
			t.Fatal("loser did not reopen winner", e)
		}
		entries, e = os.ReadDir(locks)
		if e != nil || len(entries) != 1 || entries[0].Name() != "same.lock" {
			t.Fatal("candidate leaked", entries, e)
		}
	})
	for _, kind := range []string{"cancel", "fault", "link-failure"} {
		t.Run(kind, func(t *testing.T) {
			ns := filepath.Join(stageOwnedTempDir(t), "stages")
			if e := os.Mkdir(ns, 0700); e != nil {
				t.Fatal(e)
			}
			locks := filepath.Join(ns, ".locks")
			if e := os.Mkdir(locks, 0700); e != nil {
				t.Fatal(e)
			}
			// An unrelated crash leftover must survive every ordinary cleanup.
			stale := filepath.Join(locks, ".private-lock-crash-leftover")
			if e := os.WriteFile(stale, nil, 0600); e != nil {
				t.Fatal(e)
			}
			snapshot := stageFix1Evidence(t, ns)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if kind == "link-failure" {
				if _, _, e := childStagePublishLock(ctx, locks, "missing/lock", nil); e == nil {
					t.Fatal("real link failure accepted")
				}
			} else {
				_, e := childStageLockWithBoundary(ctx, ns, "same", func(p string) error {
					if p == "lock-private-held" {
						if kind == "cancel" {
							cancel()
						} else {
							return errors.New("private boundary interrupted")
						}
					}
					return nil
				})
				if e == nil || (kind == "cancel" && !errors.Is(e, context.Canceled)) {
					t.Fatal("interruption accepted", e)
				}
			}
			if snapshot != stageFix1Evidence(t, ns) {
				t.Fatal("cleanup mutated unrelated evidence or leaked candidate")
			}
		})
	}
}

func TestReviewChildStagingFix3PublishedCrash(t *testing.T) {
	f, o, req := newStageOwner(t)
	o.fault = func(p string) error {
		if p == "lock-published-held" {
			return errors.New("crash before owner")
		}
		return nil
	}
	if _, e := o.StageDeclared(context.Background(), req); e == nil {
		t.Fatal("interruption absent")
	}
	ns := filepath.Join(o.admission.Artifacts.SubmissionRoot, childStageNamespace)
	entries, e := os.ReadDir(filepath.Join(ns, ".locks"))
	if e != nil || len(entries) != 1 || !strings.HasSuffix(entries[0].Name(), ".lock") {
		t.Fatal("canonical or cleanup wrong", e)
	}
	evidence := stageFix1Evidence(t, ns)
	before := independentDeclaredTables(t, stagingSQL(t, f))
	o.fault = nil
	if _, e = o.StageDeclared(context.Background(), req); e == nil || !strings.Contains(e.Error(), "existing owner missing") {
		t.Fatal("ambiguous publication crash adopted", e)
	}
	if evidence != stageFix1Evidence(t, ns) || before != independentDeclaredTables(t, stagingSQL(t, f)) {
		t.Fatal("crash custody repaired")
	}
}

func TestReviewChildStagingFix3PostWaitIdentity(t *testing.T) {
	ns := filepath.Join(stageOwnedTempDir(t), "stages")
	if e := os.Mkdir(ns, 0700); e != nil {
		t.Fatal(e)
	}
	held, e := childStageLock(context.Background(), ns, "same")
	if e != nil {
		t.Fatal(e)
	}
	waiting := make(chan struct{})
	var once sync.Once
	result := make(chan error, 1)
	go func() {
		l, e := childStageLockWithBoundary(context.Background(), ns, "same", func(p string) error {
			if p == "lock-waiting" {
				once.Do(func() { close(waiting) })
			}
			return nil
		})
		if l != nil {
			l.Close()
		}
		result <- e
	}()
	<-waiting
	path := filepath.Join(ns, ".locks", "same.lock")
	if e = os.Rename(path, path+".saved"); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(path, nil, 0600); e != nil {
		t.Fatal(e)
	}
	evidence := stageFix1Evidence(t, ns)
	if e = held.Close(); e != nil {
		t.Fatal(e)
	}
	if e = <-result; e == nil || !strings.Contains(e.Error(), "opened owner lock changed") {
		t.Fatal("post-wait replacement adopted", e)
	}
	if evidence != stageFix1Evidence(t, ns) {
		t.Fatal("post-wait refusal repaired evidence")
	}
}
