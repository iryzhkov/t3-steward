package backlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

func TestReviewChildStagingFix2EntryMatrix(t *testing.T) {
	cases := []string{"lock/missing", "locks/missing", "lock/fifo", "lock/directory", "lock/symlink", "lock/mode", "lock/special-mode", "lock/bytes", "locks/symlink", "locks/mode", "locks/special-mode", "owner/missing", "owner/symlink", "owner/mode", "namespace/mode"}
	for _, damaged := range []bool{false, true} {
		for _, kind := range cases {
			t.Run(map[bool]string{false: "intact", true: "damaged"}[damaged]+"/"+kind, func(t *testing.T) {
				t.Parallel()
				f, o, req := newStageOwner(t)
				first, err := o.StageDeclared(context.Background(), req)
				if err != nil {
					t.Fatal(err)
				}
				dir := filepath.Dir(stageFile(t, o, first))
				namespace := filepath.Dir(dir)
				if damaged {
					if err := os.Remove(filepath.Join(dir, "request.json")); err != nil {
						t.Fatal(err)
					}
				}
				switch kind {
				case "locks/missing":
					if err := os.Remove(filepath.Join(namespace, ".locks", first.Checkpoint.Key()+".lock")); err != nil {
						t.Fatal(err)
					}
					if err := os.Remove(filepath.Join(namespace, ".locks")); err != nil {
						t.Fatal(err)
					}
				case "owner/missing":
					if err := os.Rename(dir, dir+"-saved"); err != nil {
						t.Fatal(err)
					}
				case "lock/bytes":
					if err := os.WriteFile(filepath.Join(namespace, ".locks", first.Checkpoint.Key()+".lock"), []byte("x"), 0600); err != nil {
						t.Fatal(err)
					}
				case "lock/special-mode", "locks/special-mode":
					target, mode := filepath.Join(namespace, ".locks", first.Checkpoint.Key()+".lock"), os.FileMode(0600|os.ModeSetuid)
					if strings.HasPrefix(kind, "locks/") {
						target, mode = filepath.Join(namespace, ".locks"), 0700|os.ModeSticky
					}
					if err := os.Chmod(target, mode); err != nil {
						t.Fatal(err)
					}
				default:
					stageFix1Mutate(t, dir, kind)
				}
				evidence := stageFix1Evidence(t, namespace)
				before := independentDeclaredTables(t, stagingSQL(t, f))
				reopened, err := sqlitetest.OpenMigrated(f.store.dbPath)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				admission := o.admission
				admission.Store = reopened
				admission.Artifacts.Catalog = reopened
				fresh, err := NewDeclaredReviewStaging(admission, reopened, time.Now)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_, err = fresh.StageDeclared(ctx, req)
				if err == nil || errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("invalid entry accepted or blocked", err)
				}
				if evidence != stageFix1Evidence(t, namespace) {
					t.Fatal("entry refusal repaired filesystem evidence")
				}
				if before != independentDeclaredTables(t, stagingSQL(t, f)) {
					t.Fatal("entry refusal changed logical SQL/native audit")
				}
				t.Logf("reopened entry refuses without private FS or logical SQL repair: %v", err)
			})
		}
	}
}

func TestReviewChildStagingFix2LockedOwnerRemoval(t *testing.T) {
	f, o, req := newStageOwner(t)
	first, err := o.StageDeclared(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Dir(stageFile(t, o, first))
	namespace := filepath.Dir(dir)
	var evidence, before string
	fired := false
	o.fault = func(phase string) error {
		if phase == "owner-locked" {
			fired = true
			if err := os.Rename(dir, dir+"-saved"); err != nil {
				t.Fatal(err)
			}
			evidence = stageFix1Evidence(t, namespace)
			before = independentDeclaredTables(t, stagingSQL(t, f))
		}
		return nil
	}
	_, err = o.StageDeclared(context.Background(), req)
	if !fired || err == nil {
		t.Fatal("owner removed under held lock accepted", err)
	}
	if evidence != stageFix1Evidence(t, namespace) || before != independentDeclaredTables(t, stagingSQL(t, f)) {
		t.Fatal("post-lock refusal recreated owner or changed SQL")
	}
}

func TestReviewChildStagingFix2InitialCustody(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(map[bool]string{false: "identical", true: "conflicting"}[conflict], func(t *testing.T) {
			f, o, req := newStageOwner(t)
			locked, release := make(chan struct{}), make(chan struct{})
			o.fault = func(phase string) error {
				if phase == "owner-locked" {
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
			secondCh := make(chan outcome, 1)
			go func() { got, err := fresh.StageDeclared(context.Background(), other); secondCh <- outcome{got, err} }()
			close(release)
			first, second := <-firstCh, <-secondCh
			if first.err != nil {
				t.Fatal(first.err)
			}
			if conflict {
				if second.err == nil {
					t.Fatal("conflicting loser accepted")
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

func TestReviewChildStagingFix2EmptyOwnerCrashRecovery(t *testing.T) {
	f, o, req := newStageOwner(t)
	o.fault = func(phase string) error {
		if phase == "owner-locked" {
			return errors.New("crash after locked empty owner")
		}
		return nil
	}
	if _, err := o.StageDeclared(context.Background(), req); err == nil {
		t.Fatal("interruption missing")
	}
	before := independentDeclaredTables(t, stagingSQL(t, f))
	namespace := filepath.Join(o.admission.Artifacts.SubmissionRoot, childStageNamespace)
	entries, err := os.ReadDir(filepath.Join(namespace, ".locks"))
	if err != nil || len(entries) != 1 {
		t.Fatal(err)
	}
	lockPath := filepath.Join(namespace, ".locks", entries[0].Name())
	inode, err := os.Lstat(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := NewDeclaredReviewStaging(o.admission, o.store, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fresh.StageDeclared(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	current, err := os.Lstat(lockPath)
	if err != nil || !os.SameFile(inode, current) {
		t.Fatal("crash recovery replaced lock", err)
	}
	if before != independentDeclaredTables(t, stagingSQL(t, f)) || got.Receipt.Deadline != req.Deadline {
		t.Fatal("recovery changed allocation/deadline")
	}
	if _, err := got.Preparation.Build(got.Admission.Authority, got.Checkpoint, got.Receipt.CreatedAt); err != nil {
		t.Fatal(err)
	}
}

func TestReviewChildStagingFix2AmbiguousLockOnly(t *testing.T) {
	f, o, req := newStageOwner(t)
	o.fault = func(phase string) error {
		if phase == "owner-locked" {
			return errors.New("interrupted before intent")
		}
		return nil
	}
	if _, err := o.StageDeclared(context.Background(), req); err == nil {
		t.Fatal("interruption missing")
	}
	namespace := filepath.Join(o.admission.Artifacts.SubmissionRoot, childStageNamespace)
	entries, err := os.ReadDir(namespace)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != ".locks" {
			if err := os.Remove(filepath.Join(namespace, e.Name())); err != nil {
				t.Fatal(err)
			}
		}
	}
	evidence := stageFix1Evidence(t, namespace)
	before := independentDeclaredTables(t, stagingSQL(t, f))
	fresh, err := NewDeclaredReviewStaging(o.admission, o.store, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fresh.StageDeclared(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "existing owner missing") {
		t.Fatal("ambiguous lock-only evidence adopted", err)
	}
	if evidence != stageFix1Evidence(t, namespace) || before != independentDeclaredTables(t, stagingSQL(t, f)) {
		t.Fatal("ambiguous crash repaired evidence")
	}
	// Also prove the held lock descriptor still has the required native mode.
	locks, err := os.ReadDir(filepath.Join(namespace, ".locks"))
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(filepath.Join(namespace, ".locks", locks[0].Name()))
	if err != nil || info.Mode() != 0600 || info.Sys().(*syscall.Stat_t).Ino == 0 {
		t.Fatal(err)
	}
}
