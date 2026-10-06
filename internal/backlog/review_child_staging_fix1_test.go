package backlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

// Snapshot all private evidence without following symlinks or opening special
// files. Equality includes inode, mode, regular bytes and link destinations.
// Access timestamps and physical SQLite pages are deliberately not compared.
func stageFix1Evidence(t *testing.T, namespace string) string {
	t.Helper()
	var records []string
	err := filepath.WalkDir(namespace, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		payload := ""
		if info.Mode().IsRegular() {
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			payload = fmt.Sprintf("%x", raw)
		} else if info.Mode()&os.ModeSymlink != 0 {
			payload, err = os.Readlink(path)
			if err != nil {
				return err
			}
		}
		inode := info.Sys().(*syscall.Stat_t).Ino
		records = append(records, fmt.Sprintf("%s %d %d %d %s", path, info.Mode(), info.Size(), inode, payload))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(records, "\n")
}

func stageFix1Mutate(t *testing.T, dir, kind string) {
	t.Helper()
	namespace := filepath.Dir(dir)
	target := filepath.Join(dir, "request.json")
	parts := strings.Split(kind, "/")
	switch parts[0] {
	case "receipt":
		target = filepath.Join(dir, "receipt.json")
	case "blob", "pending-blob":
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".blob") && !strings.HasPrefix(e.Name(), ".pending-") {
				target = filepath.Join(dir, e.Name())
				break
			}
		}
	case "owner":
		target = dir
	case "namespace":
		target = namespace
	case "locks":
		target = filepath.Join(namespace, ".locks")
	case "lock":
		target = filepath.Join(namespace, ".locks", filepath.Base(dir)+".lock")
	case "pending-request":
		target = filepath.Join(dir, ".pending-request.json")
	case "pending-receipt":
		target = filepath.Join(dir, ".pending-receipt.json")
	case "unknown", "bound":
		target = filepath.Join(dir, "unrecognized")
	}
	if parts[0] == "pending-blob" {
		target = filepath.Join(dir, ".pending-"+filepath.Base(target))
	}
	if strings.HasPrefix(parts[0], "pending-") {
		if _, err := os.Lstat(target); os.IsNotExist(err) {
			source := filepath.Join(dir, strings.TrimPrefix(filepath.Base(target), ".pending-"))
			raw, err := os.ReadFile(source)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(target, raw, 0400); err != nil {
				t.Fatal(err)
			}
		}
	}
	var err error
	switch parts[1] {
	case "missing":
		err = os.Remove(target)
	case "bytes":
		raw, e := os.ReadFile(target)
		if e != nil {
			t.Fatal(e)
		}
		raw[len(raw)-1] ^= 1 // Same size and restored canonical 0400 mode.
		if err = os.Chmod(target, 0600); err == nil {
			err = os.WriteFile(target, raw, 0400)
		}
		if err == nil {
			err = os.Chmod(target, 0400)
		}
	case "mode":
		mode := os.FileMode(0600)
		if parts[0] == "owner" || parts[0] == "namespace" || parts[0] == "locks" {
			mode = 0755
		}
		if parts[0] == "lock" {
			mode = 0400
		}
		err = os.Chmod(target, mode)
	case "special-mode":
		err = os.Chmod(target, 0400|os.ModeSetuid)
	case "symlink":
		if err = os.Rename(target, target+"-saved"); err == nil {
			err = os.Symlink(target+"-saved", target)
		}
	case "directory":
		if err = os.Remove(target); err == nil {
			err = os.Mkdir(target, 0400)
		}
	case "fifo":
		if err = os.Remove(target); err == nil {
			err = syscall.Mkfifo(target, 0400)
		}
	case "extra":
		if parts[0] == "bound" {
			entries, e := os.ReadDir(dir)
			if e != nil {
				t.Fatal(e)
			}
			for _, entry := range entries {
				raw, e := os.ReadFile(filepath.Join(dir, entry.Name()))
				if e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(filepath.Join(dir, ".pending-"+entry.Name()), raw, 0400); e != nil {
					t.Fatal(e)
				}
			}
		}
		err = os.WriteFile(target, []byte("unexpected retained evidence"), 0400)
	default:
		t.Fatal("unknown mutation", kind)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestReviewChildStagingFix1CompleteReplayMatrix(t *testing.T) {
	cases := []string{
		"request/missing", "request/bytes", "request/mode", "request/special-mode", "request/symlink", "request/directory", "request/fifo",
		"receipt/bytes", "receipt/mode", "receipt/symlink", "receipt/directory", "receipt/fifo",
		"blob/missing", "blob/mode", "blob/symlink", "blob/fifo",
		"owner/mode", "owner/symlink", "namespace/mode", "locks/mode", "locks/symlink", "lock/mode", "lock/symlink",
		"pending-request/bytes", "pending-receipt/bytes", "pending-blob/bytes", "pending-request/mode", "pending-receipt/symlink", "unknown/extra", "bound/extra",
	}
	for _, kind := range cases {
		t.Run(kind, func(t *testing.T) {
			f, o, req := newStageOwner(t)
			first, err := o.StageDeclared(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			dir := filepath.Dir(stageFile(t, o, first))
			// Valid pending evidence survives completed replay; damaged pending bytes
			// must not be ignored or removed even if the final file remains intact.
			if strings.HasPrefix(kind, "pending-") {
				name := strings.TrimPrefix(strings.Split(kind, "/")[0], "pending-")
				source := filepath.Join(dir, name+".json")
				if name == "blob" {
					source = stageFile(t, o, first)
				}
				raw, e := os.ReadFile(source)
				if e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(filepath.Join(dir, ".pending-"+filepath.Base(source)), raw, 0400); e != nil {
					t.Fatal(e)
				}
			}
			stageFix1Mutate(t, dir, kind)
			namespace := filepath.Dir(dir)
			evidence := stageFix1Evidence(t, namespace)
			db := stagingSQL(t, f)
			before := independentDeclaredTables(t, db)
			// Fresh migrated store attachment and owner, not an OS restart.
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
			_, err = fresh.StageDeclared(context.Background(), req)
			if err == nil {
				t.Fatal("complete corruption accepted", kind)
			}
			if evidence != stageFix1Evidence(t, namespace) {
				t.Fatal("refusal repaired or removed filesystem evidence")
			}
			if before != independentDeclaredTables(t, db) {
				t.Fatal("refusal changed logical SQL/native audit")
			}
			t.Logf("unchanged filesystem and all logical SQL/native audit; refusal: %v", err)
		})
	}
}

func TestReviewChildStagingFix1FinalReadOnlyMatrix(t *testing.T) {
	cases := []string{
		"request/missing", "request/bytes", "request/mode", "request/symlink", "request/directory", "request/fifo",
		"receipt/missing", "receipt/bytes", "receipt/mode", "receipt/symlink", "receipt/directory", "receipt/fifo",
		"blob/missing", "blob/bytes", "blob/mode", "blob/symlink",
		"owner/mode", "owner/symlink", "namespace/mode", "locks/mode", "locks/symlink", "lock/mode", "lock/missing", "lock/symlink",
		"unknown/extra", "bound/extra", "pending-request/bytes", "pending-receipt/mode", "pending-blob/symlink",
	}
	for _, boundary := range []string{"receipt-retained", "after-prepare-build-current-deadline"} {
		for _, kind := range cases {
			t.Run(boundary+"/"+kind, func(t *testing.T) {
				f, o, req := newStageOwner(t)
				db := stagingSQL(t, f)
				var evidence, before, namespace string
				fired := false
				mutate := func() {
					if fired {
						return
					}
					fired = true
					namespace = filepath.Join(o.admission.Artifacts.SubmissionRoot, childStageNamespace)
					entries, err := os.ReadDir(namespace)
					if err != nil {
						t.Fatal(err)
					}
					var dir string
					for _, e := range entries {
						if e.Name() != ".locks" {
							dir = filepath.Join(namespace, e.Name())
							break
						}
					}
					if dir == "" {
						t.Fatal("missing actual owner")
					}
					stageFix1Mutate(t, dir, kind)
					evidence = stageFix1Evidence(t, namespace)
					before = independentDeclaredTables(t, db)
				}
				if boundary == "receipt-retained" {
					o.fault = func(phase string) error {
						if phase == boundary {
							mutate()
						}
						return nil
					}
				} else {
					calls := 0
					o.now = func() time.Time {
						calls++
						if calls == 2 {
							mutate()
						} // final deadline callback follows Prepare/Build/current guard
						return time.Now()
					}
				}
				_, err := o.StageDeclared(context.Background(), req)
				if !fired || err == nil {
					t.Fatal("late corruption returned preparation", fired, err)
				}
				if evidence != stageFix1Evidence(t, namespace) {
					t.Fatal("final check repaired or removed evidence")
				}
				if before != independentDeclaredTables(t, db) {
					t.Fatal("late refusal changed all logical SQL/native audit")
				}
				t.Logf("real late boundary, no filesystem repair/all logical SQL unchanged: %v", err)
			})
		}
	}
}

func TestReviewChildStagingFix1PendingRecovery(t *testing.T) {
	for _, kind := range []string{"intent", "file", "receipt", "complete-pending"} {
		t.Run(kind, func(t *testing.T) {
			f, o, req := newStageOwner(t)
			ctx := context.Background()
			if kind == "complete-pending" {
				first, err := o.StageDeclared(ctx, req)
				if err != nil {
					t.Fatal(err)
				}
				dir := filepath.Dir(stageFile(t, o, first))
				raw, err := os.ReadFile(filepath.Join(dir, "request.json"))
				if err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(filepath.Join(dir, ".pending-request.json"), raw, 0400); err != nil {
					t.Fatal(err)
				}
				evidence := stageFix1Evidence(t, filepath.Dir(dir))
				before := independentDeclaredTables(t, stagingSQL(t, f))
				again, err := o.StageDeclared(ctx, req)
				if err != nil || !reflect.DeepEqual(first, again) {
					t.Fatal("valid complete pending replay", err)
				}
				if evidence != stageFix1Evidence(t, filepath.Dir(dir)) {
					t.Fatal("complete replay removed pending evidence")
				}
				if before != independentDeclaredTables(t, stagingSQL(t, f)) {
					t.Fatal("complete replay changed SQL")
				}
				return
			}
			fired := false
			var stableRaw []byte
			o.fault = func(phase string) error {
				match := kind == "intent" && phase == "request-retained" || kind == "file" && strings.HasPrefix(phase, "file-retained:") || kind == "receipt" && phase == "receipt-retained"
				if !match || fired {
					return nil
				}
				fired = true
				namespace := filepath.Join(o.admission.Artifacts.SubmissionRoot, childStageNamespace)
				entries, err := os.ReadDir(namespace)
				if err != nil {
					t.Fatal(err)
				}
				var dir string
				for _, e := range entries {
					if e.Name() != ".locks" {
						dir = filepath.Join(namespace, e.Name())
						break
					}
				}
				stableRaw, err = os.ReadFile(filepath.Join(dir, "request.json"))
				if err != nil {
					t.Fatal(err)
				}
				name := "request.json"
				if kind == "receipt" {
					name = "receipt.json"
				}
				if kind == "file" {
					entries, err := os.ReadDir(dir)
					if err != nil {
						t.Fatal(err)
					}
					for _, e := range entries {
						if strings.HasSuffix(e.Name(), ".blob") && !strings.HasPrefix(e.Name(), ".pending-") {
							name = e.Name()
							break
						}
					}
				}
				// Reconstruct the real no-replace publication boundary before final link.
				if err = os.Rename(filepath.Join(dir, name), filepath.Join(dir, ".pending-"+name)); err != nil {
					t.Fatal(err)
				}
				return errors.New("interrupted pending publication")
			}
			if _, err := o.StageDeclared(ctx, req); err == nil || !fired {
				t.Fatal("missing real interruption", err)
			}
			before := independentDeclaredTables(t, stagingSQL(t, f))
			fresh, err := NewDeclaredReviewStaging(o.admission, o.store, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			changed := req
			changed.Deadline = req.Deadline.Add(time.Minute)
			if _, err = fresh.StageDeclared(ctx, changed); err == nil {
				t.Fatal("pending first deadline extended")
			}
			if before != independentDeclaredTables(t, stagingSQL(t, f)) {
				t.Fatal("conflicting recovery changed SQL")
			}
			got, err := fresh.StageDeclared(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(got.Receipt)
			if err != nil {
				t.Fatal(err)
			}
			if string(raw) != string(stableRaw) || got.Receipt.Deadline != req.Deadline {
				t.Fatal("recovery changed first retained intent/deadline")
			}
			if before != independentDeclaredTables(t, stagingSQL(t, f)) {
				t.Fatal("exact recovery changed logical SQL/native audit")
			}
			if _, err = got.Preparation.Build(got.Admission.Authority, got.Checkpoint, got.Receipt.CreatedAt); err != nil {
				t.Fatal(err)
			}
			t.Log("exact pending publication recovery; stable intent/deadline; all logical SQL/native audit unchanged")
		})
	}
}
