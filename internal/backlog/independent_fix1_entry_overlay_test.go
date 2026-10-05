package backlog

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestIndependentFix1EntryLockEvidence(t *testing.T) {
	for _, kind := range []string{"missing-lock", "missing-locks", "damaged-intent-missing-lock", "damaged-intent-missing-locks"} {
		t.Run(kind, func(t *testing.T) {
			f, o, req := newStageOwner(t)
			first, err := o.StageDeclared(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			dir := filepath.Dir(stageFile(t, o, first))
			namespace := filepath.Dir(dir)
			locks := filepath.Join(namespace, ".locks")
			name := filepath.Join(locks, first.Checkpoint.Key()+".lock")
			if err = os.Remove(name); err != nil {
				t.Fatal(err)
			}
			if kind == "missing-locks" || kind == "damaged-intent-missing-locks" {
				if err = os.Remove(locks); err != nil {
					t.Fatal(err)
				}
			}
			damaged := kind == "damaged-intent-missing-lock" || kind == "damaged-intent-missing-locks"
			if damaged {
				if err = os.Remove(filepath.Join(dir, "request.json")); err != nil {
					t.Fatal(err)
				}
			}
			beforeFS := stageFix1Evidence(t, namespace)
			beforeSQL := independentDeclaredTables(t, stagingSQL(t, f))
			fresh, err := NewDeclaredReviewStaging(o.admission, o.store, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			got, err := fresh.StageDeclared(context.Background(), req)
			sameFS := beforeFS == stageFix1Evidence(t, namespace)
			_, lockErr := os.Lstat(name)
			sameSQL := beforeSQL == independentDeclaredTables(t, stagingSQL(t, f))
			t.Logf("damaged=%v replay=%v lock-recreated=%v private-evidence-unchanged=%v SQL-unchanged=%v receipt-equal=%v", damaged, err, lockErr == nil, sameFS, sameSQL, reflect.DeepEqual(first.Receipt, got.Receipt))
			if !sameSQL {
				t.Fatal("SQL mutated")
			}
			// Refusal of damaged COMPLETE evidence must preserve all private evidence.
			if damaged && (err == nil || !sameFS) {
				t.Error("completed damaged-stage refusal recreated private custody evidence")
			}
		})
	}
}

func TestIndependentFix1HeldLockReplacement(t *testing.T) {
	for _, kind := range []string{"lock", "owner"} {
		t.Run(kind, func(t *testing.T) {
			f, o, req := newStageOwner(t)
			db := stagingSQL(t, f)
			fired := false
			var beforeFS, beforeSQL, namespace string
			calls := 0
			o.now = func() time.Time {
				calls++
				if calls == 2 {
					fired = true
					namespace = filepath.Join(o.admission.Artifacts.SubmissionRoot, childStageNamespace)
					entries, e := os.ReadDir(namespace)
					if e != nil {
						t.Fatal(e)
					}
					var dir string
					for _, entry := range entries {
						if entry.Name() != ".locks" {
							dir = filepath.Join(namespace, entry.Name())
							break
						}
					}
					if kind == "lock" {
						p := filepath.Join(namespace, ".locks", filepath.Base(dir)+".lock")
						if e = os.Rename(p, p+"-held"); e != nil {
							t.Fatal(e)
						}
						if e = os.WriteFile(p, nil, 0600); e != nil {
							t.Fatal(e)
						}
					} else {
						if e = os.Rename(dir, dir+"-opened"); e != nil {
							t.Fatal(e)
						}
						if e = os.Mkdir(dir, 0700); e != nil {
							t.Fatal(e)
						}
						files, e := os.ReadDir(dir + "-opened")
						if e != nil {
							t.Fatal(e)
						}
						for _, file := range files {
							raw, e := os.ReadFile(filepath.Join(dir+"-opened", file.Name()))
							if e != nil {
								t.Fatal(e)
							}
							if e = os.WriteFile(filepath.Join(dir, file.Name()), raw, 0400); e != nil {
								t.Fatal(e)
							}
						}
					}
					beforeFS = stageFix1Evidence(t, namespace)
					beforeSQL = independentDeclaredTables(t, db)
				}
				return time.Now()
			}
			_, err := o.StageDeclared(context.Background(), req)
			if !fired || err == nil {
				t.Fatal("same-mode replacement returned preparation", fired, err)
			}
			if beforeFS != stageFix1Evidence(t, namespace) || beforeSQL != independentDeclaredTables(t, db) {
				t.Fatal("refusal mutated evidence")
			}
			t.Logf("same-mode same-bytes %s replacement refused without repair: %v", kind, err)
		})
	}
}
