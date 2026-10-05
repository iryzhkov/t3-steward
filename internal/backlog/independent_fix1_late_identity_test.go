package backlog

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestIndependentFix1LateIdentity(t *testing.T) {
	for _, kind := range []string{"owner-replaced", "lock-replaced", "lock-bytes", "lock-special", "namespace-special", "locks-special", "owner-special", "pending-conflict"} {
		t.Run(kind, func(t *testing.T) {
			f, o, req := newStageOwner(t)
			first, err := o.StageDeclared(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			dir := filepath.Dir(stageFile(t, o, first))
			namespace := filepath.Dir(dir)
			lock := filepath.Join(namespace, ".locks", filepath.Base(dir)+".lock")
			db := stagingSQL(t, f)
			var evidence, sqlBefore string
			calls := 0
			o.now = func() time.Time {
				calls++
				if calls == 2 {
					switch kind {
					case "owner-replaced":
						saved := dir + "-saved"
						if err := os.Rename(dir, saved); err != nil {
							t.Fatal(err)
						}
						if err := os.Mkdir(dir, 0700); err != nil {
							t.Fatal(err)
						}
						entries, err := os.ReadDir(saved)
						if err != nil {
							t.Fatal(err)
						}
						for _, e := range entries {
							raw, err := os.ReadFile(filepath.Join(saved, e.Name()))
							if err != nil {
								t.Fatal(err)
							}
							if err := os.WriteFile(filepath.Join(dir, e.Name()), raw, 0400); err != nil {
								t.Fatal(err)
							}
						}
					case "lock-replaced":
						if err := os.Rename(lock, lock+"-saved"); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(lock, nil, 0600); err != nil {
							t.Fatal(err)
						}
					case "lock-bytes":
						if err := os.WriteFile(lock, []byte("x"), 0600); err != nil {
							t.Fatal(err)
						}
					case "lock-special":
						if err := os.Chmod(lock, 0600|os.ModeSetuid); err != nil {
							t.Fatal(err)
						}
					case "namespace-special", "locks-special", "owner-special":
						target := namespace
						if kind == "locks-special" {
							target = filepath.Join(namespace, ".locks")
						}
						if kind == "owner-special" {
							target = dir
						}
						if err := os.Chmod(target, 0700|os.ModeSticky); err != nil {
							t.Fatal(err)
						}
					case "pending-conflict":
						raw, err := os.ReadFile(filepath.Join(dir, "receipt.json"))
						if err != nil {
							t.Fatal(err)
						}
						raw[len(raw)-1] ^= 1
						if err := os.WriteFile(filepath.Join(dir, ".pending-receipt.json"), raw, 0400); err != nil {
							t.Fatal(err)
						}
					}
					evidence = stageFix1Evidence(t, namespace)
					sqlBefore = independentDeclaredTables(t, db)
				}
				return time.Now()
			}
			_, err = o.StageDeclared(context.Background(), req)
			if calls != 2 || err == nil || !strings.Contains(err.Error(), "phase final-custody") {
				t.Fatalf("late replay accepted or wrong boundary: calls=%d err=%v", calls, err)
			}
			if evidence != stageFix1Evidence(t, namespace) {
				t.Fatal("final refusal changed private evidence")
			}
			if sqlBefore != independentDeclaredTables(t, db) {
				t.Fatal("final refusal changed logical SQL/native audit")
			}
			t.Logf("unchanged private filesystem and all logical SQL; %v", err)
		})
	}
}
