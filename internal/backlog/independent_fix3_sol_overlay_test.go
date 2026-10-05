package backlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIndependentFix3PrivateIdentity(t *testing.T) {
	for _, kind := range []string{"private-replaced", "directory-substituted"} {
		t.Run(kind, func(t *testing.T) {
			ns := filepath.Join(stageOwnedTempDir(t), "stages")
			if err := os.Mkdir(ns, 0700); err != nil {
				t.Fatal(err)
			}
			locks := filepath.Join(ns, ".locks")
			var evidence string
			var saved string
			lock, err := childStageLockWithBoundary(context.Background(), ns, "same", func(phase string) error {
				if phase != "lock-private-held" {
					return nil
				}
				entries, e := os.ReadDir(locks)
				if e != nil || len(entries) != 1 {
					t.Fatal(e, entries)
				}
				private := filepath.Join(locks, entries[0].Name())
				if kind == "private-replaced" {
					saved = private + ".saved"
					if e = os.Rename(private, saved); e != nil {
						t.Fatal(e)
					}
					if e = os.WriteFile(private, []byte("replacement-evidence"), 0600); e != nil {
						t.Fatal(e)
					}
				} else {
					saved = locks + ".saved"
					if e = os.Rename(locks, saved); e != nil {
						t.Fatal(e)
					}
					if e = os.Mkdir(locks, 0700); e != nil {
						t.Fatal(e)
					}
					if e = os.WriteFile(private, []byte("replacement-evidence"), 0600); e != nil {
						t.Fatal(e)
					}
				}
				evidence = stageFix1Evidence(t, ns)
				return nil
			})
			if lock != nil {
				lock.Close()
				t.Fatal("substituted private custody accepted")
			}
			if err == nil || !strings.Contains(err.Error(), "identity refused") {
				t.Fatal("wrong refusal", err)
			}
			if evidence != stageFix1Evidence(t, ns) {
				t.Fatal("refusal deleted or mutated replaced evidence")
			}
			if _, e := os.Lstat(saved); e != nil {
				t.Fatal("original evidence lost", e)
			}
			if _, e := os.Lstat(filepath.Join(locks, "same.lock")); !errors.Is(e, os.ErrNotExist) {
				t.Fatal("canonical unexpectedly published", e)
			}
			if _, e := os.Lstat(filepath.Join(ns, "same")); !errors.Is(e, os.ErrNotExist) {
				t.Fatal("owner unexpectedly created", e)
			}
		})
	}
}
