package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTaskRunInputChangesDefaultKey(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "plan.md")
	run := func(content string) taskRunRecord {
		t.Helper()
		if err := os.WriteFile(file, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		h := newTaskRunHarness()
		if err := h.run("--project", "steward", "--model", "t3-primary/opus", "--input", file, "--json", "--no-notify", "--", "review"); err != nil {
			t.Fatal(err)
		}
		return h.record(t)
	}
	a := run("one")
	b := run("two")
	c := run("two")
	if a.IdempotencyKey == b.IdempotencyKey || b.IdempotencyKey != c.IdempotencyKey || a.InputManifest.Digest == b.InputManifest.Digest {
		t.Fatal("input bytes did not determine the key")
	}
}
