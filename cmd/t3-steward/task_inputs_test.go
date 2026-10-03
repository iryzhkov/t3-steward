package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestTaskRunPinsLocalInputs(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		t.Run(map[bool]string{false: "git", true: "fresh"}[fresh], func(t *testing.T) {
			sourceRoot, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			file := filepath.Join(sourceRoot, "plan.md")
			if err := os.WriteFile(file, []byte("frozen plan"), 0600); err != nil {
				t.Fatal(err)
			}
			second := filepath.Join(sourceRoot, "criteria.md")
			if err := os.WriteFile(second, []byte("criteria"), 0600); err != nil {
				t.Fatal(err)
			}
			h := newTaskRunHarness()
			args := []string{"--project", "steward", "--model", "t3-primary/opus", "--input", file, "--input", second, "--no-notify", "--json"}
			if fresh {
				args = append(args, "--fresh")
			}
			args = append(args, "--", "read .t3/inputs/plan.md")
			if err := h.cli().run(context.Background(), args); err != nil {
				t.Fatal(err)
			}
			if len(h.archives) != 1 {
				t.Fatal("no submission")
			}
			manifest, files := h.manifest(t)
			if len(manifest.Inputs) != 2 || files["plan.md"] != "frozen plan" || files["criteria.md"] != "criteria" {
				t.Fatalf("inputs %v files %v", manifest.Inputs, files)
			}
			var record map[string]json.RawMessage
			if err := json.Unmarshal(h.stdout.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			if len(record["inputManifest"]) == 0 {
				t.Fatal("submission omitted input manifest")
			}
		})
	}
}
