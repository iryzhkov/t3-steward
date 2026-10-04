package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRetiredBacklogCommandsHaveNoFileOrStdinEffects(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "existing"}[existing], func(t *testing.T) {
			root := t.TempDir()
			drop := filepath.Join(root, "drop")
			state := filepath.Join(root, "state.db")
			raw := []byte("historical task bytes\n")
			if existing {
				if err := os.Mkdir(drop, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(drop, "retained.md"), raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			input, err := os.CreateTemp(root, "stdin")
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()
			if _, err := input.Write(raw); err != nil {
				t.Fatal(err)
			}
			if _, err := input.Seek(0, 0); err != nil {
				t.Fatal(err)
			}
			oldStdin := os.Stdin
			os.Stdin = input
			defer func() { os.Stdin = oldStdin }()
			cfgPath := filepath.Join(root, "config.yaml")
			cfg := "state_path: " + state + "\nbacklog:\n  enabled: false\n  dir: " + drop + "\n  default_host: remote-must-not-be-contacted\nreport:\n  remotes: [remote-must-not-be-contacted]\n"
			if err := os.WriteFile(cfgPath, []byte(cfg), 0600); err != nil {
				t.Fatal(err)
			}
			for _, args := range [][]string{{"new", "../escape"}, {"path"}, {"check", "-"}, {"check", filepath.Join(drop, "retained.md")}, {"receive", "incoming"}, {"list", "--all"}, {"list", "--all", "--json"}} {
				err := cmdBacklog(globalFlags{configPath: cfgPath}, args)
				if err == nil || !strings.Contains(err.Error(), "retired") || !strings.Contains(err.Error(), "task run or campaign submit") {
					t.Fatalf("%v: %v", args, err)
				}
			}
			offset, err := input.Seek(0, 1)
			if err != nil || offset != 0 {
				t.Fatalf("stdin read: offset=%d err=%v", offset, err)
			}
			if _, err := os.Stat(state); !os.IsNotExist(err) {
				t.Fatalf("state opened/created: %v", err)
			}
			if existing {
				entries, err := os.ReadDir(drop)
				if err != nil || len(entries) != 1 {
					t.Fatalf("drop changed: %v %v", entries, err)
				}
				got, err := os.ReadFile(filepath.Join(drop, "retained.md"))
				if err != nil || string(got) != string(raw) {
					t.Fatalf("historical bytes changed: %q %v", got, err)
				}
			} else if _, err := os.Stat(drop); !os.IsNotExist(err) {
				t.Fatalf("missing directory created: %v", err)
			}
		})
	}
}

func TestRetiredBacklogRefusalDoesNotNeedConfiguration(t *testing.T) {
	for _, args := range [][]string{{"new", "id"}, {"path"}, {"check", "-"}, {"receive", "id"}, {"list", "--all"}} {
		err := cmdBacklog(globalFlags{configPath: filepath.Join(t.TempDir(), "missing.yaml")}, args)
		if err == nil || !strings.Contains(err.Error(), "retired") {
			t.Fatalf("%v: %v", args, err)
		}
	}
}
