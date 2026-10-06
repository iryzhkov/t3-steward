// Package buildgate holds tests of the repository's Makefile gates. It has no
// code of its own.
package buildgate

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const noSQLiteCheckptr = "-gcflags=modernc.org/...=-d=checkptr=0"

// fakeGo stands in for the go command: it records every invocation and answers
// go list as if the module held the packages m/a and m/b.
const fakeGo = `#!/bin/sh
printf '%s\n' "$*" >> "$GO_LOG"
case "$1" in
list)
	shift
	if [ "$1" = ./... ]; then printf 'm/a\nm/b\n'; else for p; do printf 'm/%s\n' "${p#./}"; done; fi
	;;
env)
	echo /nonexistent
	;;
esac
exit 0
`

// TestGateRacePassKeepsCheckptrByDefault runs each local gate target of the
// real Makefile against a scratch repository whose package a changed since the
// base commit, with a fake go command, and checks the flags of the race pass.
// check-fast and check-review must keep checkptr in every package, modernc.org
// included; only the explicitly named opt-in targets may turn it off there.
func TestGateRacePassKeepsCheckptrByDefault(t *testing.T) {
	for _, tool := range []string{"make", "git", "sh"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is not installed: %v", tool, err)
		}
	}
	root := repositoryRoot(t)
	dir := t.TempDir()
	copyFile(t, filepath.Join(root, "Makefile"), filepath.Join(dir, "Makefile"))
	copyFile(t, filepath.Join(root, "scripts", "changed-go-packages.sh"), filepath.Join(dir, "scripts", "changed-go-packages.sh"))
	writeFile(t, filepath.Join(dir, "a", "a.go"), "package a\n")
	writeFile(t, filepath.Join(dir, "b", "b.go"), "package b\n")
	bin := filepath.Join(dir, "fakebin")
	writeFile(t, filepath.Join(bin, "go"), fakeGo)
	if err := os.Chmod(filepath.Join(bin, "go"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(dir, ".gitignore"), "fakebin/\n*.log\n")

	env := gateEnv(bin)
	run := func(name string, args ...string) string {
		t.Helper()
		cmd := exec.Command(name, args...)
		cmd.Dir = dir
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s %v: %v\n%s", name, args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	run("git", "init", "-q")
	run("git", "add", "-A")
	run("git", "commit", "-q", "-m", "base")
	base := run("git", "rev-parse", "HEAD")
	writeFile(t, filepath.Join(dir, "a", "a.go"), "package a\n\nconst Changed = true\n")
	run("git", "commit", "-q", "-am", "change a")

	for _, tc := range []struct {
		target   string
		short    bool
		checkptr bool
	}{
		{target: "check-fast", short: true, checkptr: true},
		{target: "check-review", short: false, checkptr: true},
		{target: "check-fast-no-sqlite-checkptr", short: true, checkptr: false},
		{target: "check-review-no-sqlite-checkptr", short: false, checkptr: false},
	} {
		t.Run(tc.target, func(t *testing.T) {
			log := filepath.Join(dir, tc.target+".log")
			cmd := exec.Command("make", "--no-print-directory", tc.target, "FAST_BASE="+base, "MAKE=true")
			cmd.Dir = dir
			cmd.Env = append(env, "GO_LOG="+log)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("make %s: %v\n%s", tc.target, err, out)
			}
			data, err := os.ReadFile(log)
			if err != nil {
				t.Fatal(err)
			}
			var race []string
			for _, line := range strings.Split(string(data), "\n") {
				if strings.HasPrefix(line, "test ") && strings.Contains(line, "-race") {
					race = append(race, line)
				}
			}
			if len(race) != 1 {
				t.Fatalf("want exactly one race invocation, got %q in\n%s", race, data)
			}
			fields := strings.Fields(race[0])
			if got := fields[len(fields)-1]; got != "./a" {
				t.Errorf("race pass runs %q, want the changed package ./a: %s", got, race[0])
			}
			if got := strings.Contains(race[0], " -short "); got != tc.short {
				t.Errorf("race pass -short = %v, want %v: %s", got, tc.short, race[0])
			}
			if tc.checkptr {
				if strings.Contains(race[0], "checkptr") || strings.Contains(race[0], "-gcflags") {
					t.Errorf("default gate %s changes checkptr in its race pass: %s", tc.target, race[0])
				}
			} else if !strings.Contains(race[0], noSQLiteCheckptr) {
				t.Errorf("opt-in gate %s race pass lacks %s: %s", tc.target, noSQLiteCheckptr, race[0])
			}
		})
	}
}

// gateEnv is the test process's environment with the fake go first on PATH and
// without anything an enclosing make or the caller could pass down to change
// the gate's variables.
func gateEnv(bin string) []string {
	env := []string{
		"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
		"GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@example.invalid",
		"GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@example.invalid",
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1",
	}
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		switch {
		case name == "PATH", strings.HasPrefix(name, "GIT_"), strings.HasPrefix(name, "MAKE"),
			name == "MFLAGS", name == "GNUMAKEFLAGS", name == "RACE_GCFLAGS", name == "FAST_BASE":
			continue
		}
		env = append(env, kv)
	}
	return env
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func copyFile(t *testing.T, from, to string) {
	t.Helper()
	data, err := os.ReadFile(from)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, to, string(data))
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
