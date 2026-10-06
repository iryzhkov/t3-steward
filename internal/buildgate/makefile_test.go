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
// go list as if the module held the packages m/a and m/b. With
// FAKE_GO_FAIL_LIST_ALL set, go list ./... fails as a real one does when it
// cannot load the module.
const fakeGo = `#!/bin/sh
printf '%s\n' "$*" >> "$GO_LOG"
case "$1" in
list)
	shift
	if [ "$1" = ./... ]; then
		if [ -n "${FAKE_GO_FAIL_LIST_ALL:-}" ]; then echo 'go: cannot load module' >&2; exit 1; fi
		printf 'm/a\nm/b\n'
	else
		for p; do printf 'm/%s\n' "${p#./}"; done
	fi
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
	dir, base := gateRepository(t)
	env := gateEnv(filepath.Join(dir, "fakebin"))

	for _, tc := range []struct {
		target   string
		short    bool
		checkptr bool
		extra    []string
	}{
		{target: "check-fast", short: true, checkptr: true},
		{target: "check-review", short: false, checkptr: true},
		{target: "check-fast-no-sqlite-checkptr", short: true, checkptr: false},
		{target: "check-review-no-sqlite-checkptr", short: false, checkptr: false},
		// A RACE_GCFLAGS left in the environment by some other tool must not
		// silently turn checkptr off in the default gate.
		{target: "check-review", short: false, checkptr: true, extra: []string{"RACE_GCFLAGS=-gcflags=all=-d=checkptr=0"}},
	} {
		name := tc.target
		if tc.extra != nil {
			name += "/with-environment"
		}
		t.Run(name, func(t *testing.T) {
			log := filepath.Join(t.TempDir(), "go.log")
			cmd := exec.Command("make", "--no-print-directory", tc.target, "FAST_BASE="+base, "MAKE=true")
			cmd.Dir = dir
			cmd.Env = append(append(append([]string(nil), env...), "GO_LOG="+log), tc.extra...)
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

// TestGateFailsWhenThePackageListFails checks that every gate target fails,
// rather than skipping its short pass and passing, when go list ./... fails.
func TestGateFailsWhenThePackageListFails(t *testing.T) {
	dir, base := gateRepository(t)
	env := gateEnv(filepath.Join(dir, "fakebin"))
	for _, target := range []string{"check-fast", "check-review", "check-fast-no-sqlite-checkptr", "check-review-no-sqlite-checkptr"} {
		t.Run(target, func(t *testing.T) {
			log := filepath.Join(t.TempDir(), "go.log")
			cmd := exec.Command("make", "--no-print-directory", target, "FAST_BASE="+base, "MAKE=true")
			cmd.Dir = dir
			cmd.Env = append(append([]string(nil), env...), "GO_LOG="+log, "FAKE_GO_FAIL_LIST_ALL=1")
			out, err := cmd.CombinedOutput()
			if err == nil {
				data, _ := os.ReadFile(log)
				t.Fatalf("make %s passed although go list ./... failed\n%s\ngo invocations:\n%s", target, out, data)
			}
		})
	}
}

// TestGateRunsOnlyTheRacePassWhenEveryPackageChanged checks the other side of
// the package-list check: when every package changed, the empty short pass is
// not an error, and the race pass runs every package.
func TestGateRunsOnlyTheRacePassWhenEveryPackageChanged(t *testing.T) {
	dir, base := gateRepository(t)
	env := gateEnv(filepath.Join(dir, "fakebin"))
	writeFile(t, filepath.Join(dir, "b", "b.go"), "package b\n\nconst Changed = true\n")
	git := exec.Command("git", "commit", "-q", "-am", "change b")
	git.Dir = dir
	git.Env = env
	if out, err := git.CombinedOutput(); err != nil {
		t.Fatalf("git commit: %v\n%s", err, out)
	}
	log := filepath.Join(t.TempDir(), "go.log")
	cmd := exec.Command("make", "--no-print-directory", "check-review", "FAST_BASE="+base, "MAKE=true")
	cmd.Dir = dir
	cmd.Env = append(append([]string(nil), env...), "GO_LOG="+log)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("make check-review: %v\n%s", err, out)
	}
	data, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "test -short") {
		t.Errorf("short pass ran although every package is in the race pass:\n%s", data)
	}
	if !strings.Contains(string(data), "test -race -timeout 25m ./a ./b\n") {
		t.Errorf("race pass does not run both changed packages:\n%s", data)
	}
}

// gateRepository builds a scratch git repository holding the real Makefile,
// the changed-package script, a fake go command in fakebin, and the packages a
// and b, of which only a changed after the returned base commit.
func gateRepository(t *testing.T) (dir, base string) {
	t.Helper()
	for _, tool := range []string{"make", "git", "sh"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is not installed: %v", tool, err)
		}
	}
	root := repositoryRoot(t)
	dir = t.TempDir()
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
	base = run("git", "rev-parse", "HEAD")
	writeFile(t, filepath.Join(dir, "a", "a.go"), "package a\n\nconst Changed = true\n")
	run("git", "commit", "-q", "-am", "change a")
	return dir, base
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
