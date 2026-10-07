package buildgate

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// affectedModule is the scratch module the test-affected tests select from.
// domain is imported by store, which api imports in turn; testonly imports
// domain only from an external _test.go file and inner only from an internal
// one. leaf, other and tmpl import nothing and nothing imports them; tmpl
// carries a template and a testdata file, and docs holds no package.
var affectedModule = map[string]string{
	"go.mod":                    "module example.com/m\n\ngo 1.21\n",
	"domain/domain.go":          "package domain\n\nfunc Name() string { return \"domain\" }\n",
	"store/store.go":            "package store\n\nimport \"example.com/m/domain\"\n\nfunc Name() string { return domain.Name() }\n",
	"api/api.go":                "package api\n\nimport \"example.com/m/store\"\n\nfunc Name() string { return store.Name() }\n",
	"testonly/testonly.go":      "package testonly\n",
	"testonly/testonly_test.go": "package testonly_test\n\nimport (\n\t\"testing\"\n\n\t\"example.com/m/domain\"\n)\n\nfunc TestName(t *testing.T) { _ = domain.Name() }\n",
	"inner/inner.go":            "package inner\n",
	"inner/inner_test.go":       "package inner\n\nimport (\n\t\"testing\"\n\n\t\"example.com/m/domain\"\n)\n\nfunc TestName(t *testing.T) { _ = domain.Name() }\n",
	"leaf/leaf.go":              "package leaf\n",
	"other/other.go":            "package other\n",
	"tmpl/tmpl.go":              "package tmpl\n",
	"tmpl/templates/a.md.tmpl":  "template\n",
	"tmpl/testdata/golden/a.md": "golden\n",
	"docs/readme.md":            "docs\n",
	"README.md":                 "readme\n",
	".gitignore":                "fakebin/\n*.log\n",
}

var affectedAll = []string{
	"example.com/m/api", "example.com/m/domain", "example.com/m/inner", "example.com/m/leaf",
	"example.com/m/other", "example.com/m/store", "example.com/m/testonly", "example.com/m/tmpl",
}

// affectedRepository commits affectedModule, with the real Makefile and both
// package scripts, to a scratch git repository and returns it and that base
// commit. Every command it and the tests run uses the real go, with the
// module isolated from any workspace, proxy or toolchain switch.
func affectedRepository(t *testing.T) (dir, base string, env []string) {
	t.Helper()
	for _, tool := range []string{"go", "git", "sh"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is not installed: %v", tool, err)
		}
	}
	root := repositoryRoot(t)
	dir = t.TempDir()
	copyFile(t, filepath.Join(root, "Makefile"), filepath.Join(dir, "Makefile"))
	for _, script := range []string{"changed-go-packages.sh", "affected-go-packages.sh"} {
		copyFile(t, filepath.Join(root, "scripts", script), filepath.Join(dir, "scripts", script))
	}
	for path, content := range affectedModule {
		writeFile(t, filepath.Join(dir, filepath.FromSlash(path)), content)
	}
	env = append(gateEnv(filepath.Join(dir, "fakebin")),
		"GOWORK=off", "GOFLAGS=", "GOTOOLCHAIN=local", "GOPROXY=off", "BASE=", "TEST_AFFECTED_LIST=")
	gitIn(t, dir, env, "init", "-q")
	gitIn(t, dir, env, "add", "-A")
	gitIn(t, dir, env, "commit", "-q", "-m", "base")
	base = gitIn(t, dir, env, "rev-parse", "HEAD")
	return dir, base, env
}

func gitIn(t *testing.T, dir string, env []string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// commitChange writes each file and commits the lot on top of the base.
func commitChange(t *testing.T, dir string, env []string, files map[string]string) {
	t.Helper()
	for path, content := range files {
		writeFile(t, filepath.Join(dir, filepath.FromSlash(path)), content)
	}
	gitIn(t, dir, env, "add", "-A")
	gitIn(t, dir, env, "commit", "-q", "-m", "change")
}

// runAffected runs the script and returns its standard output, standard
// error and exit status.
func runAffected(t *testing.T, dir string, env []string, args ...string) (stdout, stderr string, status int) {
	t.Helper()
	cmd := exec.Command("sh", append([]string{"scripts/affected-go-packages.sh"}, args...)...)
	cmd.Dir = dir
	cmd.Env = env
	var out, errOut strings.Builder
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err := cmd.Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exit):
		status = exit.ExitCode()
	default:
		t.Fatal(err)
	}
	return out.String(), errOut.String(), status
}

func wantAffected(t *testing.T, dir string, env []string, base string, want ...string) {
	t.Helper()
	out, errOut, status := runAffected(t, dir, env, base)
	if status != 0 {
		t.Fatalf("affected-go-packages.sh exit %d\n%s", status, errOut)
	}
	if got, wanted := out, strings.Join(want, "\n"); strings.TrimSuffix(got, "\n") != wanted || (wanted == "" && got != "") {
		t.Fatalf("selected:\n%s\nwant:\n%s", got, wanted)
	}
}

func TestAffectedPackagesLeafSelectsOnlyItself(t *testing.T) {
	dir, base, env := affectedRepository(t)
	commitChange(t, dir, env, map[string]string{"leaf/leaf.go": "package leaf\n\nconst Changed = true\n"})
	wantAffected(t, dir, env, base, "example.com/m/leaf")
}

func TestAffectedPackagesSelectsImportersAndTestImporters(t *testing.T) {
	dir, base, env := affectedRepository(t)
	commitChange(t, dir, env, map[string]string{"domain/domain.go": "package domain\n\nfunc Name() string { return \"changed\" }\n"})
	wantAffected(t, dir, env, base,
		"example.com/m/api", "example.com/m/domain", "example.com/m/inner", "example.com/m/store", "example.com/m/testonly")

	// --changed lists the changed packages alone, before importers are added.
	out, errOut, status := runAffected(t, dir, env, "--changed", base)
	if status != 0 || out != "example.com/m/domain\n" {
		t.Fatalf("--changed: exit %d, %q\n%s", status, out, errOut)
	}
}

func TestAffectedPackagesCountsUncommittedAndUntrackedChanges(t *testing.T) {
	dir, base, env := affectedRepository(t)
	writeFile(t, filepath.Join(dir, "leaf", "leaf.go"), "package leaf\n\nconst Uncommitted = true\n")
	writeFile(t, filepath.Join(dir, "other", "new.go"), "package other\n\nconst Untracked = true\n")
	writeFile(t, filepath.Join(dir, "api", "ignored.log"), "ignored\n")
	wantAffected(t, dir, env, base, "example.com/m/leaf", "example.com/m/other")
}

func TestAffectedPackagesMapsOtherFilesToTheirEnclosingPackage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(t *testing.T, dir string, env []string)
		want   []string
	}{
		{"template", func(t *testing.T, dir string, env []string) {
			commitChange(t, dir, env, map[string]string{"tmpl/templates/a.md.tmpl": "changed\n"})
		}, []string{"example.com/m/tmpl"}},
		{"testdata golden", func(t *testing.T, dir string, env []string) {
			commitChange(t, dir, env, map[string]string{"tmpl/testdata/golden/a.md": "changed\n"})
		}, []string{"example.com/m/tmpl"}},
		{"new testdata directory", func(t *testing.T, dir string, env []string) {
			writeFile(t, filepath.Join(dir, "tmpl", "testdata", "new", "b.md"), "untracked\n")
		}, []string{"example.com/m/tmpl"}},
		{"deleted testdata", func(t *testing.T, dir string, env []string) {
			gitIn(t, dir, env, "rm", "-q", "tmpl/testdata/golden/a.md")
			gitIn(t, dir, env, "commit", "-q", "-m", "delete")
		}, []string{"example.com/m/tmpl"}},
		{"docs", func(t *testing.T, dir string, env []string) {
			commitChange(t, dir, env, map[string]string{"docs/readme.md": "changed\n", "README.md": "changed\n", "Makefile": "all:\n"})
		}, nil},
		{"go.mod", func(t *testing.T, dir string, env []string) {
			commitChange(t, dir, env, map[string]string{"go.mod": "module example.com/m\n\ngo 1.21\n\n// changed\n"})
		}, affectedAll},
		{"go.sum", func(t *testing.T, dir string, env []string) {
			writeFile(t, filepath.Join(dir, "go.sum"), "")
		}, affectedAll},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, base, env := affectedRepository(t)
			tc.change(t, dir, env)
			wantAffected(t, dir, env, base, tc.want...)
		})
	}
}

func TestAffectedPackagesNothingChanged(t *testing.T) {
	dir, base, env := affectedRepository(t)
	wantAffected(t, dir, env, base)
	wantAffected(t, dir, env, "HEAD")
}

func TestAffectedPackagesRefusesAMissingOrUnknownBase(t *testing.T) {
	dir, _, env := affectedRepository(t)
	for _, args := range [][]string{{"nosuchref"}, {}, {""}, {"--changed", "nosuchref"}} {
		out, errOut, status := runAffected(t, dir, env, args...)
		if status != 2 || out != "" || errOut == "" {
			t.Fatalf("%q: exit %d, stdout %q, stderr %q; want exit 2 with a reason", args, status, out, errOut)
		}
	}
}

// A module that does not load is a failure, never an empty selection.
func TestAffectedPackagesFailsWhenTheModuleDoesNotLoad(t *testing.T) {
	dir, base, env := affectedRepository(t)
	writeFile(t, filepath.Join(dir, "leaf", "leaf.go"), "package leaf\n\nimport _ \"example.com/m/missing\"\n")
	out, errOut, status := runAffected(t, dir, env, base)
	if status == 0 || status == 2 || out != "" || errOut == "" {
		t.Fatalf("exit %d, stdout %q, stderr %q; want a go failure", status, out, errOut)
	}
}

// goWrapper delegates every go command to the real one except go test, which
// it only records, so that the Makefile target selects packages with the real
// go list without running a test.
const goWrapper = `#!/bin/sh
if [ "$1" = test ]; then
	printf '%s\n' "$*" >> "$GO_LOG"
	exit 0
fi
exec "$REAL_GO" "$@"
`

// runTestAffected runs make test-affected with the arguments in the scratch
// repository and returns its combined output, whether it passed, and the
// go test invocations it made.
func runTestAffected(t *testing.T, dir string, env []string, args ...string) (out string, ok bool, tests []string) {
	t.Helper()
	log := filepath.Join(t.TempDir(), "go.log")
	cmd := exec.Command("make", append([]string{"--no-print-directory", "test-affected"}, args...)...)
	cmd.Dir = dir
	cmd.Env = append(append([]string(nil), env...), "GO_LOG="+log)
	raw, err := cmd.CombinedOutput()
	data, readErr := os.ReadFile(log)
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		t.Fatal(readErr)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if line != "" {
			tests = append(tests, line)
		}
	}
	return string(raw), err == nil, tests
}

func testAffectedRepository(t *testing.T) (dir, base string, env []string) {
	t.Helper()
	if _, err := exec.LookPath("make"); err != nil {
		t.Skipf("make is not installed: %v", err)
	}
	dir, base, env = affectedRepository(t)
	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("go is not installed: %v", err)
	}
	bin := filepath.Join(dir, "fakebin")
	writeFile(t, filepath.Join(bin, "go"), goWrapper)
	if err := os.Chmod(filepath.Join(bin, "go"), 0o755); err != nil {
		t.Fatal(err)
	}
	env = append(env, "REAL_GO="+realGo)
	commitChange(t, dir, env, map[string]string{"domain/domain.go": "package domain\n\nfunc Name() string { return \"changed\" }\n"})
	return dir, base, env
}

const testAffectedSelection = "example.com/m/api example.com/m/domain example.com/m/inner example.com/m/store example.com/m/testonly"

func TestTestAffectedRequiresBase(t *testing.T) {
	dir, base, env := testAffectedRepository(t)
	for name, run := range map[string]func() (string, bool, []string){
		"missing": func() (string, bool, []string) { return runTestAffected(t, dir, env) },
		"empty":   func() (string, bool, []string) { return runTestAffected(t, dir, env, "BASE=") },
		// A BASE left in the environment is not the command line's; the
		// target never picks a base the caller did not name.
		"environment only": func() (string, bool, []string) {
			return runTestAffected(t, dir, append(append([]string(nil), env...), "BASE="+base))
		},
	} {
		t.Run(name, func(t *testing.T) {
			out, ok, tests := run()
			if ok || !strings.Contains(out, "test-affected: BASE is required: make test-affected BASE=<commit>") || tests != nil {
				t.Fatalf("passed=%v, go test %q:\n%s", ok, tests, out)
			}
		})
	}
}

func TestTestAffectedListsWithoutRunning(t *testing.T) {
	dir, base, env := testAffectedRepository(t)
	out, ok, tests := runTestAffected(t, dir, env, "BASE="+base, "TEST_AFFECTED_LIST=1")
	if !ok || tests != nil {
		t.Fatalf("passed=%v, go test %q:\n%s", ok, tests, out)
	}
	want := "test-affected: 1 packages changed against " + base + ", 5 selected with their importers:\n" +
		strings.ReplaceAll(testAffectedSelection, " ", "\n") + "\n"
	if out != want {
		t.Fatalf("output:\n%s\nwant:\n%s", out, want)
	}
}

func TestTestAffectedRunsTheSelectionOnceUnderTheRaceDetector(t *testing.T) {
	dir, base, env := testAffectedRepository(t)
	// TEST_AFFECTED_LIST, like BASE, counts only on the command line.
	out, ok, tests := runTestAffected(t, dir, append(append([]string(nil), env...), "TEST_AFFECTED_LIST=1"), "BASE="+base)
	if !ok {
		t.Fatalf("make test-affected failed:\n%s", out)
	}
	if want := "test -race -count=1 -timeout 25m " + testAffectedSelection; len(tests) != 1 || tests[0] != want {
		t.Fatalf("go test invocations %q, want exactly %q\n%s", tests, want, out)
	}
}

func TestTestAffectedRunsNothingWhenNothingChanged(t *testing.T) {
	dir, _, env := testAffectedRepository(t)
	out, ok, tests := runTestAffected(t, dir, env, "BASE=HEAD")
	if !ok || tests != nil || !strings.Contains(out, "test-affected: no Go package changed against HEAD; nothing to run") {
		t.Fatalf("passed=%v, go test %q:\n%s", ok, tests, out)
	}
}

func TestTestAffectedRefusesAnUnknownBase(t *testing.T) {
	dir, _, env := testAffectedRepository(t)
	out, ok, tests := runTestAffected(t, dir, env, "BASE=nosuchref")
	if ok || tests != nil || !strings.Contains(out, "nosuchref does not name a commit") {
		t.Fatalf("passed=%v, go test %q:\n%s", ok, tests, out)
	}
}
