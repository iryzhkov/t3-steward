package backlog

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Re-exec just this test under the worker's umask rather than changing the
// shared test process's umask while other goroutines may be creating files.
func runModeTestUnderWorkerUmask(t *testing.T) bool {
	t.Helper()
	const helper = "T3_TEST_WORKER_UMASK"
	if os.Getenv(helper) == t.Name() {
		return false
	}
	command := exec.Command("/bin/sh", "-c", `umask 077 && exec "$0" "$@"`,
		os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$", "-test.count=1", "-test.v")
	command.Env = append(os.Environ(), helper+"="+t.Name())
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("test under worker umask: %v\n%s", err, output)
	}
	// A -test.run pattern that matches nothing also exits 0, so require the
	// child to report this exact test as passed.
	if !strings.Contains(string(output), "--- PASS: "+t.Name()+" (") {
		t.Fatalf("test under worker umask did not run %s:\n%s", t.Name(), output)
	}
	return true
}

func TestVerificationUsesConventionalUmask(t *testing.T) {
	if runModeTestUnderWorkerUmask(t) {
		return
	}

	// The verification shell reports $PWD with symlinks resolved, and on macOS
	// t.TempDir() lives under /var, a symlink to /private/var.
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	assertPrivateCreation := func(name string) {
		t.Helper()
		path := filepath.Join(workspace, name)
		if err := os.WriteFile(path, nil, 0o666); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("worker-created %s mode = %#o, want 0600", name, got)
		}
	}
	assertPrivateCreation("before")
	finalizer := AttemptFinalizer{Processes: &directRunner{}}
	report, err := finalizer.runVerification(context.Background(), "verify-umask", workspace, "umask")
	if err != nil {
		t.Fatal(err)
	}
	if report.ExitCode != 0 || !strings.HasSuffix(report.Output, "\n0022\n") {
		t.Errorf("verification umask: exit = %d, output = %q; want 0, 0022", report.ExitCode, report.Output)
	}
	if report.Command != "umask" {
		t.Errorf("reported command = %q, want original command", report.Command)
	}
	assertPrivateCreation("after")

	// Keep the user's command as a literal argument to its own shell. Quotes,
	// expansions, working directory, output and nonzero exit status survive.
	command := `value='literal $HOME; "quoted"'; printf '%s\n' "$value"; printf '%s\n' "$PWD"; exit 7`
	report, err = finalizer.runVerification(context.Background(), "verify-shell", workspace, command)
	if err != nil {
		t.Fatal(err)
	}
	want := "literal $HOME; \"quoted\"\n" + workspace + "\n"
	if report.Command != command || report.ExitCode != 7 || !strings.HasSuffix(report.Output, want) {
		t.Errorf("shell command report = %+v, want original command, exit 7, output %q", report, want)
	}
}
