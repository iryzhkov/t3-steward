package backlog

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
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
		os.Args[0], "-test.run=^"+t.Name()+"$", "-test.count=1")
	command.Env = append(os.Environ(), helper+"="+t.Name())
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("test under worker umask: %v\n%s", err, output)
	}
	return true
}

func TestVerificationUsesConventionalUmask(t *testing.T) {
	if runModeTestUnderWorkerUmask(t) {
		return
	}

	workspace := t.TempDir()
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
	if report.ExitCode != 0 || strings.TrimSpace(report.Output) != "0022" {
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
	if report.Command != command || report.ExitCode != 7 || report.Output != want {
		t.Errorf("shell command report = %+v, want original command, exit 7, output %q", report, want)
	}
}
