package backlog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestSystemdScopeRunnerBuildsContainedUserScope(t *testing.T) {
	root := t.TempDir()
	argsPath := filepath.Join(root, "run.args")
	systemdRun := writeExecutable(t, root, "systemd-run", fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$@\" > %q\n", argsPath))
	systemctl := writeExecutable(t, root, "systemctl", "#!/bin/sh\nexit 99\n")
	var log bytes.Buffer

	_, err := (SystemdScopeRunner{SystemdRunBinary: systemdRun, SystemctlBinary: systemctl}).Run(
		context.Background(),
		ProcessRequest{ID: "attempt-1-setup", Dir: root, Program: "/bin/sh", Args: []string{"-c", "printf ready"}, Log: &log},
	)
	if err != nil {
		t.Fatalf("run contained process: %v", err)
	}
	args := readAbsoluteTestFile(t, argsPath)
	for _, want := range []string{
		"--user", "--scope", "--collect",
		"--property=KillMode=control-group",
		"--working-directory=" + root,
		"--", "/bin/sh", "-c", "printf ready",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("systemd-run arguments missing %q:\n%s", want, args)
		}
	}
	for _, incompatible := range []string{"--wait", "--pipe"} {
		if strings.Contains(args, incompatible) {
			t.Fatalf("systemd-run scope arguments contain incompatible flag %q:\n%s", incompatible, args)
		}
	}
	if wantUnit := "--unit=" + processScopeUnit("attempt-1-setup"); !strings.Contains(args, wantUnit) {
		t.Fatalf("systemd-run arguments missing deterministic unit %q:\n%s", wantUnit, args)
	}
	if !strings.Contains(log.String(), systemdRun) {
		t.Fatalf("process log omitted scope command: %s", log.String())
	}
}

func TestSystemdScopeRunnerCancellationKillsWholeScope(t *testing.T) {
	root := t.TempDir()
	runArgs := filepath.Join(root, "run.args")
	killArgs := filepath.Join(root, "kill.args")
	parentPath := filepath.Join(root, "parent.pid")
	childPath := filepath.Join(root, "child.pid")
	// Each PID file is written whole and renamed into place. A redirection
	// creates the file empty before printf fills it, and on a slow runner
	// (macOS CI, run 36988459562) the test read it in between: parse PID "".
	systemdRun := writeExecutable(t, root, "systemd-run", fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$@\" > %q\nprintf '%%s' \"$$\" > %q.tmp && mv %q.tmp %q\nsleep 30 &\nchild=$!\nprintf '%%s' \"$child\" > %q.tmp && mv %q.tmp %q\nwait \"$child\"\n", runArgs, parentPath, parentPath, parentPath, childPath, childPath, childPath))
	systemctl := writeExecutable(t, root, "systemctl", fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$@\" > %q\nchild=$(cat %q)\nparent=$(cat %q)\nkill -KILL \"$child\" 2>/dev/null || true\nkill -KILL \"$parent\" 2>/dev/null || true\n", killArgs, childPath, parentPath))

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := (SystemdScopeRunner{SystemdRunBinary: systemdRun, SystemctlBinary: systemctl}).Run(
			ctx, ProcessRequest{ID: "attempt-2", Dir: root, Program: "/bin/sh", Args: []string{"-c", "ignored"}},
		)
		result <- err
	}()
	childPID := waitForPID(t, childPath)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled process error = %v, want context canceled", err)
	}
	kill := readAbsoluteTestFile(t, killArgs)
	for _, want := range []string{"--user", "kill", "--kill-who=all", "--signal=KILL", processScopeUnit("attempt-2")} {
		if !strings.Contains(kill, want) {
			t.Fatalf("systemctl arguments missing %q:\n%s", want, kill)
		}
	}
	waitForProcessExit(t, childPID)
}

func TestSystemdScopeRunnerReportsExitCodeAndOutput(t *testing.T) {
	root := t.TempDir()
	systemdRun := writeExecutable(t, root, "systemd-run", "#!/bin/sh\nprintf failed\nexit 7\n")
	systemctl := writeExecutable(t, root, "systemctl", "#!/bin/sh\nexit 99\n")

	result, err := (SystemdScopeRunner{SystemdRunBinary: systemdRun, SystemctlBinary: systemctl}).Run(
		context.Background(), ProcessRequest{ID: "attempt-failed", Dir: root, Program: "false"},
	)
	var exitError *ProcessExitError
	if !errors.As(err, &exitError) || exitError.ExitCode != 7 {
		t.Fatalf("process error = %v, want exit code 7", err)
	}
	if result.ExitCode != 7 || result.Output != "failed" {
		t.Fatalf("process result = %#v", result)
	}
}

func TestSystemdScopeRunnerValidatesBeforeStarting(t *testing.T) {
	runner := SystemdScopeRunner{SystemdRunBinary: filepath.Join(t.TempDir(), "missing")}
	for _, request := range []ProcessRequest{
		{Dir: "/tmp", Program: "true"},
		{ID: "id", Dir: "relative", Program: "true"},
		{ID: "id", Dir: "/tmp"},
	} {
		if _, err := runner.Run(context.Background(), request); err == nil {
			t.Fatalf("invalid request %#v succeeded", request)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := runner.Run(ctx, ProcessRequest{ID: "id", Dir: "/tmp", Program: "true"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled error = %v", err)
	}
}

func writeExecutable(t *testing.T, root, name, content string) string {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatalf("write executable %s: %v", name, err)
	}
	return path
}

// waitForPID waits until path holds a whole PID. A file that exists but does
// not parse yet is still being written.
func waitForPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if raw, err := os.ReadFile(path); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil && pid > 0 {
				return pid
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for a PID in %s", path)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitForProcessExit waits until pid has exited. Linux reads /proc so that a
// zombie counts as exited; elsewhere (macOS has no /proc) signal 0 is the
// probe, which used to be skipped there because the missing /proc file read
// as "exited" on the first poll.
func waitForProcessExit(t *testing.T, pid int) {
	t.Helper()
	_, procErr := os.Stat("/proc/self/stat")
	hasProc := procErr == nil
	path := filepath.Join("/proc", strconv.Itoa(pid), "stat")
	deadline := time.Now().Add(10 * time.Second)
	for {
		if hasProc {
			raw, err := os.ReadFile(path)
			if errors.Is(err, os.ErrNotExist) {
				return
			}
			if err == nil {
				fields := strings.Fields(string(raw))
				if len(fields) >= 3 && fields[2] == "Z" {
					return
				}
			}
		} else if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("process %d remained after scope kill", pid)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
