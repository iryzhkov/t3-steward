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
		"--user", "--scope", "--wait", "--collect", "--pipe",
		"--property=KillMode=control-group",
		"--working-directory=" + root,
		"--", "/bin/sh", "-c", "printf ready",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("systemd-run arguments missing %q:\n%s", want, args)
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
	systemdRun := writeExecutable(t, root, "systemd-run", fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$@\" > %q\nprintf '%%s' \"$$\" > %q\nsleep 30 &\nchild=$!\nprintf '%%s' \"$child\" > %q\nwait \"$child\"\n", runArgs, parentPath, childPath))
	systemctl := writeExecutable(t, root, "systemctl", fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$@\" > %q\nchild=$(cat %q)\nparent=$(cat %q)\nkill -KILL \"$child\" 2>/dev/null || true\nkill -KILL \"$parent\" 2>/dev/null || true\n", killArgs, childPath, parentPath))

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := (SystemdScopeRunner{SystemdRunBinary: systemdRun, SystemctlBinary: systemctl}).Run(
			ctx, ProcessRequest{ID: "attempt-2", Dir: root, Program: "/bin/sh", Args: []string{"-c", "ignored"}},
		)
		result <- err
	}()
	waitForFile(t, childPath)
	childPID := readPID(t, childPath)
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

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", path)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func readPID(t *testing.T, path string) int {
	t.Helper()
	value := strings.TrimSpace(readAbsoluteTestFile(t, path))
	pid, err := strconv.Atoi(value)
	if err != nil {
		t.Fatalf("parse PID %q: %v", value, err)
	}
	return pid
}

func waitForProcessExit(t *testing.T, pid int) {
	t.Helper()
	path := filepath.Join("/proc", strconv.Itoa(pid), "stat")
	deadline := time.Now().Add(time.Second)
	for {
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
		if time.Now().After(deadline) {
			t.Fatalf("process %d remained after scope kill", pid)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
