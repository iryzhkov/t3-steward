//go:build linux

package workerruntime

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// killOnCleanup ends a test process however the test leaves.
func killOnCleanup(t *testing.T, pid int) {
	t.Helper()
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
}

// startDetached starts a command the way an agent does when it backgrounds a
// gate with nohup: a shell in dir starts it in the background and exits, so
// the command is adopted by the nearest subreaper, which is an ancestor of
// this test process just as systemd --user is an ancestor of the worker.
func startDetached(t *testing.T, dir, command string) int {
	t.Helper()
	shell := exec.Command("sh", "-c", command+" >/dev/null 2>&1 & echo $!")
	shell.Dir = dir
	out, err := shell.Output()
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		t.Fatal(err)
	}
	killOnCleanup(t, pid)
	// The background child may not have replaced the shell's program yet.
	program := strings.Fields(command)[0]
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if raw, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline")); err == nil && strings.HasPrefix(string(raw), program+"\x00") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never started", command)
		}
	}
	return pid
}

func commandPIDs(report LiveCommandReport) []int {
	pids := make([]int, 0, len(report.Commands))
	for _, command := range report.Commands {
		pids = append(pids, command.PID)
	}
	return pids
}

func reports(report LiveCommandReport, pid int) bool {
	for _, command := range report.Commands {
		if command.PID == pid {
			return true
		}
	}
	return false
}

// A command an agent detached inside its workspace outlives the turn, and the
// worker finds it by its working directory once its shell has exited.
func TestLiveCommandsFindsADetachedCommandInTheWorkspace(t *testing.T) {
	workspace := t.TempDir()
	pid := startDetached(t, workspace, "sleep 61")
	report, err := scanLiveCommandsIn("linux", "/proc", os.Getpid(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	if report.Unsupported != "" {
		t.Fatalf("Linux /proc reported unsupported: %s", report.Unsupported)
	}
	if !reports(report, pid) {
		t.Fatalf("detached command %d not found; found %v", pid, commandPIDs(report))
	}
	for _, command := range report.Commands {
		if command.PID == pid && !strings.Contains(command.Command, "sleep 61") {
			t.Fatalf("command text = %q", command.Command)
		}
	}
}

// The same command outside the workspace, including in a sibling directory
// whose name merely starts with the workspace's, is somebody else's.
func TestLiveCommandsIgnoresCommandsOutsideTheWorkspace(t *testing.T) {
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	sibling := filepath.Join(root, "workspace-other")
	for _, dir := range []string{workspace, sibling} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	pid := startDetached(t, sibling, "sleep 62")
	report, err := scanLiveCommandsIn("linux", "/proc", os.Getpid(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	if reports(report, pid) {
		t.Fatalf("command in a sibling directory reported: %v", report.Commands)
	}
}

// The worker's own children, such as verification commands, run in the
// workspace too and are never the task's background commands.
func TestLiveCommandsExcludesTheWorkersOwnChildren(t *testing.T) {
	workspace := t.TempDir()
	child := exec.Command("sleep", "63")
	child.Dir = workspace
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	// Cleanups run last first: the kill must come before the wait.
	t.Cleanup(func() { _ = child.Wait() })
	killOnCleanup(t, child.Process.Pid)
	report, err := scanLiveCommandsIn("linux", "/proc", os.Getpid(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	if reports(report, child.Process.Pid) {
		t.Fatalf("the worker's own child was reported: %v", report.Commands)
	}
}

// waitForPIDs reads the whitespace-separated pids a helper writes to path
// once every one of its processes is running.
func waitForPIDs(t *testing.T, path string, count int) []int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if raw, err := os.ReadFile(path); err == nil {
			if fields := strings.Fields(string(raw)); len(fields) == count {
				pids := make([]int, 0, count)
				for _, field := range fields {
					pid, err := strconv.Atoi(field)
					if err != nil {
						t.Fatal(err)
					}
					killOnCleanup(t, pid)
					pids = append(pids, pid)
				}
				return pids
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the helper processes never started: %s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitInWorkspace waits until pid runs in workspace, which must hold before
// its presence in a report, or its absence, means anything.
func waitInWorkspace(t *testing.T, pid int, workspace string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if cwd, err := os.Readlink(filepath.Join("/proc", strconv.Itoa(pid), "cwd")); err == nil && cwd == workspace {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("process %d never entered the workspace", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A command that a live shell outside the workspace started inside it, and
// that stays attached to that shell, is a running command too: the shell is
// waiting for it, not hosting a session.
func TestLiveCommandsFindsACommandAttachedToALiveShellOutsideTheWorkspace(t *testing.T) {
	workspace := t.TempDir()
	pidFile := filepath.Join(t.TempDir(), "pid")
	// The launcher exits at once, so the middle shell is no longer related to
	// this test process; it stays outside the workspace and keeps its child
	// in the workspace attached to it.
	launcher := exec.Command("sh", "-c",
		`sh -c '(cd "$1" && exec sleep 64) & echo $! > "$2"; wait' server "$1" "$2" >/dev/null 2>&1 &`,
		"launcher", workspace, pidFile)
	launcher.Dir = "/"
	if err := launcher.Run(); err != nil {
		t.Fatal(err)
	}
	pid := waitForPIDs(t, pidFile, 1)[0]
	waitInWorkspace(t, pid, workspace)
	report, err := scanLiveCommandsIn("linux", "/proc", os.Getpid(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	if !reports(report, pid) {
		t.Fatalf("running task command %d attached to a live shell was not reported: %+v", pid, report)
	}
}

// The worker's own command shell outside the workspace, running a
// verification command inside it, is the worker's work and is not reported.
func TestLiveCommandsExcludesTheWorkersOwnCommandShell(t *testing.T) {
	workspace := t.TempDir()
	pidFile := filepath.Join(t.TempDir(), "pid")
	shell := exec.Command("sh", "-c", `(cd "$1" && exec sleep 65) & echo $! > "$2"; wait`, "verify", workspace, pidFile)
	shell.Dir = "/"
	if err := shell.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shell.Wait() })
	killOnCleanup(t, shell.Process.Pid)
	pid := waitForPIDs(t, pidFile, 1)[0]
	waitInWorkspace(t, pid, workspace)
	report, err := scanLiveCommandsIn("linux", "/proc", os.Getpid(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	if reports(report, pid) || reports(report, shell.Process.Pid) {
		t.Fatalf("the worker's own command was reported: %v", report.Commands)
	}
}

// liveCommandsHelperRole selects what TestLiveCommandsSessionHelper plays.
const liveCommandsHelperRole = "T3_STEWARD_LIVE_COMMANDS_HELPER"

// TestLiveCommandsSessionHelper is not a test. Re-executed by
// TestLiveCommandsFindsACommandTheProviderStillTracks it plays the T3 server
// outside the workspace, which starts the provider session inside it. The
// provider starts an MCP server directly and a command through its command
// tool's shell, and tracks both, the way a run_in_background command stays
// attached to the provider.
func TestLiveCommandsSessionHelper(t *testing.T) {
	role := os.Getenv(liveCommandsHelperRole)
	if role == "" {
		t.Skip("helper process")
	}
	workspace, pidFile := os.Getenv("T3_STEWARD_LIVE_COMMANDS_WORKSPACE"), os.Getenv("T3_STEWARD_LIVE_COMMANDS_PIDS")
	switch role {
	case "server":
		provider := exec.Command(os.Args[0], "-test.run=^TestLiveCommandsSessionHelper$")
		provider.Dir = workspace
		provider.Env = append(os.Environ(), liveCommandsHelperRole+"=provider")
		if err := provider.Start(); err != nil {
			os.Exit(2)
		}
		_ = provider.Wait()
	case "provider":
		mcp := exec.Command("sleep", "302")
		commandFile := pidFile + ".command"
		tool := exec.Command("sh", "-c", `sleep 303 & echo $! > "$1"; wait`, "sh", commandFile)
		for _, child := range []*exec.Cmd{mcp, tool} {
			child.Dir = workspace
			if err := child.Start(); err != nil {
				os.Exit(2)
			}
		}
		var command []byte
		for len(strings.TrimSpace(string(command))) == 0 {
			time.Sleep(10 * time.Millisecond)
			command, _ = os.ReadFile(commandFile)
		}
		pids := fmt.Sprintf("%d %d %d %d %s\n", os.Getppid(), os.Getpid(), mcp.Process.Pid, tool.Process.Pid, strings.TrimSpace(string(command)))
		if err := os.WriteFile(pidFile+".tmp", []byte(pids), 0o600); err != nil || os.Rename(pidFile+".tmp", pidFile) != nil {
			os.Exit(2)
		}
		_ = tool.Wait()
	}
	os.Exit(0)
}

// A command the provider still tracks as its own background task stays
// attached to the provider session, beside the session's MCP servers. It is
// reported, by the command its shell runs, and the provider and its MCP
// server are not.
func TestLiveCommandsFindsACommandTheProviderStillTracks(t *testing.T) {
	workspace := t.TempDir()
	pidFile := filepath.Join(t.TempDir(), "pids")
	// The launching shell exits at once, so the server is not this test
	// process's descendant, as the T3 server is not the worker's.
	launcher := exec.Command("sh", "-c", `"$1" -test.run='^TestLiveCommandsSessionHelper$' >/dev/null 2>&1 &`, "launcher", os.Args[0])
	launcher.Dir = t.TempDir()
	launcher.Env = append(os.Environ(), liveCommandsHelperRole+"=server",
		"T3_STEWARD_LIVE_COMMANDS_WORKSPACE="+workspace, "T3_STEWARD_LIVE_COMMANDS_PIDS="+pidFile)
	if err := launcher.Run(); err != nil {
		t.Fatal(err)
	}
	pids := waitForPIDs(t, pidFile, 5)
	server, provider, mcp, shell, command := pids[0], pids[1], pids[2], pids[3], pids[4]
	for _, pid := range []int{provider, mcp, shell, command} {
		waitInWorkspace(t, pid, workspace)
	}
	report, err := scanLiveCommandsIn("linux", "/proc", os.Getpid(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	if !reports(report, command) {
		t.Fatalf("the provider-tracked command %d was not reported: %+v", command, report)
	}
	for name, pid := range map[string]int{"server": server, "provider": provider, "MCP server": mcp, "command shell": shell} {
		if reports(report, pid) {
			t.Fatalf("the %s %d was reported: %+v", name, pid, report)
		}
	}
	if len(report.Commands) != 1 || !strings.Contains(report.Commands[0].Command, "sleep 303") {
		t.Fatalf("report = %+v, want only the tracked command", report)
	}
	// The driver's turn-end check, which holds the attempt instead of
	// collecting it, sees the same command.
	driverReport, err := (&LocalDriver{}).LiveCommands(context.Background(), testPackage(), workspace)
	if err != nil || !reports(driverReport, command) {
		t.Fatalf("driver report = %+v, %v", driverReport, err)
	}
}

// Without /proc the check cannot answer, and says so rather than reporting
// nothing running, which would read as a clean turn end.
func TestLiveCommandsReportsUnsupportedWithoutProc(t *testing.T) {
	workspace := t.TempDir()
	for _, tc := range []struct{ goos, procRoot string }{
		{"darwin", "/proc"},
		{"linux", filepath.Join(t.TempDir(), "missing-proc")},
	} {
		report, err := scanLiveCommandsIn(tc.goos, tc.procRoot, os.Getpid(), workspace)
		if err != nil {
			t.Fatalf("%s: %v", tc.goos, err)
		}
		if report.Unsupported == "" || len(report.Commands) != 0 {
			t.Fatalf("%s %s: report = %+v, want unsupported", tc.goos, tc.procRoot, report)
		}
	}
}
