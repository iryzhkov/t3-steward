//go:build linux

package workerruntime

import (
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

// A process in the workspace whose parent is a live process outside it, as
// the provider session and its MCP servers are children of the T3 server, is
// session infrastructure and is not reported.
func TestLiveCommandsExcludesProcessesAttachedToAnotherLiveProcess(t *testing.T) {
	workspace := t.TempDir()
	pidFile := filepath.Join(t.TempDir(), "pid")
	// The middle shell stands in for the T3 server: it stays outside the
	// workspace, is not related to this test process once its parent exits,
	// and keeps its child in the workspace attached to it.
	launcher := exec.Command("sh", "-c",
		`sh -c '(cd "$1" && exec sleep 64) & echo $! > "$2"; wait' server "$1" "$2" >/dev/null 2>&1 &`,
		"launcher", workspace, pidFile)
	launcher.Dir = "/"
	if err := launcher.Run(); err != nil {
		t.Fatal(err)
	}
	var pid int
	deadline := time.Now().Add(5 * time.Second)
	for pid == 0 {
		if raw, err := os.ReadFile(pidFile); err == nil && strings.TrimSpace(string(raw)) != "" {
			if pid, err = strconv.Atoi(strings.TrimSpace(string(raw))); err != nil {
				t.Fatal(err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the attached command never started")
		}
		time.Sleep(10 * time.Millisecond)
	}
	killOnCleanup(t, pid)
	// The command must really be running in the workspace before its absence
	// from the report means anything.
	for {
		if cwd, err := os.Readlink(filepath.Join("/proc", strconv.Itoa(pid), "cwd")); err == nil && cwd == workspace {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the attached command never entered the workspace")
		}
		time.Sleep(10 * time.Millisecond)
	}
	report, err := scanLiveCommandsIn("linux", "/proc", os.Getpid(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	if reports(report, pid) {
		t.Fatalf("a process attached to a live process outside the workspace was reported: %v", report.Commands)
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
