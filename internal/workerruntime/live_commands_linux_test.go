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

	"github.com/iryzhkov/t3-steward/internal/testutil"
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
	workspace := testutil.RealTempDir(t)
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
	root := testutil.RealTempDir(t)
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
	workspace := testutil.RealTempDir(t)
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
	workspace := testutil.RealTempDir(t)
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
	workspace := testutil.RealTempDir(t)
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

// liveCommandsProviderRole selects which provider the server helper starts.
const liveCommandsProviderRole = "T3_STEWARD_LIVE_COMMANDS_PROVIDER"

// TestLiveCommandsSessionHelper is not a test. Re-executed by
// TestLiveCommandsFindsACommandTheProviderStillTracks it plays the T3 server
// outside the workspace, which starts the provider session inside it. The
// provider starts an MCP server directly and a command through its command
// tool's shell, and tracks both, the way a run_in_background command stays
// attached to the provider. Re-executed by
// TestLiveCommandsFindsToolExecutionsWhoseShellIsGone the provider instead
// runs its tool commands the way Codex does, each in a session of its own.
func TestLiveCommandsSessionHelper(t *testing.T) {
	role := os.Getenv(liveCommandsHelperRole)
	if role == "" {
		t.Skip("helper process")
	}
	workspace, pidFile := os.Getenv("T3_STEWARD_LIVE_COMMANDS_WORKSPACE"), os.Getenv("T3_STEWARD_LIVE_COMMANDS_PIDS")
	switch role {
	case "server":
		providerRole := os.Getenv(liveCommandsProviderRole)
		if providerRole == "" {
			providerRole = "provider"
		}
		provider := exec.Command(os.Args[0], "-test.run=^TestLiveCommandsSessionHelper$")
		provider.Dir = workspace
		provider.Env = append(os.Environ(), liveCommandsHelperRole+"="+providerRole)
		if err := provider.Start(); err != nil {
			os.Exit(2)
		}
		_ = provider.Wait()
	case "codex-provider":
		// Two MCP servers in the provider's session: one reading requests
		// from a pipe, one that put /dev/null on its stdin, as a Python MCP
		// server on a fleet host does.
		piped := exec.Command("sleep", "302")
		piped.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		requests, err := piped.StdinPipe()
		if err != nil {
			os.Exit(2)
		}
		defer requests.Close()
		quiet := exec.Command("sleep", "306")
		// A Codex command: bash -lc '<command>' in a session of its own, where
		// the shell replaces itself with the command.
		inPlace := exec.Command("sh", "-c", "exec sleep 305")
		inPlace.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		// A command under a sandbox wrapper that forks the command rather
		// than becoming it, as bwrap does. The wrapper stays in the
		// provider's session, so its name alone marks it as a tool execution.
		wrapper := exec.Command(filepath.Join(filepath.Dir(pidFile), "bwrap"), "-test.run=^TestLiveCommandsSessionHelper$")
		wrapper.Env = append(os.Environ(), liveCommandsHelperRole+"=wrapper")
		for _, child := range []*exec.Cmd{piped, quiet, inPlace, wrapper} {
			child.Dir = workspace
			if err := child.Start(); err != nil {
				os.Exit(2)
			}
		}
		var wrapped []byte
		for len(strings.TrimSpace(string(wrapped))) == 0 {
			time.Sleep(10 * time.Millisecond)
			wrapped, _ = os.ReadFile(pidFile + ".wrapped")
		}
		pids := fmt.Sprintf("%d %d %d %d %d %d %s\n", os.Getppid(), os.Getpid(), piped.Process.Pid, quiet.Process.Pid,
			inPlace.Process.Pid, wrapper.Process.Pid, strings.TrimSpace(string(wrapped)))
		if err := os.WriteFile(pidFile+".tmp", []byte(pids), 0o600); err != nil || os.Rename(pidFile+".tmp", pidFile) != nil {
			os.Exit(2)
		}
		_ = inPlace.Wait()
	case "wrapper":
		command := exec.Command("sh", "-c", "exec sleep 307")
		command.Dir = workspace
		if err := command.Start(); err != nil {
			os.Exit(2)
		}
		if err := os.WriteFile(pidFile+".wrapped", []byte(strconv.Itoa(command.Process.Pid)), 0o600); err != nil {
			os.Exit(2)
		}
		_ = command.Wait()
	case "provider":
		mcp := exec.Command("sleep", "302")
		commandFile := pidFile + ".command"
		// The group runs in a subshell that carries the tool shell's own
		// command line, as a pipeline stage does.
		tool := exec.Command("sh", "-c", `(sleep 304; true) & sleep 303 & echo $! > "$1"; wait`, "sh", commandFile)
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
	workspace := testutil.RealTempDir(t)
	pidFile := filepath.Join(t.TempDir(), "pids")
	// The launching shell exits at once, so the server is not this test
	// process's descendant, as the T3 server is not the worker's. The server
	// leads its own process group, so the cleanup ends every helper.
	launcher := exec.Command("sh", "-c", `setsid "$1" -test.run='^TestLiveCommandsSessionHelper$' >/dev/null 2>&1 &`, "launcher", os.Args[0])
	launcher.Dir = t.TempDir()
	launcher.Env = append(os.Environ(), liveCommandsHelperRole+"=server",
		"T3_STEWARD_LIVE_COMMANDS_WORKSPACE="+workspace, "T3_STEWARD_LIVE_COMMANDS_PIDS="+pidFile)
	if err := launcher.Run(); err != nil {
		t.Fatal(err)
	}
	pids := waitForPIDs(t, pidFile, 5)
	server, provider, mcp, shell, command := pids[0], pids[1], pids[2], pids[3], pids[4]
	t.Cleanup(func() { _ = syscall.Kill(-server, syscall.SIGKILL) })
	for _, pid := range []int{provider, mcp, shell, command} {
		waitInWorkspace(t, pid, workspace)
	}
	// Both sleeps may still be replacing the shell's program; the scan is
	// repeated until both run, and its last answer is checked.
	var report LiveCommandReport
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		var err error
		if report, err = scanLiveCommandsIn("linux", "/proc", os.Getpid(), workspace); err != nil {
			t.Fatal(err)
		}
		sleeps := 0
		for _, found := range report.Commands {
			if strings.HasPrefix(found.Command, "sleep 30") {
				sleeps++
			}
		}
		if sleeps == 2 || time.Now().After(deadline) {
			break
		}
	}
	if !reports(report, command) {
		t.Fatalf("the provider-tracked command %d was not reported: %+v", command, report)
	}
	for name, pid := range map[string]int{"server": server, "provider": provider, "MCP server": mcp, "command shell": shell} {
		if reports(report, pid) {
			t.Fatalf("the %s %d was reported: %+v", name, pid, report)
		}
	}
	if len(report.Commands) != 2 || report.Commands[0].Command == report.Commands[1].Command ||
		!strings.HasPrefix(report.Commands[0].Command, "sleep 30") || !strings.HasPrefix(report.Commands[1].Command, "sleep 30") {
		t.Fatalf("report = %+v, want the two tracked sleeps and neither shell", report)
	}
	// The driver's turn-end check, which holds the attempt instead of
	// collecting it, sees the same command.
	driverReport, err := (&LocalDriver{}).LiveCommands(context.Background(), testPackage(), workspace)
	if err != nil || !reports(driverReport, command) {
		t.Fatalf("driver report = %+v, %v", driverReport, err)
	}
}

// A Codex command leads a session of its own, and its shell may replace itself
// with the command or run it under a sandbox wrapper, so no command shell is
// left below the provider. The command is still reported, and the MCP servers
// in the provider's session are not, whatever their stdin.
func TestLiveCommandsFindsToolExecutionsWhoseShellIsGone(t *testing.T) {
	workspace := testutil.RealTempDir(t)
	helpers := t.TempDir()
	pidFile := filepath.Join(helpers, "pids")
	if err := os.Symlink(os.Args[0], filepath.Join(helpers, "bwrap")); err != nil {
		t.Fatal(err)
	}
	launcher := exec.Command("sh", "-c", `setsid "$1" -test.run='^TestLiveCommandsSessionHelper$' >/dev/null 2>&1 &`, "launcher", os.Args[0])
	launcher.Dir = t.TempDir()
	launcher.Env = append(os.Environ(), liveCommandsHelperRole+"=server", liveCommandsProviderRole+"=codex-provider",
		"T3_STEWARD_LIVE_COMMANDS_WORKSPACE="+workspace, "T3_STEWARD_LIVE_COMMANDS_PIDS="+pidFile)
	if err := launcher.Run(); err != nil {
		t.Fatal(err)
	}
	pids := waitForPIDs(t, pidFile, 7)
	server, provider, piped, quiet, inPlace, wrapper, wrapped := pids[0], pids[1], pids[2], pids[3], pids[4], pids[5], pids[6]
	t.Cleanup(func() { _ = syscall.Kill(-server, syscall.SIGKILL) })
	for _, pid := range pids[1:] {
		killOnCleanup(t, pid)
		waitInWorkspace(t, pid, workspace)
	}
	// Wait until both commands have replaced their shells, as the provider's
	// command has by the time an agent ends its turn.
	for _, pid := range []int{inPlace, wrapped} {
		for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			if raw, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline")); err == nil && strings.HasPrefix(string(raw), "sleep\x00") {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("command %d never replaced its shell", pid)
			}
		}
	}
	report, err := scanLiveCommandsIn("linux", "/proc", os.Getpid(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	if !reports(report, inPlace) {
		t.Fatalf("the command %d its shell was replaced by was not reported: %+v", inPlace, report)
	}
	if !reports(report, wrapped) {
		t.Fatalf("the command %d under a sandbox wrapper was not reported: %+v", wrapped, report)
	}
	for name, pid := range map[string]int{"server": server, "provider": provider, "piped MCP server": piped, "MCP server without stdin": quiet, "sandbox wrapper": wrapper} {
		if reports(report, pid) {
			t.Fatalf("the %s %d was reported: %+v", name, pid, report)
		}
	}
	if len(report.Commands) != 2 || report.Commands[0].Command == report.Commands[1].Command ||
		!strings.HasPrefix(report.Commands[0].Command, "sleep 30") || !strings.HasPrefix(report.Commands[1].Command, "sleep 30") {
		t.Fatalf("report = %+v, want the two commands and nothing else", report)
	}
	driverReport, err := (&LocalDriver{}).LiveCommands(context.Background(), testPackage(), workspace)
	if err != nil || !reports(driverReport, inPlace) || !reports(driverReport, wrapped) {
		t.Fatalf("driver report = %+v, %v", driverReport, err)
	}
}

// Without /proc the check cannot answer, and says so rather than reporting
// nothing running, which would read as a clean turn end.
func TestLiveCommandsReportsUnsupportedWithoutProc(t *testing.T) {
	workspace := testutil.RealTempDir(t)
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
