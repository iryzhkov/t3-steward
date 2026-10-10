//go:build unix

package workerruntime

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
)

// The shell starts a detached process group with a live grandchild. Neither
// exec may retain the ownership descriptor after its runtime owner closes it.
func TestDrainSocketLockDetachedExecGrandchild(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker.sock")
	owner, err := LockWorkerSocket(path)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	before, err := os.Stat(path + ".lock")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("sh", "-c", "sleep 30 & echo $!; wait")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
	}()
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		t.Fatal("detached grandchild did not report")
	}
	grandchild, err := strconv.Atoi(scanner.Text())
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(grandchild, 0); err != nil {
		t.Fatalf("grandchild not alive: %v", err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	replacement, err := LockWorkerSocket(path)
	if err != nil {
		t.Fatalf("detached exec retained runtime lock: %v", err)
	}
	defer replacement.Close()
	after, err := os.Stat(path + ".lock")
	if err != nil || !os.SameFile(before, after) {
		t.Fatalf("ownership lock replaced instead of reacquired: %v", err)
	}
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("reacquisition depended on child exit: %v", err)
	}
	if err := syscall.Kill(grandchild, 0); err != nil {
		t.Fatalf("reacquisition depended on grandchild exit: %v", err)
	}
}
