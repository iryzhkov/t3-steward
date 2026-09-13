//go:build unix

package workerruntime

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestWorkerSocketOwnershipAndCrashRecovery(t *testing.T) {
	// Keep the socket name below Darwin’s sockaddr_un path limit.
	root, err := os.MkdirTemp("", "t3-sock-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	path := filepath.Join(root, "worker.sock")
	owner, err := LockWorkerSocket(path)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	listener.(*net.UnixListener).SetUnlinkOnClose(false)
	if second, err := LockWorkerSocket(path); err == nil {
		second.Close()
		t.Fatal("second runtime acquired custody")
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatal("second runtime removed live socket", err)
	}
	listener.Close()
	owner.Close()
	replacement, err := LockWorkerSocket(path)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatal("stale socket survived ownership transfer", err)
	}
}
