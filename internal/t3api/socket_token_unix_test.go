//go:build linux || darwin

package t3api

import (
	"context"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestContainedTokenFIFOIsRejectedWithoutBlocking(t *testing.T) {
	root := t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(root, "token"), 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := (SocketTokenFile{Control: root}).Token(context.Background()); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("FIFO accepted")
		}
	case <-time.After(time.Second):
		t.Fatal("token reader blocked on provider FIFO")
	}
}
