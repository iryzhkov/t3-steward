package workerruntime

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerSocketPersistsAcrossReconnectAndStopsIdlePeers(t *testing.T) {
	// Keep the socket name below Darwin’s sockaddr_un path limit.
	root, err := os.MkdirTemp("", "t3-sock-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	path := filepath.Join(root, "worker.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var effects atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- ServeWorkerListener(ctx, listener, 1024, time.Second, 2, func(_ context.Context, raw []byte) ([]byte, error) { effects.Add(1); return raw, nil })
	}()
	frames := workerproto.FrameCodec{MaxBytes: 1024}
	for i := 0; i < 2; i++ {
		c, err := net.Dial("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		c.SetDeadline(time.Now().Add(time.Second))
		for j := 0; j < 2; j++ {
			if err = frames.Write(c, []byte("signed intent")); err != nil {
				t.Fatal(err)
			}
			got, err := frames.Read(c)
			if err != nil || string(got) != "signed intent" {
				t.Fatal(string(got), err)
			}
		}
		c.Close()
	}
	if effects.Load() != 4 {
		t.Fatal("lost worker state on reconnect")
	}
	idle, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("idle peer prevented shutdown")
	}
}
