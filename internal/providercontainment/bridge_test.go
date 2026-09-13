package providercontainment

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBridgeUsesUnixGatewayAndClosesOnCancellation(t *testing.T) {
	// macOS's default temporary path plus the test name exceeds sockaddr_un.
	// Keep a private, uniquely allocated directory with a short absolute path.
	root, err := os.MkdirTemp("/tmp", "t3-bridge-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	socket := filepath.Join(root, "gateway.sock")
	gateway, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bridgeDone := make(chan error, 1)
	go func() { bridgeDone <- Bridge(ctx, listener, socket) }()
	gatewayDone := make(chan error, 1)
	go func() {
		c, err := gateway.Accept()
		if err != nil {
			gatewayDone <- err
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		data := make([]byte, 4)
		if _, err := io.ReadFull(c, data); err != nil {
			gatewayDone <- err
			return
		}
		if _, err := c.Write(data); err != nil {
			gatewayDone <- err
			return
		}
		_, err = c.Read(data)
		gatewayDone <- err
	}()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := client.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 4)
	if _, err := io.ReadFull(client, data); err != nil || string(data) != "ping" {
		t.Fatalf("bridge exchange: %q %v", data, err)
	}
	cancel()
	select {
	case err := <-bridgeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bridge accept did not stop")
	}
	select {
	case err := <-gatewayDone:
		if err != io.EOF {
			t.Fatalf("gateway not closed cleanly: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("active gateway connection did not close")
	}
}
