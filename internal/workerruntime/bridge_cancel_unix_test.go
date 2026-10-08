//go:build unix

package workerruntime

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// startBridge runs BridgeWorkerStream in the background and returns its
// result channel.
func startBridge(ctx context.Context, input io.Reader, output io.Writer, conn net.Conn, limit int64) <-chan error {
	done := make(chan error, 1)
	go func() { done <- BridgeWorkerStream(ctx, input, output, conn, limit, time.Minute) }()
	return done
}

func awaitBridge(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("bridge did not return after cancellation")
		return nil
	}
}

// An idle bridge blocked reading its input returns on cancellation and
// closes the streams it owns.
func TestBridgeReturnsOnCancelWhileBlockedOnInput(t *testing.T) {
	input, inputWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer inputWriter.Close()
	outputReader, output, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer outputReader.Close()
	conn, peer := net.Pipe()
	defer peer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startBridge(ctx, input, output, conn, 1024)
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := awaitBridge(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled bridge error = %v, want context canceled", err)
	}
	if err := outputReader.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if n, err := outputReader.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("bridge output still open after cancellation: (%d, %v)", n, err)
	}
}

// A bridge blocked writing a reply nobody reads returns on cancellation.
func TestBridgeReturnsOnCancelWhileBlockedOnOutput(t *testing.T) {
	input, inputWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer inputWriter.Close()
	outputReader, output, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer outputReader.Close()
	conn, peer := net.Pipe()
	defer peer.Close()
	const limit = 4 << 20
	frames := workerproto.FrameCodec{MaxBytes: limit}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startBridge(ctx, input, output, conn, limit)
	if err := frames.Write(inputWriter, []byte("request")); err != nil {
		t.Fatal(err)
	}
	if _, err := frames.Read(peer); err != nil {
		t.Fatal(err)
	}
	// Far larger than any pipe buffer, so the bridge blocks writing it.
	if err := frames.Write(peer, bytes.Repeat([]byte("r"), 2<<20)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := awaitBridge(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled bridge error = %v, want context canceled", err)
	}
}

// The command passes its standard streams through InterruptibleFile, which
// makes even an inherited blocking descriptor interruptible by cancellation.
func TestBridgeInterruptsABlockingInheritedInput(t *testing.T) {
	var fds [2]int
	if err := syscall.Pipe(fds[:]); err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fds[1])
	input, err := InterruptibleFile(uintptr(fds[0]), "stdin")
	if err != nil {
		t.Fatal(err)
	}
	outputReader, output, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer outputReader.Close()
	conn, peer := net.Pipe()
	defer peer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startBridge(ctx, input, output, conn, 1024)
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := awaitBridge(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled bridge error = %v, want context canceled", err)
	}
}

// Normal end of input still ends the bridge cleanly after carrying frames.
func TestBridgeCarriesFramesUntilEndOfInput(t *testing.T) {
	input, inputWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	outputReader, output, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer outputReader.Close()
	conn, peer := net.Pipe()
	defer peer.Close()
	frames := workerproto.FrameCodec{MaxBytes: 1024}
	done := startBridge(context.Background(), input, output, conn, 1024)
	go func() {
		raw, err := frames.Read(peer)
		if err == nil {
			_ = frames.Write(peer, append([]byte("echo "), raw...))
		}
	}()
	if err := frames.Write(inputWriter, []byte("frame")); err != nil {
		t.Fatal(err)
	}
	reply, err := frames.Read(outputReader)
	if err != nil || string(reply) != "echo frame" {
		t.Fatalf("bridge reply = (%q, %v)", reply, err)
	}
	inputWriter.Close()
	if err := awaitBridge(t, done); err != nil {
		t.Fatalf("bridge at end of input = %v, want nil", err)
	}
}
