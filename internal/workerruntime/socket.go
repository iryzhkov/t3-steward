package workerruntime

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// FrameHandler serves one frame. A handler that refuses a request with a
// complete signed answer returns that reply with an *AnsweredError; any other
// error closes the stream without a reply.
type FrameHandler func(context.Context, []byte) ([]byte, error)

// AnsweredError is a handler error whose reply is a complete answer: the peer
// receives the reply, the stream stays open, and the error is logged. Any
// other handler error closes the stream without writing, because a handler
// may fail after writing part of a reply, and a partial reply must never reach
// the peer as a complete one; the peer sees the stream end and retries.
type AnsweredError struct{ Err error }

func (e *AnsweredError) Error() string { return e.Err.Error() }
func (e *AnsweredError) Unwrap() error { return e.Err }

// listenerLog is where the listener reports a request it could not serve.
// Nothing a peer sends ends its stream without a logged reason.
var listenerLog = slog.Default

// ServeWorkerListener bounds peers and frame size and serializes runtime effects.
// FrameHandler must authenticate every request; socket access grants no authority.
func ServeWorkerListener(ctx context.Context, listener net.Listener, limit int64, timeout time.Duration, maxPeers int, handle FrameHandler) error {
	if listener == nil || limit <= 0 || timeout <= 0 || maxPeers < 1 || handle == nil {
		return errors.New("worker listener requires limits and authenticated handler")
	}
	ctx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()
	peers := make(chan struct{}, maxPeers)
	serial := make(chan struct{}, 1)
	var wg sync.WaitGroup
	stop := context.AfterFunc(ctx, func() { listener.Close() })
	defer func() { cancelAll(); stop(); listener.Close(); wg.Wait() }()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		select {
		case peers <- struct{}{}:
		default:
			conn.Close()
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-peers }()
			defer conn.Close()
			stopPeer := context.AfterFunc(ctx, func() { conn.Close() })
			defer stopPeer()
			codec := workerproto.FrameCodec{MaxBytes: limit}
			for {
				if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
					return
				}
				raw, err := codec.Read(conn)
				if err != nil {
					if !errors.Is(err, io.EOF) && !errors.Is(err, os.ErrDeadlineExceeded) && ctx.Err() == nil {
						listenerLog().Warn("worker stream request refused", "error", err)
					}
					return
				}
				requestCtx, cancel := context.WithTimeout(ctx, timeout)
				select {
				case serial <- struct{}{}:
				case <-requestCtx.Done():
					cancel()
					return
				}
				reply, err := handle(requestCtx, raw)
				<-serial
				cancel()
				if err != nil {
					var answered *AnsweredError
					if !errors.As(err, &answered) || len(reply) == 0 {
						listenerLog().Warn("worker stream request failed", "error", err)
						return
					}
					listenerLog().Warn("worker stream request refused", "error", err)
				}
				if err := codec.Write(conn, reply); err != nil {
					listenerLog().Warn("worker stream reply not sent", "error", err)
					return
				}
			}
		}()
	}
}

// BridgeWorkerStream carries bounded opaque frames to the local worker daemon.
// It neither reads credentials nor interprets an envelope as an admin command.
//
// The bridge owns conn, and input and output when they are io.Closers: on
// cancellation it closes all three, which is what releases a read or write
// blocked on them, and returns ctx.Err() once the closing is done. A stream
// whose Close does not interrupt a blocked call, such as an inherited
// blocking descriptor, must first be made interruptible with
// InterruptibleFile.
func BridgeWorkerStream(ctx context.Context, input io.Reader, output io.Writer, conn net.Conn, limit int64, timeout time.Duration) error {
	if conn == nil || timeout <= 0 {
		return errors.New("worker bridge requires connection and timeout")
	}
	defer conn.Close()
	closed := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(closed)
		conn.Close()
		if closer, ok := input.(io.Closer); ok {
			closer.Close()
		}
		if closer, ok := output.(io.Closer); ok {
			closer.Close()
		}
	})
	err := forwardWorkerFrames(input, output, conn, limit, timeout)
	if !stop() {
		// Cancellation won the race: join the closing before reporting it.
		<-closed
		return ctx.Err()
	}
	return err
}

func forwardWorkerFrames(input io.Reader, output io.Writer, conn net.Conn, limit int64, timeout time.Duration) error {
	codec := workerproto.FrameCodec{MaxBytes: limit}
	for {
		raw, err := codec.Read(input)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err = conn.SetDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
		if err = codec.Write(conn, raw); err != nil {
			return err
		}
		reply, err := codec.Read(conn)
		if err != nil {
			return err
		}
		if err = codec.Write(output, reply); err != nil {
			return err
		}
	}
}
