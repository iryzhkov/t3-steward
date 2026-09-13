package workerruntime

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type FrameHandler func(context.Context, []byte) ([]byte, error)

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
				if err == nil {
					err = codec.Write(conn, reply)
				}
				cancel()
				if err != nil {
					return
				}
			}
		}()
	}
}

// BridgeWorkerStream carries bounded opaque frames to the local worker daemon.
// It neither reads credentials nor interprets an envelope as an admin command.
func BridgeWorkerStream(ctx context.Context, input io.Reader, output io.Writer, conn net.Conn, limit int64, timeout time.Duration) error {
	if conn == nil || timeout <= 0 {
		return errors.New("worker bridge requires connection and timeout")
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
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
