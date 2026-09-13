package workerproto

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// FrameCodec is a bounded transport frame around the existing signed envelope.
// Its length prefix permits multiple exchanges without waiting for EOF.
type FrameCodec struct{ MaxBytes int64 }

func (c FrameCodec) Read(r io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	n := int64(binary.BigEndian.Uint32(header[:]))
	if c.MaxBytes <= 0 || n == 0 || n > c.MaxBytes {
		return nil, &ProtocolError{Code: ErrorLimit, Message: "stream frame exceeds limit"}
	}
	raw := make([]byte, int(n))
	_, err := io.ReadFull(r, raw)
	return raw, err
}
func (c FrameCodec) Write(w io.Writer, raw []byte) error {
	if len(raw) == 0 || int64(len(raw)) > c.MaxBytes || uint64(len(raw)) > uint64(^uint32(0)) {
		return &ProtocolError{Code: ErrorLimit, Message: "stream frame exceeds limit"}
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(raw)))
	if err := writeAll(w, header[:]); err != nil {
		return err
	}
	return writeAll(w, raw)
}
func writeAll(w io.Writer, raw []byte) error {
	for len(raw) > 0 {
		n, err := w.Write(raw)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		raw = raw[n:]
	}
	return nil
}

type StreamDialer func(context.Context) (net.Conn, error)

// StreamTransport serializes exchanges on one persistent authenticated channel.
// On uncertainty it closes the connection; retries keep the original envelope.
type StreamTransport struct {
	base   *SSHTransport
	dial   StreamDialer
	gate   chan struct{}
	mu     sync.Mutex
	conn   net.Conn
	closed bool
}

func NewStreamTransport(config SSHConfig, dial StreamDialer) (*StreamTransport, error) {
	base, err := NewSSHTransport(config)
	if err != nil {
		return nil, err
	}
	if dial == nil {
		return nil, errors.New("stream transport requires dialer")
	}
	return &StreamTransport{base: base, dial: dial, gate: make(chan struct{}, 1)}, nil
}
func (t *StreamTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	if t.conn != nil {
		err := t.conn.Close()
		t.conn = nil
		return err
	}
	return nil
}

const StreamArtifactLimit int64 = 16 << 20

func (t *StreamTransport) RoundTrip(ctx context.Context, request Envelope) (Envelope, error) {
	response, _, err := t.roundTrip(ctx, request, nil, 0)
	return response, err
}
func (t *StreamTransport) roundTrip(ctx context.Context, request Envelope, suffix []byte, maxResponseSuffix int64) (Envelope, []byte, error) {
	deadline := time.Now().Add(t.base.config.RequestTimeout)
	if !request.Deadline.IsZero() && request.Deadline.Before(deadline) {
		deadline = request.Deadline
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	select {
	case t.gate <- struct{}{}:
		defer func() { <-t.gate }()
	case <-ctx.Done():
		return Envelope{}, nil, ctx.Err()
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return Envelope{}, nil, errors.New("stream transport closed")
	}
	conn := t.conn
	t.mu.Unlock()
	if conn == nil {
		var err error
		conn, err = t.dial(ctx)
		if err != nil {
			return Envelope{}, nil, err
		}
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			conn.Close()
			return Envelope{}, nil, errors.New("stream transport closed")
		}
		t.conn = conn
		t.mu.Unlock()
	}
	failed := true
	defer func() {
		if failed {
			conn.Close()
			t.mu.Lock()
			if t.conn == conn {
				t.conn = nil
			}
			t.mu.Unlock()
		}
	}()
	if err := conn.SetDeadline(deadline); err != nil {
		return Envelope{}, nil, err
	}
	// Cancellation closes the channel and cannot race the next exchange.
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { conn.Close(); close(done) })
	defer func() {
		if !stop() {
			<-done
		}
	}()
	var payload bytes.Buffer
	if err := t.base.codec.Encode(&payload, request); err != nil {
		return Envelope{}, nil, err
	}
	payload.Write(suffix)
	codec := FrameCodec{MaxBytes: t.base.config.MaxMessageBytes + StreamArtifactLimit}
	if err := codec.Write(conn, payload.Bytes()); err != nil {
		return Envelope{}, nil, fmt.Errorf("stream write: %w", err)
	}
	raw, err := codec.Read(conn)
	if err != nil {
		return Envelope{}, nil, fmt.Errorf("stream read: %w", err)
	}
	var response Envelope
	header, tail, found := bytes.Cut(raw, []byte{'\n'})
	if !found || int64(len(tail)) > maxResponseSuffix {
		return Envelope{}, nil, errors.New("invalid stream response suffix")
	}
	if err = t.base.codec.Decode(bytes.NewReader(header), &response); err != nil {
		return Envelope{}, nil, err
	}
	if err = t.base.validateResponse(request, response); err != nil {
		return Envelope{}, nil, err
	}
	if err = ctx.Err(); err != nil {
		return Envelope{}, nil, err
	}
	failed = false
	return response, tail, nil
}
func (t *StreamTransport) RoundTripWithRetry(ctx context.Context, request Envelope, policy RetryPolicy) (Envelope, error) {
	if policy.MaxAttempts < 1 {
		return Envelope{}, errors.New("stream retry attempts must be positive")
	}
	var last error
	for i := 1; i <= policy.MaxAttempts; i++ {
		response, err := t.RoundTrip(ctx, request)
		if err == nil {
			return response, nil
		}
		last = err
		var p *ProtocolError
		if errors.As(err, &p) && !p.Retryable {
			return Envelope{}, err
		}
		if i == policy.MaxAttempts {
			break
		}
		timer := time.NewTimer(policy.Delay(i))
		select {
		case <-ctx.Done():
			timer.Stop()
			return Envelope{}, ctx.Err()
		case <-timer.C:
		}
	}
	return Envelope{}, last
}
