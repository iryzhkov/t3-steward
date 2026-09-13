package workerproto

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestFrameRejectsOversizedPrefixWithoutBody(t *testing.T) {
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], 1<<30)
	if _, err := (FrameCodec{MaxBytes: 1024}).Read(bytes.NewReader(prefix[:])); err == nil {
		t.Fatal("oversized frame accepted")
	}
}
func TestStreamReusesConnectionAndRetriesExactIdentity(t *testing.T) {
	var dials atomic.Int32
	var observed atomic.Int32
	request := signedLiveEnvelope(t)
	dial := func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		generation := dials.Add(1)
		go func() {
			defer server.Close()
			frames := FrameCodec{MaxBytes: 64 << 10}
			codec := Codec{MaxBytes: 64 << 10}
			for {
				raw, err := frames.Read(server)
				if err != nil {
					return
				}
				var got Envelope
				if err = codec.Decode(bytes.NewReader(raw), &got); err != nil {
					return
				}
				if got.RequestID != request.RequestID || got.Authentication.Signature != request.Authentication.Signature {
					return
				}
				observed.Add(1)
				if generation == 1 {
					return
				} // Lost response after receiving the intent.
				reply := got
				reply.InReplyTo = got.RequestID
				reply.RequestID = "reply"
				reply.Sender, reply.Recipient = got.Recipient, got.Sender
				if err = SignEnvelope(&reply, "worker:normandy", "worker-key", testSecret); err != nil {
					return
				}
				var out bytes.Buffer
				if err = codec.Encode(&out, reply); err != nil {
					return
				}
				if err = frames.Write(server, out.Bytes()); err != nil {
					return
				}
			}
		}()
		return client, nil
	}
	transport, err := NewStreamTransport(SSHConfig{Address: "worker", RemoteCommand: "bridge", RequestTimeout: time.Second, ConnectTimeout: time.Second, MaxMessageBytes: 64 << 10, MaxStderrBytes: 1024, ResponsePrincipal: "worker:normandy", ResponseKeyID: "worker-key", ResponseSecret: testSecret}, dial)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	policy := RetryPolicy{MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond}
	for i := 0; i < 3; i++ {
		if _, err = transport.RoundTripWithRetry(context.Background(), request, policy); err != nil {
			t.Fatal(err)
		}
	}
	if dials.Load() != 2 || observed.Load() != 4 {
		t.Fatal("connection or replay identity changed", dials.Load(), observed.Load())
	}
}
func TestStreamCancellationClosesUnresponsivePeer(t *testing.T) {
	var peer net.Conn
	transport, err := NewStreamTransport(SSHConfig{Address: "worker", RemoteCommand: "bridge", RequestTimeout: time.Second, ConnectTimeout: time.Second, MaxMessageBytes: 64 << 10, MaxStderrBytes: 1024, ResponsePrincipal: "worker:normandy", ResponseKeyID: "worker-key", ResponseSecret: testSecret}, func(context.Context) (net.Conn, error) {
		c, s := net.Pipe()
		peer = s
		go func() { _, _ = io.Copy(io.Discard, s) }()
		return c, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err = transport.RoundTrip(ctx, signedLiveEnvelope(t)); err == nil {
		t.Fatal("unresponsive peer succeeded")
	}
	peer.Close()
	if _, err = transport.RoundTrip(ctx, signedLiveEnvelope(t)); err == nil {
		t.Fatal(fmt.Errorf("cancelled caller accepted"))
	}
}
