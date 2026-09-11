package workerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestProtocolReplayStorePersistsResponseSequenceAndPendingRecovery(t *testing.T) {
	root := t.TempDir()
	store := openTestProtocolReplayStore(t, root)
	first := workerproto.Envelope{SessionID: "session-1", RequestID: "request-1", Sequence: 1}

	transaction, err := store.Begin("ssh:coordinator", first, "digest-1", 8)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := transaction.CachedResponse(); ok {
		t.Fatal("new request unexpectedly had cached response")
	}
	response := workerproto.Envelope{RequestID: "response-request-1", InReplyTo: first.RequestID}
	if err := transaction.Complete(response); err != nil {
		t.Fatal(err)
	}

	store = openTestProtocolReplayStore(t, root)
	transaction, err = store.Begin("ssh:coordinator", first, "digest-1", 8)
	if err != nil {
		t.Fatal(err)
	}
	cached, ok := transaction.CachedResponse()
	if !ok || !bytes.Equal(mustReplayJSON(t, cached), mustReplayJSON(t, response)) {
		t.Fatalf("cached response = %+v, ok=%v", cached, ok)
	}
	if err := transaction.Release(); err != nil {
		t.Fatal(err)
	}

	_, err = store.Begin("ssh:coordinator", first, "changed", 8)
	assertReplayCode(t, err, workerproto.ErrorReplay)
	reordered := workerproto.Envelope{SessionID: "session-1", RequestID: "request-3", Sequence: 3}
	_, err = store.Begin("ssh:coordinator", reordered, "digest-3", 8)
	assertReplayCode(t, err, workerproto.ErrorReordered)

	pending := workerproto.Envelope{SessionID: "session-1", RequestID: "request-2", Sequence: 2}
	transaction, err = store.Begin("ssh:coordinator", pending, "digest-2", 8)
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.Release(); err != nil {
		t.Fatal(err)
	}
	blocked := workerproto.Envelope{SessionID: "session-1", RequestID: "request-other", Sequence: 2}
	_, err = store.Begin("ssh:coordinator", blocked, "digest-other", 8)
	assertReplayCode(t, err, workerproto.ErrorBackpressure)

	store = openTestProtocolReplayStore(t, root)
	transaction, err = store.Begin("ssh:coordinator", pending, "digest-2", 8)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := transaction.CachedResponse(); ok {
		t.Fatal("crash-pending request unexpectedly ready")
	}
	if err := transaction.Complete(workerproto.Envelope{RequestID: "response-request-2"}); err != nil {
		t.Fatal(err)
	}
}

func TestProtocolServerUsesReplayStoreAcrossRestart(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	request := signedReplayEnvelope(t, now, 1, "request-1", map[string]string{"value": "one"})
	calls := 0
	handler := func(context.Context, workerproto.Envelope) (workerproto.MessageType, any, error) {
		calls++
		return workerproto.MessageSnapshot, map[string]string{"result": "ok"}, nil
	}
	server := newReplayServer(t, openTestProtocolReplayStore(t, root), now)
	first, err := server.Handle(context.Background(), request, handler)
	if err != nil {
		t.Fatal(err)
	}

	server = newReplayServer(t, openTestProtocolReplayStore(t, root), now)
	replayed, err := server.Handle(context.Background(), request, handler)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || !bytes.Equal(mustReplayJSON(t, first), mustReplayJSON(t, replayed)) {
		t.Fatalf("restart replay calls=%d first=%+v replayed=%+v", calls, first, replayed)
	}

	changed := signedReplayEnvelope(t, now, 1, "request-1", map[string]string{"value": "changed"})
	_, err = server.Handle(context.Background(), changed, handler)
	assertReplayCode(t, err, workerproto.ErrorReplay)
	next := signedReplayEnvelope(t, now, 2, "request-2", map[string]string{"value": "two"})
	if _, err := server.Handle(context.Background(), next, handler); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("handler calls = %d, want 2", calls)
	}
}

func TestProtocolServerRejectsTamperedDurableResponse(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	request := signedReplayEnvelope(t, now, 1, "request-1", map[string]string{"value": "one"})
	store := openTestProtocolReplayStore(t, root)
	server := newReplayServer(t, store, now)
	calls := 0
	handler := func(context.Context, workerproto.Envelope) (workerproto.MessageType, any, error) {
		calls++
		return workerproto.MessageSnapshot, map[string]string{"result": "ok"}, nil
	}
	if _, err := server.Handle(context.Background(), request, handler); err != nil {
		t.Fatal(err)
	}
	state, err := store.read()
	if err != nil {
		t.Fatal(err)
	}
	for key, record := range state.Requests {
		var response workerproto.Envelope
		if err := decodeProtocolResponse(record.Response, &response); err != nil {
			t.Fatal(err)
		}
		response.Payload = json.RawMessage(`{"result":"tampered"}`)
		record.Response, err = json.Marshal(response)
		if err != nil {
			t.Fatal(err)
		}
		state.Requests[key] = record
	}
	if err := store.write(state); err != nil {
		t.Fatal(err)
	}

	server = newReplayServer(t, openTestProtocolReplayStore(t, root), now)
	_, err = server.Handle(context.Background(), request, handler)
	assertReplayCode(t, err, workerproto.ErrorAuthentication)
	if calls != 1 {
		t.Fatalf("handler calls after corrupt replay = %d", calls)
	}
}

func TestProtocolReplayStoreAdoptsNewerCoordinatorEpochAndRejectsCorruption(t *testing.T) {
	root := t.TempDir()
	store := openTestProtocolReplayStore(t, root)
	state, err := store.read()
	if err != nil {
		t.Fatal(err)
	}
	state.Sessions["session-1"] = 3
	if err := store.write(state); err != nil {
		t.Fatal(err)
	}

	adopted, err := OpenProtocolReplayStore(root, "coordinator", "normandy", 10, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	state, err = adopted.read()
	if err != nil {
		t.Fatal(err)
	}
	if state.CoordinatorEpoch != 10 {
		t.Fatalf("coordinator epoch = %d, want 10", state.CoordinatorEpoch)
	}
	if state.Sessions["session-1"] != 3 {
		t.Fatalf("durable session sequence = %d, want 3", state.Sessions["session-1"])
	}
	if _, err := OpenProtocolReplayStore(root, "coordinator", "normandy", 9, "worker-1"); err == nil ||
		!bytes.Contains([]byte(err.Error()), []byte("mismatch")) {
		t.Fatalf("stale epoch error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "protocol-replay.json"), []byte("{broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenProtocolReplayStore(root, "coordinator", "normandy", 10, "worker-1"); err == nil ||
		!bytes.Contains([]byte(err.Error()), []byte("decode")) {
		t.Fatalf("corruption error = %v", err)
	}
}
func openTestProtocolReplayStore(t *testing.T, root string) *ProtocolReplayStore {
	t.Helper()
	store, err := OpenProtocolReplayStore(root, "coordinator", "normandy", 9, "worker-1")
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func newReplayServer(t *testing.T, store *ProtocolReplayStore, now time.Time) *workerproto.Server {
	t.Helper()
	server, err := workerproto.NewServer(workerproto.ServerConfig{
		CoordinatorID: "coordinator", WorkerID: "normandy", CoordinatorEpoch: 9, WorkerEpoch: "worker-1",
		PeerPrincipal: "ssh:coordinator", PeerKeyID: "coordinator-key", PeerSecret: []byte("coordinator-secret"),
		SignerPrincipal: "ssh:normandy", SignerKeyID: "worker-key", SignerSecret: []byte("worker-response-secret"),
		Allowed:     map[workerproto.MessageType]bool{workerproto.MessageSnapshot: true},
		ReplayStore: store, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func signedReplayEnvelope(t *testing.T, now time.Time, sequence int64, requestID string, payload any) workerproto.Envelope {
	t.Helper()
	envelope, err := workerproto.NewEnvelope(
		workerproto.MessageSnapshot, "session-1", requestID, "coordinator", "normandy",
		9, "worker-1", sequence, now, now.Add(time.Minute), payload,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := workerproto.SignEnvelope(&envelope, "ssh:coordinator", "coordinator-key", []byte("coordinator-secret")); err != nil {
		t.Fatal(err)
	}
	return envelope
}

func assertReplayCode(t *testing.T, err error, code workerproto.ErrorCode) {
	t.Helper()
	var protocolErr *workerproto.ProtocolError
	if !errors.As(err, &protocolErr) || protocolErr.Code != code {
		t.Fatalf("protocol error = %v, want %s", err, code)
	}
}

func mustReplayJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
