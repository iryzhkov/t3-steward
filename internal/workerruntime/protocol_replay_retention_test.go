package workerruntime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestProtocolReplayMigratesLegacyOnceAndRollsBackCorruption(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(corrupt), func(t *testing.T) {
			root := t.TempDir()
			response := workerproto.Envelope{RequestID: "response-first", Payload: json.RawMessage(`{"ok":true}`)}
			legacy := protocolReplayState{
				Version: 1, CoordinatorID: "coordinator", WorkerID: "normandy", CoordinatorEpoch: 9, WorkerEpoch: "worker-1",
				Sessions: map[string]int64{"s": 2},
				Requests: map[string]protocolReplayRecord{
					"ssh:coordinator/first":   {Digest: "one", Ready: true, Response: mustReplayJSON(t, response)},
					"ssh:coordinator/pending": {Digest: "two", StartedAt: time.Now()},
				}, RequestOrder: []string{"ssh:coordinator/first", "ssh:coordinator/pending"},
			}
			raw := mustReplayJSON(t, legacy)
			if corrupt {
				raw = append(raw, []byte(" trailing corruption")...)
			}
			path := filepath.Join(root, "protocol-replay.json")
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			store, err := OpenProtocolReplayStore(root, "coordinator", "normandy", 9, "worker-1")
			if corrupt {
				if err == nil {
					t.Fatal("corrupt migration accepted")
				}
				if err := os.WriteFile(path, mustReplayJSON(t, legacy), 0o600); err != nil {
					t.Fatal(err)
				}
				store = openTestProtocolReplayStore(t, root)
			} else if err != nil {
				t.Fatal(err)
			}
			first := workerproto.Envelope{SessionID: "s", RequestID: "first", Sequence: 1}
			tr, err := store.Begin("ssh:coordinator", first, "one", 8)
			if err != nil {
				t.Fatal(err)
			}
			cached, ok := tr.CachedResponse()
			if !ok || !bytes.Equal(mustReplayJSON(t, cached), mustReplayJSON(t, response)) {
				t.Fatal("migration changed response")
			}
			tr.Release()
			tr, err = store.Begin("ssh:coordinator", workerproto.Envelope{SessionID: "s", RequestID: "pending", Sequence: 2}, "two", 8)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := tr.CachedResponse(); ok {
				t.Fatal("pending became complete")
			}
			if err := tr.Complete(workerproto.Envelope{RequestID: "response-pending"}); err != nil {
				t.Fatal(err)
			}
			// The old JSON no longer controls state after the migration commit.
			if err := os.WriteFile(path, []byte("old file unavailable"), 0o600); err != nil {
				t.Fatal(err)
			}
			store = openTestProtocolReplayStore(t, root)
			tr, err = store.Begin("ssh:coordinator", first, "one", 8)
			if err != nil {
				t.Fatal(err)
			}
			tr.Release()
		})
	}
}

func TestProtocolReplayByteAndAgeRetentionPreservesSequenceFence(t *testing.T) {
	store := openTestProtocolReplayStore(t, t.TempDir())
	now := time.Now()
	store.now = func() time.Time { return now }
	store.maxBytes = 2048
	body := json.RawMessage(fmt.Sprintf("%q", string(bytes.Repeat([]byte("x"), 700))))
	requests := make([]workerproto.Envelope, 0, 12)
	for i := 1; i <= 12; i++ {
		request := workerproto.Envelope{SessionID: "s", RequestID: fmt.Sprint(i), Sequence: int64(i)}
		requests = append(requests, request)
		tr, err := store.Begin("ssh:c", request, fmt.Sprint(i), 4096)
		if err != nil {
			t.Fatal(err)
		}
		if err := tr.Complete(workerproto.Envelope{RequestID: "response-" + request.RequestID, Payload: body}); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Second)
	}
	db, err := store.openDB()
	if err != nil {
		t.Fatal(err)
	}
	var total, count int64
	if err := db.QueryRow("SELECT coalesce(sum(length(body)),0),count(*) FROM responses").Scan(&total, &count); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if total > store.maxBytes || count >= 12 {
		t.Fatalf("bytes=%d count=%d", total, count)
	}
	_, err = store.Begin("ssh:c", requests[0], "1", 4096)
	assertReplayCode(t, err, workerproto.ErrorReordered)
	// Keep session fencing newer than the first retained response.
	now = now.Add(2 * time.Hour)
	store.maxAge = time.Hour
	tr, err := store.Begin("ssh:c", workerproto.Envelope{SessionID: "other", RequestID: "other", Sequence: 1}, "other", 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Complete(workerproto.Envelope{RequestID: "response-other"}); err != nil {
		t.Fatal(err)
	}
	db, err = store.openDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.QueryRow("SELECT count(*) FROM responses").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("aged responses remain: %d", count)
	}
	var sessions int
	if err := db.QueryRow("SELECT count(*) FROM sessions").Scan(&sessions); err != nil {
		t.Fatal(err)
	}
	if sessions != 1 {
		t.Fatalf("aged session metadata remains: %d", sessions)
	}
}

func TestProtocolReplayOversizedResponseRetainsPendingIdentity(t *testing.T) {
	store := openTestProtocolReplayStore(t, t.TempDir())
	store.maxBytes = 128
	req := workerproto.Envelope{SessionID: "s", RequestID: "r", Sequence: 1}
	tr, err := store.Begin("ssh:c", req, "digest", 8)
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Complete(workerproto.Envelope{Payload: json.RawMessage(fmt.Sprintf("%q", string(bytes.Repeat([]byte("x"), 512))))}); err == nil {
		t.Fatal("oversized response accepted")
	}
	tr.Release()
	store = openTestProtocolReplayStore(t, store.root)
	tr, err = store.Begin("ssh:c", req, "digest", 8)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := tr.CachedResponse(); ok {
		t.Fatal("oversized response became completed")
	}
	tr.Release()
}

func TestProtocolReplayAbandonedRequestAndCountEviction(t *testing.T) {
	store := openTestProtocolReplayStore(t, t.TempDir())
	now := time.Now()
	store.now = func() time.Time { return now }
	first := workerproto.Envelope{SessionID: "s", RequestID: "first", Sequence: 1}
	tr, err := store.Begin("ssh:c", first, "one", 1)
	if err != nil {
		t.Fatal(err)
	}
	tr.Release() // Simulate death after durable Begin.
	now = now.Add(abandonedRequestAge + time.Second)
	second := workerproto.Envelope{SessionID: "s", RequestID: "second", Sequence: 2}
	tr, err = store.Begin("ssh:c", second, "two", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Complete(workerproto.Envelope{RequestID: "response-second"}); err != nil {
		t.Fatal(err)
	}
	_, err = store.Begin("ssh:c", first, "one", 1)
	assertReplayCode(t, err, workerproto.ErrorReordered)
	third := workerproto.Envelope{SessionID: "s", RequestID: "third", Sequence: 3}
	tr, err = store.Begin("ssh:c", third, "three", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Complete(workerproto.Envelope{RequestID: "response-third"}); err != nil {
		t.Fatal(err)
	}
	_, err = store.Begin("ssh:c", second, "two", 1)
	assertReplayCode(t, err, workerproto.ErrorReordered)
}

func TestProtocolReplayProcessDeathAfterBegin(t *testing.T) {
	if root := os.Getenv("S0_REPLAY_CRASH_ROOT"); root != "" {
		store := openTestProtocolReplayStore(t, root)
		if _, err := store.Begin("ssh:c", workerproto.Envelope{SessionID: "s", RequestID: "r", Sequence: 1}, "digest", 8); err != nil {
			t.Fatal(err)
		}
		os.Exit(0) // Deliberately skip Release and all defers.
	}
	root := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestProtocolReplayProcessDeathAfterBegin$")
	cmd.Env = append(os.Environ(), "S0_REPLAY_CRASH_ROOT="+root)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child: %v %s", err, out)
	}
	store := openTestProtocolReplayStore(t, root)
	tr, err := store.Begin("ssh:c", workerproto.Envelope{SessionID: "s", RequestID: "r", Sequence: 1}, "digest", 8)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := tr.CachedResponse(); ok {
		t.Fatal("killed handler returned a completed response")
	}
	if err := tr.Complete(workerproto.Envelope{RequestID: "response-r"}); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkProtocolReplayBoundedHistory(b *testing.B) {
	store, err := OpenProtocolReplayStore(b.TempDir(), "coordinator", "worker", 1, "worker-1")
	if err != nil {
		b.Fatal(err)
	}
	body := json.RawMessage(fmt.Sprintf("%q", string(bytes.Repeat([]byte("x"), 128<<10))))
	exchange := func(i int) {
		req := workerproto.Envelope{SessionID: "s", RequestID: fmt.Sprint(i), Sequence: int64(i)}
		tr, err := store.Begin("ssh:c", req, "digest", 4096)
		if err != nil {
			b.Fatal(err)
		}
		if err := tr.Complete(workerproto.Envelope{RequestID: "response", Payload: body}); err != nil {
			b.Fatal(err)
		}
	}
	for i := 1; i <= 256; i++ {
		exchange(i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 1; i <= b.N; i++ {
		exchange(256 + i)
	}
}
