package workerruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// uploadedThreadArchive returns the thread archive object of the collected
// result upload and its bytes as custody holds them.
func uploadedThreadArchive(t *testing.T, f *collectionFixture) (workerproto.ArtifactObject, []byte) {
	t.Helper()
	pending, err := f.custody.PendingUploadByPurpose("result")
	if err != nil || pending == nil {
		t.Fatalf("no result upload: %v", err)
	}
	for _, object := range pending.Manifest.Objects {
		if object.Path != "results/thread.json" {
			continue
		}
		reader, err := f.custody.OpenArtifact(context.Background(), object)
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		return object, data
	}
	t.Fatal("result upload has no thread archive")
	return workerproto.ArtifactObject{}, nil
}

// compactableArchive is a clean, completed thread whose size is in its
// messages, the shape of the field archive of 2026-10-06: a long session.
func compactableArchive(t *testing.T, size int) []byte {
	t.Helper()
	messages := make([]map[string]string, 0, size/200+1)
	for index := 0; index*200 < size; index++ {
		messages = append(messages, map[string]string{
			"id": fmt.Sprintf("m%d", index), "role": "assistant", "text": strings.Repeat("work ", 36), "createdAt": "2026-09-13T05:00:30Z",
		})
	}
	raw, err := json.Marshal(messages)
	if err != nil {
		t.Fatal(err)
	}
	return []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},` +
		`"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null},"messages":` + string(raw) + `}}`)
}

// The field defect of 2026-10-06: a finished task whose thread archive alone
// was over the per-object limit was never collected. It now collects, with a
// compacted archive that names the full one kept on the worker.
func TestOversizeThreadArchiveCollectsCompacted(t *testing.T) {
	f := newCollectionFixture(t, 8192, 16384, 100, 100)
	f.control.archive = compactableArchive(t, 20000)
	full := append([]byte(nil), f.control.archive...)
	if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
		t.Fatalf("collection failed: %v", err)
	}
	if record := f.record(t); record.Phase != PhaseCompleted || record.Failure != "" {
		t.Fatalf("record=%+v", record)
	}
	object, uploaded := uploadedThreadArchive(t, f)
	if object.Size > 8192 || bytes.Equal(uploaded, full) {
		t.Fatalf("archive uploaded uncompacted: %d bytes", object.Size)
	}
	var compacted struct {
		Truncation *backlog.ThreadArchiveTruncation `json:"stewardTruncation"`
	}
	if err := json.Unmarshal(uploaded, &compacted); err != nil || compacted.Truncation == nil {
		t.Fatalf("no truncation marker: %v", err)
	}
	sum := sha256.Sum256(full)
	marker := *compacted.Truncation
	if marker.OriginalSize != int64(len(full)) || marker.OriginalSHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("marker=%+v", marker)
	}
	retained, err := os.ReadFile(filepath.Join(f.custody.config.Root, filepath.FromSlash(marker.RetainedPath)))
	if err != nil || !bytes.Equal(retained, full) {
		t.Fatalf("full archive not retained at %q: %v", marker.RetainedPath, err)
	}
	for _, summary := range []string{"BACKLOG STATUS: done", ""} {
		want, err := backlog.ResultCompletionFailure(full, "thread-1", summary)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := backlog.ResultCompletionFailure(uploaded, "thread-1", summary); err != nil || got != want {
			t.Fatalf("completion changed: %q != %q (%v)", got, want, err)
		}
	}
}

// Data inside the latest turn and the session that no completion decision
// reads is not evidence: an archive whose size is there still collects, with
// those fields left out of the upload.
func TestThreadArchiveWithUnusedTurnDataCollectsCompacted(t *testing.T) {
	f := newCollectionFixture(t, 1024, 4096, 100, 100)
	padding := strings.Repeat("a", 2000)
	f.control.archive = []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z","padding":"` + padding + `"},` +
		`"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null,"padding":"` + padding + `"}}}`)
	full := append([]byte(nil), f.control.archive...)
	if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
		t.Fatalf("collection failed: %v", err)
	}
	if record := f.record(t); record.Phase != PhaseCompleted || record.Failure != "" {
		t.Fatalf("record=%+v", record)
	}
	object, uploaded := uploadedThreadArchive(t, f)
	if object.Size > 1024 || bytes.Contains(uploaded, []byte(padding)) {
		t.Fatalf("unused turn data uploaded: %d bytes", object.Size)
	}
	want, err := backlog.ResultCompletionFailure(full, "thread-1", "BACKLOG STATUS: done")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := backlog.ResultCompletionFailure(uploaded, "thread-1", "BACKLOG STATUS: done"); err != nil || got != want {
		t.Fatalf("completion changed: %q != %q (%v)", got, want, err)
	}
}

// An archive that fits each object but not the aggregate beside the other
// result objects is compacted to the remainder.
func TestThreadArchiveOverTheAggregateIsCompacted(t *testing.T) {
	f := newCollectionFixture(t, 8192, 9000, 4000, 100)
	f.control.archive = compactableArchive(t, 6000)
	if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
		t.Fatalf("collection failed: %v", err)
	}
	if record := f.record(t); record.Phase != PhaseCompleted || record.Failure != "" {
		t.Fatalf("record=%+v", record)
	}
	if object, _ := uploadedThreadArchive(t, f); object.Size >= int64(len(f.control.archive)) {
		t.Fatalf("archive not compacted: %d bytes", object.Size)
	}
}

// An archive within the limits is uploaded byte for byte and nothing is kept
// beside it.
func TestThreadArchiveUnderTheLimitIsUploadedUnchanged(t *testing.T) {
	f := newCollectionFixture(t, 8192, 16384, 100, 100)
	if err := f.runtime.collect(context.Background(), "assignment-1"); err != nil {
		t.Fatal(err)
	}
	if _, uploaded := uploadedThreadArchive(t, f); !bytes.Equal(uploaded, f.control.archive) {
		t.Fatal("archive within the limit was changed")
	}
	if _, err := os.Stat(filepath.Join(f.custody.config.Root, "thread-archives")); !os.IsNotExist(err) {
		t.Fatalf("an archive was retained without compaction: %v", err)
	}
}
