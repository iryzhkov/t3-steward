package workerruntime

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestCustodyDownloadSurvivesRestartAndRejectsMutation(t *testing.T) {
	root := t.TempDir()
	store := testCustodyStore(t, root, func() time.Time { return runtimeTestNow })
	object := testArtifact("input-1", "inputs/input.md", "trusted input")
	manifest := workerproto.ArtifactTransferManifest{
		Version:          workerproto.ArtifactManifestVersion,
		ID:               "download-1",
		Direction:        "download",
		CoordinatorEpoch: 9,
		WorkerID:         "normandy",
		WorkerEpoch:      "worker-1",
		AssignmentID:     "assignment-1",
		AssignmentEpoch:  2,
		Objects:          []workerproto.ArtifactObject{object},
		TotalBytes:       object.Size,
		CreatedAt:        runtimeTestNow.Add(-time.Minute),
		ExpiresAt:        runtimeTestNow.Add(time.Hour),
	}
	records, err := store.ReceiveDownload(context.Background(), manifest, map[string]io.Reader{
		object.ID: strings.NewReader("trusted input"),
	})
	if err != nil {
		t.Fatalf("receive download: %v", err)
	}
	if len(records) != 1 || records[0].From != "coordinator:coordinator" ||
		records[0].To != "worker:normandy" {
		t.Fatalf("custody records = %#v", records)
	}

	restarted := testCustodyStore(t, root, func() time.Time { return runtimeTestNow.Add(time.Minute) })
	reader, err := restarted.OpenArtifact(context.Background(), object)
	if err != nil {
		t.Fatalf("open after restart: %v", err)
	}
	got, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || string(got) != "trusted input" {
		t.Fatalf("artifact = %q, %v", got, err)
	}
	replayed, err := restarted.ReceiveDownload(context.Background(), manifest, nil)
	if err != nil || len(replayed) != 1 || replayed[0].RecordSHA256 != records[0].RecordSHA256 {
		t.Fatalf("replay = %#v, %v", replayed, err)
	}

	changed := manifest
	changed.AssignmentEpoch++
	if _, err := restarted.ReceiveDownload(context.Background(), changed, nil); err == nil ||
		!strings.Contains(err.Error(), "changed immutable content") {
		t.Fatalf("changed replay error = %v", err)
	}

	objectPath := restarted.objectPath(object.SHA256)
	if err := os.Chmod(objectPath, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(objectPath, []byte("corrupt input"), 0o444); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.OpenArtifact(context.Background(), object); err == nil ||
		!strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("tampered object error = %v", err)
	}
}

func TestCustodyPublishesRestartSafeResultAndCheckpoint(t *testing.T) {
	root := t.TempDir()
	now := runtimeTestNow
	store := testCustodyStore(t, filepath.Join(root, "custody"), func() time.Time { return now })
	pkg := testPackage()

	finalDir := filepath.Join(root, "final", "runs", "run-1", "task-1", "attempt-1")
	outputPath := filepath.Join(finalDir, "artifacts", "outputs", "result.txt")
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		t.Fatal(err)
	}
	content := []byte("verified result")
	if err := os.WriteFile(outputPath, content, 0o444); err != nil {
		t.Fatal(err)
	}
	output := objectForBytes("artifact-result", "unused", "output", "text/plain", content)
	artifact := domain.Artifact{
		ID:            output.ID,
		WorkflowRunID: "run-1",
		TaskID:        "task-1",
		AttemptID:     "attempt-1",
		Kind:          domain.ArtifactOutput,
		Name:          "result.txt",
		MediaType:     output.MediaType,
		Size:          output.Size,
		SHA256:        output.SHA256,
		StoragePath:   "runs/run-1/task-1/attempt-1/artifacts/outputs/result.txt",
	}
	result := PublishedResult{
		Finalized:     backlog.FinalizedAttempt{Artifacts: []domain.Artifact{artifact}, StorageDir: finalDir},
		FinalMessage:  "done",
		ThreadArchive: []byte("{}"),
	}
	if err := store.PublishResult(context.Background(), pkg, result); err != nil {
		t.Fatalf("publish result: %v", err)
	}
	now = now.Add(time.Hour)
	if err := store.PublishResult(context.Background(), pkg, result); err != nil {
		t.Fatalf("idempotent result replay: %v", err)
	}
	changed := result
	changed.FinalMessage = "changed"
	if err := store.PublishResult(context.Background(), pkg, changed); err == nil ||
		!strings.Contains(err.Error(), "changed immutable content") {
		t.Fatalf("changed result replay error = %v", err)
	}

	checkpoint, err := store.PublishCheckpoint(context.Background(), pkg, "worker/checkpoint.json", []byte("{\"turn\":3}"))
	if err != nil {
		t.Fatalf("publish checkpoint: %v", err)
	}
	if checkpoint.ArtifactID == "" || checkpoint.Path != "worker/checkpoint.json" || checkpoint.Size == 0 {
		t.Fatalf("checkpoint = %#v", checkpoint)
	}

	restarted := testCustodyStore(t, filepath.Join(root, "custody"), func() time.Time { return now })
	pending, err := restarted.PendingUploads()
	if err != nil {
		t.Fatalf("reload uploads: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending uploads = %d, want 2", len(pending))
	}
	if pending[0].Manifest.Direction != "upload" || len(pending[0].Custody) != len(pending[0].Manifest.Objects) {
		t.Fatalf("pending result = %#v", pending[0])
	}
	var resultPath string
	for _, upload := range pending {
		if upload.Manifest.ID == "upload-assignment-1-result" {
			resultPath = upload.Manifest.Objects[0].Path
		}
	}
	if resultPath != "results/result.txt" {
		t.Fatalf("result output path = %q", resultPath)
	}
	resultUpload, err := restarted.PendingUploadByPurpose("result")
	if err != nil || resultUpload == nil || resultUpload.Manifest.ID != "upload-assignment-1-result" {
		t.Fatalf("pending result = %#v, %v", resultUpload, err)
	}
	if err := restarted.AcknowledgeUpload(resultUpload.Manifest.ID); err != nil {
		t.Fatal(err)
	}
	if err := restarted.AcknowledgeUpload(resultUpload.Manifest.ID); err != nil {
		t.Fatalf("acknowledgement replay: %v", err)
	}
	if resultUpload, err = restarted.PendingUploadByPurpose("result"); err != nil || resultUpload != nil {
		t.Fatalf("acknowledged result rediscovered = %#v, %v", resultUpload, err)
	}
	checkpointUpload, err := restarted.PendingUploadByPurpose("checkpoint")
	if err != nil || checkpointUpload == nil {
		t.Fatalf("pending checkpoint = %#v, %v", checkpointUpload, err)
	}
	for _, upload := range pending {
		for _, object := range upload.Manifest.Objects {
			reader, err := restarted.OpenArtifact(context.Background(), object)
			if err != nil {
				t.Fatalf("open published object %q: %v", object.ID, err)
			}
			got, readErr := io.ReadAll(reader)
			reader.Close()
			if readErr != nil || int64(len(got)) != object.Size {
				t.Fatalf("published object %q = %d bytes, %v", object.ID, len(got), readErr)
			}
		}
	}
}

func TestCustodyRejectsWrongEpochAndChecksumBeforeReceipt(t *testing.T) {
	root := t.TempDir()
	store := testCustodyStore(t, root, func() time.Time { return runtimeTestNow })
	object := testArtifact("input-1", "inputs/input.md", "trusted input")
	manifest := workerproto.ArtifactTransferManifest{
		Version:          workerproto.ArtifactManifestVersion,
		ID:               "download-1",
		Direction:        "download",
		CoordinatorEpoch: 10,
		WorkerID:         "normandy",
		WorkerEpoch:      "worker-1",
		AssignmentID:     "assignment-1",
		AssignmentEpoch:  2,
		Objects:          []workerproto.ArtifactObject{object},
		TotalBytes:       object.Size,
		CreatedAt:        runtimeTestNow.Add(-time.Minute),
		ExpiresAt:        runtimeTestNow.Add(time.Hour),
	}
	if _, err := store.ReceiveDownload(context.Background(), manifest, map[string]io.Reader{
		object.ID: strings.NewReader("trusted input"),
	}); err == nil || !strings.Contains(err.Error(), "epoch binding") {
		t.Fatalf("wrong epoch error = %v", err)
	}
	manifest.CoordinatorEpoch = 9
	if _, err := store.ReceiveDownload(context.Background(), manifest, map[string]io.Reader{
		object.ID: bytes.NewReader([]byte("wrong")),
	}); err == nil || !strings.Contains(err.Error(), "size mismatch") {
		t.Fatalf("bad object error = %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "receipts"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("receipts after rejection = %v, %v", entries, err)
	}
}

func TestCustodyRejectsMalformedOrMisplacedOutboxRecord(t *testing.T) {
	root := t.TempDir()
	store := testCustodyStore(t, root, func() time.Time { return runtimeTestNow })
	if _, err := store.PublishCheckpoint(context.Background(), testPackage(), ".t3/checkpoint.md", []byte("checkpoint")); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "outbox"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("outbox = %v, %v", entries, err)
	}
	path := filepath.Join(root, "outbox", entries[0].Name())
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(append([]byte(nil), original...), []byte("junk")...), 0o444); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PendingUploads(); err == nil || !strings.Contains(err.Error(), "trailing content") {
		t.Fatalf("trailing content error = %v", err)
	}
	changed := bytes.Replace(original, []byte(`"direction":"upload"`), []byte(`"direction":"download"`), 1)
	if err := os.WriteFile(path, changed, 0o444); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PendingUploads(); err == nil || !strings.Contains(err.Error(), "non-upload") {
		t.Fatalf("misplaced manifest error = %v", err)
	}
}

func testCustodyStore(t *testing.T, root string, now func() time.Time) *CustodyStore {
	t.Helper()
	store, err := OpenCustodyStore(CustodyConfig{
		Root:             root,
		CoordinatorID:    "coordinator",
		CoordinatorEpoch: 9,
		WorkerID:         "normandy",
		WorkerEpoch:      "worker-1",
		MaxArtifactBytes: 1 << 20,
		MaxTotalBytes:    2 << 20,
		Now:              now,
	})
	if err != nil {
		t.Fatalf("open custody: %v", err)
	}
	return store
}
