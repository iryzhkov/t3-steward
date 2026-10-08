package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// workerResultQueue is one worker's outbox of results: a poll announces the
// oldest unacknowledged upload and an acknowledgement removes it, so an upload
// that is never acknowledged keeps every later one behind it.
type workerResultQueue struct {
	uploads      []workerproto.ArtifactUploadResponse
	raw          [][]byte
	acknowledged []string
}

func (q *workerResultQueue) RoundTripWithRetry(_ context.Context, request workerproto.Envelope, _ workerproto.RetryPolicy) (workerproto.Envelope, error) {
	switch request.Type {
	case workerproto.MessageArtifactPoll:
		var head *workerproto.ArtifactUploadResponse
		if len(q.uploads) > 0 {
			head = &q.uploads[0]
		}
		return coordinatorResultResponse(request, workerproto.MessageArtifactAnnouncement, workerproto.ArtifactAnnouncement{Upload: head})
	case workerproto.MessageArtifactAcknowledge:
		if len(q.uploads) == 0 {
			return workerproto.Envelope{}, errors.New("acknowledged an empty outbox")
		}
		id := q.uploads[0].Manifest.ID
		q.acknowledged = append(q.acknowledged, id)
		q.uploads, q.raw = q.uploads[1:], q.raw[1:]
		return coordinatorResultResponse(request, workerproto.MessageArtifactAcknowledged, workerproto.ArtifactAcknowledgement{ManifestID: id})
	default:
		return workerproto.Envelope{}, errors.New("unexpected control request")
	}
}

// workerResultQueueArtifacts serves the bytes of the upload at the head of
// the queue.
type workerResultQueueArtifacts struct{ queue *workerResultQueue }

func (t workerResultQueueArtifacts) RoundTripWithRetry(context.Context, workerproto.Envelope, workerproto.RetryPolicy) (workerproto.Envelope, error) {
	return workerproto.Envelope{}, errors.New("unexpected ordinary artifact request")
}

func (t workerResultQueueArtifacts) RoundTripArtifactWithRetry(_ context.Context, request workerproto.Envelope, _ workerproto.RetryPolicy, _ int64) (workerproto.Envelope, []byte, error) {
	if len(t.queue.uploads) == 0 {
		return workerproto.Envelope{}, nil, errors.New("fetched from an empty outbox")
	}
	response, err := coordinatorResultResponse(request, workerproto.MessageArtifactUpload, t.queue.uploads[0])
	return response, append([]byte(nil), t.queue.raw[0]...), err
}

// One worker holds a result whose output has the wrong media type and, behind
// it, a valid result of another task. The invalid one used to fail every pass
// without an acknowledgement, so its attempt stayed verifying and the valid
// one was never reached. Now the first pass settles the invalid attempt as
// failed and acknowledges it, and the second imports the valid result.
func TestAnInvalidResultIsSettledAndDoesNotBlockTheWorkersNextResult(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 10, 15, 0, 0, 0, time.UTC)
	store, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	archive := []byte(`{"thread":{"id":"thread-%d","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-%d","status":"ready","activeTurnId":null,"lastError":null}}}`)
	records := sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1"}}}
	queue := &workerResultQueue{}
	for index, outputMedia := range []string{"application/octet-stream", "text/plain"} {
		n := index + 1
		task := domain.Task{ID: fmt.Sprintf("task-%d", n), WorkflowID: "workflow-1", Name: fmt.Sprintf("task-%d", n), Outputs: []domain.ArtifactDeclaration{{Name: "answer.txt", MediaType: "text/plain"}}}
		attempt := domain.Attempt{ID: fmt.Sprintf("attempt-%d", n), WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressVerifying, Control: domain.ControlStopped, Revision: 2, AssignmentID: fmt.Sprintf("assignment-%d", n), UpdatedAt: now}
		assignment := domain.Assignment{ID: attempt.AssignmentID, AttemptID: attempt.ID, WorkerID: "normandy", WorkerEpoch: "worker-1", State: domain.AssignmentCompleted, ThreadID: fmt.Sprintf("thread-%d", n), Epoch: 1, LeaseToken: fmt.Sprintf("lease-%d", n), DispatchToken: fmt.Sprintf("dispatch-%d", n), CreatedAt: now, UpdatedAt: now}
		records.Tasks = append(records.Tasks, task)
		records.Attempts = append(records.Attempts, attempt)
		records.Assignments = append(records.Assignments, assignment)
		contents := [][]byte{[]byte("answer\n"), []byte("BACKLOG STATUS: done\n"), []byte(fmt.Sprintf(string(archive), n, n))}
		objects := []workerproto.ArtifactObject{
			coordinatorResultObject(fmt.Sprintf("output-%d", n), "results/answer.txt", "output", outputMedia, contents[0]),
			coordinatorResultObject("final-message-"+attempt.ID, "results/final-message.md", "summary", "text/markdown", contents[1]),
			coordinatorResultObject("thread-archive-"+attempt.ID, "results/thread.json", "log", "application/json", contents[2]),
		}
		manifest := workerproto.ArtifactTransferManifest{Version: 1, ID: "upload-" + assignment.ID + "-result", Direction: "upload", CoordinatorEpoch: 1, WorkerID: "normandy", WorkerEpoch: "worker-1", AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, Objects: objects, TotalBytes: int64(len(contents[0]) + len(contents[1]) + len(contents[2])), CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
		queue.uploads = append(queue.uploads, workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: coordinatorResultCustody(t, manifest)})
		queue.raw = append(queue.raw, bytes.Join(contents, nil))
	}
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	session := coordinatorWorkerSession{
		Client:         coordinatorResultClient(t, now, "control-queue", queue),
		ArtifactClient: coordinatorResultClient(t, now, "artifact-queue", workerResultQueueArtifacts{queue: queue}),
		Importer:       backlog.CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: backlog.CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 1024, MaxTotalBytes: 1024, Now: func() time.Time { return now.Add(time.Minute) }},
	}

	first, err := importCoordinatorWorkerResult(ctx, session, backlog.WorkerExchangeReport{}, 1024)
	if err != nil {
		t.Fatalf("the invalid result failed the pass instead of settling: %v", err)
	}
	if len(first.Imports) != 0 || len(queue.acknowledged) != 1 || queue.acknowledged[0] != "upload-assignment-1-result" {
		t.Fatalf("first pass imports=%d acknowledged=%v", len(first.Imports), queue.acknowledged)
	}
	second, err := importCoordinatorWorkerResult(ctx, session, backlog.WorkerExchangeReport{}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Imports) != 1 || len(queue.acknowledged) != 2 || queue.acknowledged[1] != "upload-assignment-2-result" {
		t.Fatalf("second pass imports=%d acknowledged=%v", len(second.Imports), queue.acknowledged)
	}
	saved, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	progress := map[string]domain.ProgressState{}
	for _, attempt := range saved.Attempts {
		progress[attempt.ID] = attempt.Progress
	}
	if progress["attempt-1"] != domain.ProgressFailed || progress["attempt-2"] != domain.ProgressSucceeded {
		t.Fatalf("attempt progress = %v, want the invalid one failed and the valid one succeeded", progress)
	}
	for _, artifact := range saved.Artifacts {
		if artifact.AttemptID == "attempt-1" {
			t.Fatalf("the rejected result published artifact %q", artifact.ID)
		}
	}
}
