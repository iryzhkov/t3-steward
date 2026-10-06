package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// pendingUpload is one upload a fake worker holds in custody.
type pendingUpload struct {
	upload workerproto.ArtifactUploadResponse
	raw    []byte
	acked  bool
}

// queuedUploadControl is a worker's control endpoint for a queue of
// checkpoint uploads: a poll announces the first one neither acknowledged nor
// excluded, and results are never pending.
type queuedUploadControl struct{ uploads []*pendingUpload }

func (t *queuedUploadControl) RoundTripWithRetry(_ context.Context, request workerproto.Envelope, _ workerproto.RetryPolicy) (workerproto.Envelope, error) {
	switch request.Type {
	case workerproto.MessageArtifactPoll:
		var poll workerproto.ArtifactPollRequest
		if err := json.Unmarshal(request.Payload, &poll); err != nil {
			return workerproto.Envelope{}, err
		}
		var announced *workerproto.ArtifactUploadResponse
		for _, pending := range t.uploads {
			if poll.Purpose == "checkpoint" && !pending.acked && !slices.Contains(poll.Exclude, pending.upload.Manifest.ID) {
				announced = &pending.upload
				break
			}
		}
		return coordinatorResultResponse(request, workerproto.MessageArtifactAnnouncement, workerproto.ArtifactAnnouncement{Upload: announced})
	case workerproto.MessageArtifactAcknowledge:
		var ack workerproto.ArtifactAcknowledgeRequest
		if err := json.Unmarshal(request.Payload, &ack); err != nil {
			return workerproto.Envelope{}, err
		}
		for _, pending := range t.uploads {
			if pending.upload.Manifest.ID == ack.ManifestID {
				pending.acked = true
			}
		}
		return coordinatorResultResponse(request, workerproto.MessageArtifactAcknowledged, workerproto.ArtifactAcknowledgement{ManifestID: ack.ManifestID})
	default:
		return workerproto.Envelope{}, errors.New("unexpected control request")
	}
}

// queuedUploadArtifacts serves the bytes of the uploads in a queue.
type queuedUploadArtifacts struct{ control *queuedUploadControl }

func (t queuedUploadArtifacts) RoundTripWithRetry(context.Context, workerproto.Envelope, workerproto.RetryPolicy) (workerproto.Envelope, error) {
	return workerproto.Envelope{}, errors.New("unexpected ordinary artifact request")
}

func (t queuedUploadArtifacts) RoundTripArtifactWithRetry(_ context.Context, request workerproto.Envelope, _ workerproto.RetryPolicy, _ int64) (workerproto.Envelope, []byte, error) {
	var download workerproto.ArtifactDownloadRequest
	if err := json.Unmarshal(request.Payload, &download); err != nil {
		return workerproto.Envelope{}, nil, err
	}
	for _, pending := range t.control.uploads {
		if pending.upload.Manifest.ID == download.ManifestID {
			response, err := coordinatorResultResponse(request, workerproto.MessageArtifactUpload, pending.upload)
			return response, append([]byte(nil), pending.raw...), err
		}
	}
	return workerproto.Envelope{}, nil, errors.New("unknown upload")
}

// Review round 2, R1: a worker queued a continuation snapshot, then the
// attempt's assignment was released before the coordinator polled it. The
// exchange imports the snapshot before reconcile builds any offer, so the
// replacement's frozen package can carry it; the other checkpoint upload the
// worker holds is left for the ordinary pass after reconcile.
func TestTheExchangeImportsContinuationSnapshotsBeforeItBuildsOffers(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 10, 16, 0, 0, 0, time.UTC)
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	task := domain.Task{ID: "task-1", WorkflowID: "workflow-1", Name: "task"}
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressFailed, Control: domain.ControlStopped, Revision: 2, AssignmentID: "assignment-1", UpdatedAt: now}
	assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "normandy", WorkerEpoch: "worker-1", State: domain.AssignmentReleased, Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: task.WorkflowID}}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	upload := func(id string, objects []workerproto.ArtifactObject, raw []byte) *pendingUpload {
		var total int64
		for _, object := range objects {
			total += object.Size
		}
		manifest := workerproto.ArtifactTransferManifest{Version: 1, ID: id, Direction: "upload", CoordinatorEpoch: 1, WorkerID: "normandy", WorkerEpoch: "worker-1", AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, Objects: objects, TotalBytes: total, CreatedAt: now.Add(time.Minute), ExpiresAt: now.Add(time.Hour)}
		return &pendingUpload{upload: workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: coordinatorResultCustody(t, manifest)}, raw: raw}
	}
	// A throttle checkpoint, first in the queue, that no evidence names.
	other := []byte("resume here\n")
	throttle := upload("upload-assignment-1-checkpoint-deadbeef", []workerproto.ArtifactObject{
		coordinatorResultObject("checkpoint-attempt-1-deadbeef", "checkpoints/checkpoint-attempt-1-deadbeef.md", "checkpoint", "text/markdown", other),
	}, other)
	// The continuation snapshot the attempt queued before it was released.
	snapshot := []byte("step 2 of 3\n")
	snapshotID, metadataID := domain.ContinuationLiveArtifactID(attempt.ID, 1, 1), domain.ContinuationLiveMetadataArtifactID(attempt.ID, 1, 1)
	snapshotObject := coordinatorResultObject(snapshotID, "checkpoints/"+snapshotID+".md", "checkpoint", "text/markdown", snapshot)
	metadata, err := json.Marshal(domain.ContinuationCheckpoint{AttemptID: attempt.ID, Sequence: 1, Turn: "turn-2", Boundary: domain.ContinuationTurnEnd,
		SHA256: snapshotObject.SHA256, Size: snapshotObject.Size, OriginalSize: snapshotObject.Size, CapturedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	continuation := upload("upload-assignment-1-checkpoint-continuation-00000000000000000001-00000000000000000001", []workerproto.ArtifactObject{
		snapshotObject,
		coordinatorResultObject(metadataID, "checkpoints/"+metadataID+".json", "checkpoint", "application/json", metadata),
	}, bytes.Join([][]byte{snapshot, metadata}, nil))
	control := &queuedUploadControl{uploads: []*pendingUpload{throttle, continuation}}
	session := coordinatorWorkerSession{
		Client:             coordinatorResultClient(t, now, "exchange-control", control),
		ArtifactClient:     coordinatorResultClient(t, now, "exchange-artifact", queuedUploadArtifacts{control: control}),
		CheckpointImporter: backlog.CoordinatorCheckpointImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: backlog.CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 1024, MaxTotalBytes: 1024, Now: func() time.Time { return now.Add(3 * time.Minute) }},
	}
	reconciled := false
	report, err := exchangeCoordinatorWorker(ctx, session, 1024, func(ctx context.Context) (backlog.WorkerExchangeReport, error) {
		reconciled = true
		records, err := store.LoadCoordinatorRecords(ctx)
		if err != nil {
			return backlog.WorkerExchangeReport{}, err
		}
		if backlog.LatestContinuationArtifact(records.Artifacts, records.Attempts, attempt.WorkflowRunID, task.ID) == nil {
			t.Error("offers were built before the queued continuation snapshot was imported")
		}
		if throttle.acked {
			t.Error("the first pass consumed an upload that is not a continuation snapshot")
		}
		return backlog.WorkerExchangeReport{}, nil
	})
	if err != nil || !reconciled {
		t.Fatalf("exchange = %v, reconciled %t", err, reconciled)
	}
	if !continuation.acked || len(report.Checkpoints) != 1 || report.Checkpoints[0].ID != snapshotObject.ID {
		t.Fatalf("report = %+v, continuation acknowledged %t", report.Checkpoints, continuation.acked)
	}
	// The ordinary pass after reconcile still disposes of the other upload.
	if !throttle.acked {
		t.Fatal("the throttle checkpoint was not handled after reconcile")
	}
}
