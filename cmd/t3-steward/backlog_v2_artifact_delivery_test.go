package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type artifactDeliveryOfferBuilder struct {
	offer workerproto.AssignmentOffer
}

func (b artifactDeliveryOfferBuilder) BuildAssignmentOffer(context.Context, domain.Assignment, time.Time) (workerproto.AssignmentOffer, error) {
	return b.offer, nil
}

type artifactDeliveryTransport struct {
	t   *testing.T
	raw []byte
}

func (t *artifactDeliveryTransport) RoundTripArtifactDownloadWithRetry(_ context.Context, request workerproto.Envelope, _ workerproto.RetryPolicy, raw io.ReadSeeker, size int64) (workerproto.Envelope, error) {
	t.t.Helper()
	var manifest workerproto.ArtifactTransferManifest
	if err := workerproto.DecodePayload(request, workerproto.MessageArtifactDownload, &manifest); err != nil {
		t.t.Fatal(err)
	}
	t.raw = make([]byte, size)
	if _, err := io.ReadFull(raw, t.raw); err != nil {
		t.t.Fatal(err)
	}
	records := make([]workerproto.ArtifactCustodyRecord, 0, len(manifest.Objects))
	previous := ""
	for index, object := range manifest.Objects {
		record, err := workerproto.BuildCustodyRecord(workerproto.ArtifactCustodyRecord{
			ManifestID: manifest.ID, ObjectID: object.ID,
			From: "coordinator:" + request.Sender, To: "worker:" + request.Recipient,
			Sequence: int64(index + 1), Size: object.Size, SHA256: object.SHA256,
			VerifiedAt: request.SentAt, PreviousSHA256: previous,
		})
		if err != nil {
			t.t.Fatal(err)
		}
		previous = record.RecordSHA256
		records = append(records, record)
	}
	response, err := workerproto.NewEnvelope(
		workerproto.MessageArtifactDownload, request.SessionID, "response-"+request.RequestID,
		request.Recipient, request.Sender, request.CoordinatorEpoch, request.WorkerEpoch,
		request.Sequence, request.SentAt, request.Deadline,
		workerproto.ArtifactDownloadReceipt{ManifestID: manifest.ID, Custody: records},
	)
	if err != nil {
		return workerproto.Envelope{}, err
	}
	response.InReplyTo = request.RequestID
	return response, nil
}

func TestCoordinatorDeliveringOfferBuilderTransfersSubmissionArtifactBeforeOffer(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 11, 16, 0, 0, 0, time.UTC)
	content := []byte("successor prompt\n")
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	root := t.TempDir()
	storagePath := filepath.Join("workflows", "workflow-1", "files", "prompts", "task.md")
	fullPath := filepath.Join(root, storagePath)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fullPath, content, 0o400); err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	artifact := domain.Artifact{
		ID: "artifact-1", WorkflowRunID: "run-1", TaskID: "task-1",
		Kind: domain.ArtifactInput, Name: "prompts/task.md", MediaType: "text/markdown",
		Size: int64(len(content)), SHA256: hash, StoragePath: filepath.ToSlash(storagePath),
		Producer: "submission", CreatedAt: now,
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Artifacts: []domain.Artifact{artifact}}); err != nil {
		t.Fatal(err)
	}
	assignment := domain.Assignment{
		ID: "assignment-1", AttemptID: "attempt-1", WorkerID: "normandy",
		WorkerEpoch: "worker-1", Epoch: 1, State: domain.AssignmentOffered,
		CreatedAt: now, LeaseExpiresAt: now.Add(time.Minute),
	}
	object := workerproto.ArtifactObject{
		ID: artifact.ID, Path: "prompt/prompts/task.md", Kind: "prompt",
		MediaType: artifact.MediaType, Size: artifact.Size, SHA256: artifact.SHA256,
	}
	offer := workerproto.AssignmentOffer{
		Assignment: assignment,
		Package: workerproto.ExecutionPackageManifest{Package: workerproto.ExecutionPackage{
			CoordinatorID: "coordinator", CoordinatorEpoch: 7,
			WorkerID: "normandy", WorkerEpoch: "worker-1", Prompt: object,
		}},
		ExpiresAt: assignment.LeaseExpiresAt,
	}
	transport := &artifactDeliveryTransport{t: t}
	builder := coordinatorDeliveringOfferBuilder{
		Base: artifactDeliveryOfferBuilder{offer: offer}, Transport: transport,
		Artifacts:     backlog.CoordinatorArtifactStore{Root: t.TempDir(), SubmissionRoot: root, Catalog: store},
		CoordinatorID: "coordinator", CoordinatorEpoch: 7, WorkerID: "normandy", WorkerEpoch: "worker-1",
		SignerPrincipal: "coordinator", SignerKeyID: "key", SignerSecret: []byte("coordinator-secret-material"),
		RequestTimeout: time.Minute, RetryPolicy: workerproto.RetryPolicy{MaxAttempts: 1},
		MaxArtifactBytes: 1024, MaxTotalBytes: 1024, Now: func() time.Time { return now },
	}
	got, err := builder.BuildAssignmentOffer(ctx, assignment, offer.ExpiresAt)
	if err != nil {
		t.Fatal(err)
	}
	if got.Assignment.ID != assignment.ID || !bytes.Equal(transport.raw, content) {
		t.Fatalf("offer=%#v raw=%q", got, transport.raw)
	}
}
