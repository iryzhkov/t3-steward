package workerruntime

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func TestWorkerArtifactStreamsAuthenticateBoundVerifyAndReplay(t *testing.T) {
	service := testWorkerService(t)
	object := testArtifact("input-1", "inputs/context.md", "trusted context")
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
	request := signedWorkerRequest(t, workerproto.MessageArtifactDownload, 1, manifest)
	input := append(mustEncodeEnvelope(t, service.Codec, request), []byte("trusted context")...)
	var output bytes.Buffer
	if err := service.ServeArtifactReceive(context.Background(), bytes.NewReader(input), &output); err != nil {
		t.Fatalf("receive artifact: %v", err)
	}
	response := decodeStreamResponse(t, service.Codec, &output)
	var receipt workerproto.ArtifactDownloadReceipt
	if err := workerproto.DecodePayload(response, workerproto.MessageArtifactDownload, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.ManifestID != manifest.ID || len(receipt.Custody) != 1 {
		t.Fatalf("receipt = %#v", receipt)
	}
	reader, err := service.Exchange.Custody.OpenArtifact(context.Background(), object)
	if err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || string(content) != "trusted context" {
		t.Fatalf("retained content = %q, %v", content, err)
	}

	output.Reset()
	if err := service.ServeArtifactReceive(context.Background(), bytes.NewReader(input), &output); err != nil {
		t.Fatalf("receive exact replay: %v", err)
	}
	replayed := decodeStreamResponse(t, service.Codec, &output)
	if !bytes.Equal(replayed.Payload, response.Payload) {
		t.Fatal("exact replay changed signed receipt payload")
	}

	if _, err := service.Exchange.Custody.PublishCheckpoint(context.Background(), testPackage(), ".t3/checkpoint.md", []byte("checkpoint")); err != nil {
		t.Fatal(err)
	}
	pending, err := service.Exchange.Custody.PendingUploads()
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending = %#v, %v", pending, err)
	}
	ids := make([]string, 0, len(pending[0].Manifest.Objects))
	for _, item := range pending[0].Manifest.Objects {
		ids = append(ids, item.ID)
	}
	uploadRequest := signedWorkerRequest(t, workerproto.MessageArtifactUpload, 2, workerproto.ArtifactDownloadRequest{
		ManifestID: pending[0].Manifest.ID,
		ObjectIDs:  ids,
	})
	output.Reset()
	if err := service.ServeArtifactSend(context.Background(), bytes.NewReader(mustEncodeEnvelope(t, service.Codec, uploadRequest)), &output); err != nil {
		t.Fatalf("send artifact: %v", err)
	}
	buffered := bufio.NewReader(&output)
	header, err := buffered.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	uploadResponse := decodeStreamResponse(t, service.Codec, bytes.NewReader(header))
	var metadata workerproto.ArtifactUploadResponse
	if err := workerproto.DecodePayload(uploadResponse, workerproto.MessageArtifactUpload, &metadata); err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(buffered)
	if err != nil || string(raw) != "checkpoint" || metadata.Manifest.ID != pending[0].Manifest.ID ||
		len(metadata.Custody) != 1 {
		t.Fatalf("upload metadata=%#v content=%q err=%v", metadata, raw, err)
	}
}

func TestWorkerArtifactStreamRejectsTrailingAndUnauthorizedInput(t *testing.T) {
	service := testWorkerService(t)
	object := testArtifact("input-1", "inputs/context.md", "trusted context")
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
	request := signedWorkerRequest(t, workerproto.MessageArtifactDownload, 1, manifest)
	input := append(mustEncodeEnvelope(t, service.Codec, request), []byte("trusted contextx")...)
	var output bytes.Buffer
	if err := service.ServeArtifactReceive(context.Background(), bytes.NewReader(input), &output); err != nil {
		t.Fatal(err)
	}
	response := decodeStreamResponse(t, service.Codec, &output)
	if response.Type != workerproto.MessageError {
		t.Fatalf("trailing input response = %s", response.Type)
	}

	unauthorized := request
	unauthorized.RequestID = "unauthorized"
	unauthorized.Authentication.Signature = string(bytes.Repeat([]byte{'0'}, 64))
	if err := service.ServeArtifactReceive(context.Background(), bytes.NewReader(append(
		mustEncodeEnvelope(t, service.Codec, unauthorized), []byte("trusted context")...,
	)), io.Discard); err == nil {
		t.Fatal("unauthorized artifact stream accepted")
	}
}

func testWorkerService(t *testing.T) *WorkerService {
	t.Helper()
	service, err := NewWorkerService(context.Background(), WorkerServiceOptions{
		Settings:            testWorkerServiceSettings(t),
		WorkerID:            "normandy",
		WorkerEpoch:         "worker-1",
		CoordinatorEpoch:    9,
		ProtocolCredentials: &staticProtocolResolver{credentials: testProtocolCredentials()},
		DryRun:              true,
		Now:                 func() time.Time { return runtimeTestNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func decodeStreamResponse(t *testing.T, codec workerproto.Codec, input io.Reader) workerproto.Envelope {
	t.Helper()
	var response workerproto.Envelope
	if err := codec.Decode(input, &response); err != nil {
		t.Fatal(err)
	}
	if err := workerproto.VerifyEnvelopeSignature(response, []byte("worker-response-secret")); err != nil {
		t.Fatal(err)
	}
	return response
}
