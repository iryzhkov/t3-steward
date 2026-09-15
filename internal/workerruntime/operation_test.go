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

// serveUnpinned is what the restricted endpoint does when its forced command
// names no operation: read the envelope from the channel, ask the signed
// message type which stream discipline carries it, and dispatch.
func serveUnpinned(t *testing.T, service *WorkerService, channel []byte, out io.Writer) (string, error) {
	t.Helper()
	envelope, buffered, err := ReadStreamEnvelope(bytes.NewReader(channel), service.Codec)
	if err != nil {
		t.Fatal(err)
	}
	operation := OperationFor(envelope.Type)
	switch operation {
	case OperationArtifactReceive:
		return operation, service.ServeArtifactReceiveEnvelope(context.Background(), envelope, buffered, out)
	case OperationArtifactSend:
		return operation, service.ServeArtifactSendEnvelope(context.Background(), envelope, buffered, out)
	default:
		return operation, service.ServeEnvelope(context.Background(), envelope, out)
	}
}

// A coordinator dials one worker address for control and for both artifact
// directions, and OpenSSH picks the authorized_keys line by key, so a forced
// command that pinned an operation confined a worker to one of the three. A
// worker pinned to control could run nothing at all: the delivery of an
// assignment's inputs arrived at the control handler, which does not know that
// message, and was refused as a message kind that is not a worker request.
//
// This is that delivery arriving at a forced command that pins nothing. The
// channel carries what an SSH channel really carries, the envelope followed by
// the raw object bytes, and the dispatch is the one the endpoint performs.
func TestArtifactDeliveryReachesAWorkerThroughAnUnpinnedForcedCommand(t *testing.T) {
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
	channel := append(mustEncodeEnvelope(t, service.Codec, request), []byte("trusted context")...)

	var output bytes.Buffer
	operation, err := serveUnpinned(t, service, channel, &output)
	if err != nil {
		t.Fatalf("artifact delivery through an unpinned forced command: %v", err)
	}
	if operation != OperationArtifactReceive {
		t.Fatalf("delivery was dispatched to %q", operation)
	}
	response := decodeStreamResponse(t, service.Codec, &output)
	var receipt workerproto.ArtifactDownloadReceipt
	if err := workerproto.DecodePayload(response, workerproto.MessageArtifactDownload, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.ManifestID != manifest.ID || len(receipt.Custody) != 1 {
		t.Fatalf("receipt = %#v", receipt)
	}
	// The bytes really landed: the worker can open the artifact it was sent.
	reader, err := service.Exchange.Custody.OpenArtifact(context.Background(), object)
	if err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || string(content) != "trusted context" {
		t.Fatalf("retained content = %q, %v", content, err)
	}
}

// The other two disciplines reach the same worker through the same unpinned
// forced command, which is what "one key serves a worker" has to mean.
func TestControlAndArtifactSendShareAnUnpinnedForcedCommand(t *testing.T) {
	service := testWorkerService(t)
	snapshot := signedWorkerRequest(t, workerproto.MessageSnapshot, 1, workerproto.SnapshotRequest{})
	var output bytes.Buffer
	operation, err := serveUnpinned(t, service, mustEncodeEnvelope(t, service.Codec, snapshot), &output)
	if err != nil {
		t.Fatalf("control through an unpinned forced command: %v", err)
	}
	if operation != OperationControl {
		t.Fatalf("a snapshot request was dispatched to %q", operation)
	}
	if response := decodeStreamResponse(t, service.Codec, &output); response.Type != workerproto.MessageObservations {
		t.Fatalf("control answered %q", response.Type)
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
	upload := signedWorkerRequest(t, workerproto.MessageArtifactUpload, 2, workerproto.ArtifactDownloadRequest{
		ManifestID: pending[0].Manifest.ID, ObjectIDs: ids,
	})
	output.Reset()
	operation, err = serveUnpinned(t, service, mustEncodeEnvelope(t, service.Codec, upload), &output)
	if err != nil {
		t.Fatalf("artifact send through an unpinned forced command: %v", err)
	}
	if operation != OperationArtifactSend {
		t.Fatalf("an upload request was dispatched to %q", operation)
	}
	buffered := bufio.NewReader(&output)
	header, err := buffered.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	if metadata := decodeStreamResponse(t, service.Codec, bytes.NewReader(header)); metadata.Type != workerproto.MessageArtifactUpload {
		t.Fatalf("artifact send answered %q", metadata.Type)
	}
	raw, err := io.ReadAll(buffered)
	if err != nil || string(raw) != "checkpoint" {
		t.Fatalf("streamed object = %q, %v", raw, err)
	}
}

// Every message type a worker accepts must land on a discipline that can carry
// it. The allowlist is the set the service admits, so walking it is what keeps
// a message type added later from reaching a handler that cannot answer it.
func TestEveryAcceptedMessageTypeHasACarryingOperation(t *testing.T) {
	accepted := []workerproto.MessageType{
		workerproto.MessageCapabilities,
		workerproto.MessageSnapshot,
		workerproto.MessageOffers,
		workerproto.MessageLeaseRenewals,
		workerproto.MessageCommands,
		workerproto.MessageThrottleCommands,
		workerproto.MessageArtifactDownload,
		workerproto.MessageArtifactUpload,
		workerproto.MessageArtifactPoll,
		workerproto.MessageArtifactAcknowledge,
		workerproto.MessageRepositoryProbe,
	}
	want := map[workerproto.MessageType]string{
		workerproto.MessageArtifactDownload: OperationArtifactReceive,
		workerproto.MessageArtifactUpload:   OperationArtifactSend,
	}
	for _, kind := range accepted {
		expected := want[kind]
		if expected == "" {
			expected = OperationControl
		}
		if got := OperationFor(kind); got != expected {
			t.Fatalf("OperationFor(%q) = %q, want %q", kind, got, expected)
		}
	}
}
