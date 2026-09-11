package workerproto

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

type artifactPushClientTransport struct {
	push func(context.Context, Envelope, []byte, RetryPolicy) (Envelope, error)
}

func (t artifactPushClientTransport) RoundTripWithRetry(context.Context, Envelope, RetryPolicy) (Envelope, error) {
	return Envelope{}, errors.New("unexpected control exchange")
}

func (t artifactPushClientTransport) RoundTripArtifactPushWithRetry(ctx context.Context, request Envelope, raw []byte, policy RetryPolicy) (Envelope, error) {
	return t.push(ctx, request, raw, policy)
}

func TestClientSendsVerifiedArtifactAndRequiresCustodyReceipt(t *testing.T) {
	now := time.Date(2026, 9, 11, 7, 0, 0, 0, time.UTC)
	content := []byte("queued prompt")
	object := artifactObject("prompt-1", "prompt/task.md", content)
	object.Kind = "prompt"
	manifest := ArtifactTransferManifest{
		Version: ArtifactManifestVersion, ID: "download-assignment-1-inputs", Direction: "download",
		CoordinatorEpoch: 9, WorkerID: "normandy", WorkerEpoch: "worker-1",
		AssignmentID: "assignment-1", AssignmentEpoch: 1, Objects: []ArtifactObject{object},
		TotalBytes: int64(len(content)), CreatedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour),
	}
	transport := artifactPushClientTransport{push: func(_ context.Context, request Envelope, raw []byte, policy RetryPolicy) (Envelope, error) {
		if policy.MaxAttempts != 3 || !bytes.Equal(raw, content) {
			t.Fatalf("push policy=%#v raw=%q", policy, raw)
		}
		var transferred ArtifactTransferManifest
		if err := DecodePayload(request, MessageArtifactDownload, &transferred); err != nil {
			t.Fatal(err)
		}
		custody, err := BuildCustodyRecord(ArtifactCustodyRecord{
			ManifestID: manifest.ID, ObjectID: object.ID,
			From: "coordinator:coordinator", To: "worker:normandy", Sequence: 1,
			Size: object.Size, SHA256: object.SHA256, VerifiedAt: now,
		})
		if err != nil {
			t.Fatal(err)
		}
		return clientResponse(t, request, MessageArtifactDownload, ArtifactDownloadReceipt{
			ManifestID: transferred.ID, Custody: []ArtifactCustodyRecord{custody},
		})
	}}
	client := newProtocolClient(t, now, transport)
	receipt, err := client.SendArtifact(context.Background(), manifest, content, 1024, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ManifestID != manifest.ID || len(receipt.Custody) != 1 {
		t.Fatalf("receipt = %#v", receipt)
	}

	bad := append([]byte(nil), content...)
	bad[0] ^= 1
	if _, err := client.SendArtifact(context.Background(), manifest, bad, 1024, 1024); err == nil {
		t.Fatal("changed bytes were accepted")
	}
}
