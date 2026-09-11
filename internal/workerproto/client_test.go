package workerproto

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type clientRoundTripFunc func(context.Context, Envelope, RetryPolicy) (Envelope, error)

func (f clientRoundTripFunc) RoundTripWithRetry(ctx context.Context, request Envelope, policy RetryPolicy) (Envelope, error) {
	return f(ctx, request, policy)
}

type artifactClientTransport struct {
	fetch func(context.Context, Envelope, RetryPolicy, int64) (Envelope, []byte, error)
}

func (t artifactClientTransport) RoundTripWithRetry(context.Context, Envelope, RetryPolicy) (Envelope, error) {
	return Envelope{}, errors.New("unexpected control exchange")
}

func (t artifactClientTransport) RoundTripArtifactWithRetry(ctx context.Context, request Envelope, policy RetryPolicy, limit int64) (Envelope, []byte, error) {
	return t.fetch(ctx, request, policy, limit)
}

func TestClientSequencesSignedTypedExchanges(t *testing.T) {
	now := time.Date(2026, 9, 10, 21, 0, 0, 0, time.UTC)
	var requests []Envelope
	transport := clientRoundTripFunc(func(_ context.Context, request Envelope, policy RetryPolicy) (Envelope, error) {
		requests = append(requests, request)
		if policy.MaxAttempts != 3 {
			t.Fatalf("retry policy = %+v", policy)
		}
		if err := VerifyEnvelopeSignature(request, []byte("coordinator-secret")); err != nil {
			t.Fatal(err)
		}
		switch request.Type {
		case MessageSnapshot:
			return clientResponse(t, request, MessageObservations, Observations{Snapshot: domain.WorkerSnapshot{
				WorkerID: "normandy", WorkerEpoch: "worker-1", CoordinatorEpoch: 9, Sequence: 4,
			}})
		case MessageCommands:
			var delivery CommandDelivery
			if err := DecodePayload(request, MessageCommands, &delivery); err != nil {
				t.Fatal(err)
			}
			return clientResponse(t, request, MessageAcknowledgements, Acknowledgements{
				Acknowledgements: []domain.WorkerAcknowledgement{{CommandID: delivery.Commands[0].ID}},
			})
		case MessageArtifactPoll:
			return clientResponse(t, request, MessageArtifactAnnouncement, ArtifactAnnouncement{Upload: &ArtifactUploadResponse{Manifest: ArtifactTransferManifest{ID: "upload-1"}}})
		case MessageArtifactAcknowledge:
			var acknowledgement ArtifactAcknowledgeRequest
			if err := DecodePayload(request, MessageArtifactAcknowledge, &acknowledgement); err != nil {
				t.Fatal(err)
			}
			return clientResponse(t, request, MessageArtifactAcknowledged, ArtifactAcknowledgement{ManifestID: acknowledgement.ManifestID})
		default:
			t.Fatalf("request type = %q", request.Type)
			return Envelope{}, nil
		}
	})
	client := newProtocolClient(t, now, transport)
	snapshot, err := client.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	acks, err := client.DeliverWorkerCommands(context.Background(), snapshot, []domain.WorkerCommand{{
		ID: "command-1", WorkerID: "normandy", WorkerEpoch: "worker-1", CoordinatorEpoch: 9,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(acks) != 1 || acks[0].CommandID != "command-1" {
		t.Fatalf("acknowledgements = %+v", acks)
	}
	upload, err := client.PollArtifact(context.Background(), "result")
	if err != nil || upload == nil || upload.Manifest.ID != "upload-1" {
		t.Fatalf("artifact poll = %#v, %v", upload, err)
	}
	if err := client.AcknowledgeArtifact(context.Background(), upload.Manifest.ID); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 4 || requests[0].Sequence != 1 || requests[0].RequestID != "cycle-1-1" ||
		requests[1].Sequence != 2 || requests[1].RequestID != "cycle-1-2" ||
		requests[2].Sequence != 3 || requests[3].Sequence != 4 {
		t.Fatalf("requests = %+v", requests)
	}
	if !requests[0].Deadline.Equal(now.Add(time.Minute)) {
		t.Fatalf("deadline = %v", requests[0].Deadline)
	}
}

func TestClientPoisonsAmbiguousSessionAndRejectsWrongSnapshot(t *testing.T) {
	now := time.Date(2026, 9, 10, 21, 0, 0, 0, time.UTC)
	calls := 0
	client := newProtocolClient(t, now, clientRoundTripFunc(func(context.Context, Envelope, RetryPolicy) (Envelope, error) {
		calls++
		return Envelope{}, errors.New("lost response")
	}))
	if _, err := client.Snapshot(context.Background()); err == nil {
		t.Fatal("ambiguous exchange succeeded")
	}
	if _, err := client.Snapshot(context.Background()); err == nil {
		t.Fatal("poisoned session was reused")
	}
	if calls != 1 {
		t.Fatalf("transport calls = %d", calls)
	}

	fresh := newProtocolClient(t, now, clientRoundTripFunc(func(context.Context, Envelope, RetryPolicy) (Envelope, error) {
		t.Fatal("wrong snapshot reached transport")
		return Envelope{}, nil
	}))
	_, err := fresh.DeliverWorkerCommands(context.Background(), domain.WorkerSnapshot{
		WorkerID: "other", WorkerEpoch: "worker-1", CoordinatorEpoch: 9,
	}, nil)
	if err == nil {
		t.Fatal("wrong snapshot accepted")
	}
}

func TestClientReturnsStructuredRemoteError(t *testing.T) {
	now := time.Date(2026, 9, 10, 21, 0, 0, 0, time.UTC)
	client := newProtocolClient(t, now, clientRoundTripFunc(func(_ context.Context, request Envelope, _ RetryPolicy) (Envelope, error) {
		return clientResponse(t, request, MessageError, ProtocolError{
			Code: ErrorStaleEpoch, Message: "worker epoch changed", RequestID: request.RequestID,
		})
	}))
	_, err := client.DeliverOffers(context.Background(), nil)
	var protocolErr *ProtocolError
	if !errors.As(err, &protocolErr) || protocolErr.Code != ErrorStaleEpoch {
		t.Fatalf("error = %v", err)
	}
}

func TestClientFetchesExactAnnouncedArtifactStream(t *testing.T) {
	now := time.Date(2026, 9, 10, 21, 0, 0, 0, time.UTC)
	content := []byte("result")
	object := artifactObject("result-1", "results/result.txt", content)
	object.Kind = "output"
	manifest := ArtifactTransferManifest{
		Version: 1, ID: "upload-assignment-1-result", Direction: "upload",
		CoordinatorEpoch: 9, WorkerID: "normandy", WorkerEpoch: "worker-1",
		AssignmentID: "assignment-1", AssignmentEpoch: 1, Objects: []ArtifactObject{object},
		TotalBytes: int64(len(content)), CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	announced := ArtifactUploadResponse{Manifest: manifest}
	transport := artifactClientTransport{fetch: func(_ context.Context, request Envelope, policy RetryPolicy, limit int64) (Envelope, []byte, error) {
		if policy.MaxAttempts != 3 || limit != 1024 {
			t.Fatalf("fetch policy=%#v limit=%d", policy, limit)
		}
		var download ArtifactDownloadRequest
		if err := DecodePayload(request, MessageArtifactUpload, &download); err != nil {
			t.Fatal(err)
		}
		if download.ManifestID != manifest.ID || len(download.ObjectIDs) != 1 || download.ObjectIDs[0] != object.ID {
			t.Fatalf("download request = %#v", download)
		}
		response, err := clientResponse(t, request, MessageArtifactUpload, announced)
		return response, content, err
	}}
	client := newProtocolClient(t, now, transport)
	fetched, err := client.FetchArtifact(context.Background(), announced, 1024, 1024)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := fetched.OpenWorkerUpload(context.Background(), object)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("fetched content = %q, %v", got, err)
	}

	changed := announced
	changed.Manifest.Objects[0].SHA256 = strings.Repeat("0", 64)
	if _, err := client.FetchArtifact(context.Background(), changed, 1024, 1024); err == nil {
		t.Fatal("changed announcement was accepted")
	}
}

func newProtocolClient(t *testing.T, now time.Time, transport RoundTripper) *Client {
	t.Helper()
	client, err := NewClient(ClientConfig{
		CoordinatorID: "coordinator", WorkerID: "normandy", CoordinatorEpoch: 9, WorkerEpoch: "worker-1",
		SessionID: "cycle-1", RequestTimeout: time.Minute,
		SignerPrincipal: "ssh:coordinator", SignerKeyID: "coordinator-key", SignerSecret: []byte("coordinator-secret"),
		RetryPolicy: RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Second},
		Transport:   transport, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func clientResponse(t *testing.T, request Envelope, kind MessageType, payload any) (Envelope, error) {
	t.Helper()
	response, err := NewEnvelope(
		kind, request.SessionID, "response-"+request.RequestID,
		request.Recipient, request.Sender, request.CoordinatorEpoch, request.WorkerEpoch,
		request.Sequence, request.SentAt, request.Deadline, payload,
	)
	if err != nil {
		t.Fatal(err)
	}
	response.InReplyTo = request.RequestID
	return response, nil
}
