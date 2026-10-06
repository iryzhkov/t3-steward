package workerproto

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"
)

type bundleRawTransport struct {
	data     []byte
	metadata CommitBundleResponse
}

func (f bundleRawTransport) RoundTripWithRetry(context.Context, Envelope, RetryPolicy) (Envelope, error) {
	panic("must use raw transport")
}
func (f bundleRawTransport) RoundTripArtifactWithRetry(ctx context.Context, r Envelope, _ RetryPolicy, _ int64) (Envelope, []byte, error) {
	e, err := NewEnvelope(MessageCommitBundle, r.SessionID, r.RequestID, r.Sender, r.Recipient, r.CoordinatorEpoch, r.WorkerEpoch, r.Sequence, r.SentAt, r.Deadline, f.metadata)
	return e, f.data, err
}
func TestCommitBundleClientVerifiesRawPayload(t *testing.T) {
	data := []byte("bundle")
	sum := sha256.Sum256(data)
	good := CommitBundleResponse{Size: int64(len(data)), SHA256: fmt.Sprintf("%x", sum)}
	for _, metadata := range []CommitBundleResponse{good, {Size: good.Size + 1, SHA256: good.SHA256}, {Size: good.Size, SHA256: "wrong"}} {
		client := &Client{config: ClientConfig{CoordinatorID: "c", WorkerID: "w", CoordinatorEpoch: 1, WorkerEpoch: "e", SessionID: "s", RequestTimeout: time.Minute, SignerPrincipal: "c", SignerKeyID: "k", SignerSecret: []byte("1234567890123456"), RetryPolicy: RetryPolicy{MaxAttempts: 1}, Transport: bundleRawTransport{data: data, metadata: metadata}, Now: time.Now}}
		reader, err := client.FetchCommitBundle(context.Background(), CommitBundleRequest{Provenance: json.RawMessage("{}"), MaxBytes: 1024})
		if metadata == good {
			if err != nil {
				t.Fatal(err)
			}
			raw, err := io.ReadAll(reader)
			reader.Close()
			if err != nil || string(raw) != "bundle" {
				t.Fatal("wrong raw bytes")
			}
		} else if err == nil {
			reader.Close()
			t.Fatal("accepted corrupt payload")
		}
	}
}
