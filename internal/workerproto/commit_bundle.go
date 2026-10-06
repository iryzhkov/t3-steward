package workerproto

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const MessageCommitBundle MessageType = "commit-bundle-export"
const MaxCommitBundleBytes int64 = 64 << 20

type CommitBundleRequest struct {
	Provenance json.RawMessage `json:"provenance"`
	MaxBytes   int64           `json:"maxBytes"`
}
type CommitBundleResponse struct {
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

func ValidateCommitBundleRequest(r CommitBundleRequest) error {
	if len(r.Provenance) == 0 || len(r.Provenance) > 65536 || !json.Valid(r.Provenance) || r.MaxBytes < 1 || r.MaxBytes > MaxCommitBundleBytes {
		return errors.New("worker commit export: invalid bounded request")
	}
	return nil
}

// FetchCommitBundle uses the existing authenticated raw artifact transport.
func (c *Client) FetchCommitBundle(ctx context.Context, r CommitBundleRequest) (io.ReadCloser, error) {
	if err := ValidateCommitBundleRequest(r); err != nil {
		return nil, err
	}
	transport, ok := c.config.Transport.(ArtifactRoundTripper)
	if !ok {
		return nil, errors.New("worker commit export: raw transport unavailable")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.unusable {
		return nil, errors.New("worker commit export: session unusable")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.sequence++
	now := c.config.Now().UTC()
	id := c.config.SessionID + "-" + strconv.FormatInt(c.sequence, 10)
	request, err := NewEnvelope(MessageCommitBundle, c.config.SessionID, id, c.config.CoordinatorID, c.config.WorkerID, c.config.CoordinatorEpoch, c.config.WorkerEpoch, c.sequence, now, now.Add(c.config.RequestTimeout), r)
	if err != nil {
		return nil, err
	}
	if err := SignEnvelope(&request, c.config.SignerPrincipal, c.config.SignerKeyID, c.config.SignerSecret); err != nil {
		return nil, err
	}
	envelope, raw, err := transport.RoundTripArtifactWithRetry(ctx, request, c.config.RetryPolicy, r.MaxBytes)
	if err != nil {
		c.unusable = true
		return nil, fmt.Errorf("worker commit export: %w", err)
	}
	if envelope.Type == MessageError {
		var e ProtocolError
		if err := DecodePayload(envelope, MessageError, &e); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("worker commit export unavailable (worker may require upgrade): %w", &e)
	}
	var response CommitBundleResponse
	if err := DecodePayload(envelope, MessageCommitBundle, &response); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw)
	if response.Size < 1 || response.Size > r.MaxBytes || response.Size != int64(len(raw)) || response.SHA256 != fmt.Sprintf("%x", sum) {
		return nil, errors.New("worker commit export: payload identity mismatch")
	}
	return io.NopCloser(bytes.NewReader(raw)), nil
}
