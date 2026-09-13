package workerproto

import (
	"context"
	"errors"
	"io"
	"time"
)

func (t *StreamTransport) RoundTripArtifactWithRetry(ctx context.Context, request Envelope, policy RetryPolicy, maxRawBytes int64) (Envelope, []byte, error) {
	if maxRawBytes < 0 {
		return Envelope{}, nil, errors.New("invalid artifact response limit")
	}
	return t.retryStream(ctx, request, policy, nil, min(maxRawBytes, StreamArtifactLimit))
}
func (t *StreamTransport) RoundTripArtifactDownloadWithRetry(ctx context.Context, request Envelope, policy RetryPolicy, raw io.ReadSeeker, rawBytes int64) (Envelope, error) {
	if raw == nil || rawBytes < 0 || rawBytes > StreamArtifactLimit {
		return Envelope{}, errors.New("stream artifact exceeds 16 MiB transfer limit")
	}
	if _, err := raw.Seek(0, io.SeekStart); err != nil {
		return Envelope{}, err
	}
	suffix, err := io.ReadAll(io.LimitReader(raw, rawBytes+1))
	if err != nil || int64(len(suffix)) != rawBytes {
		return Envelope{}, errors.New("stream artifact length mismatch")
	}
	reply, _, err := t.retryStream(ctx, request, policy, suffix, 0)
	return reply, err
}
func (t *StreamTransport) retryStream(ctx context.Context, request Envelope, policy RetryPolicy, suffix []byte, limit int64) (Envelope, []byte, error) {
	if policy.MaxAttempts < 1 {
		return Envelope{}, nil, errors.New("stream retry attempts must be positive")
	}
	var last error
	for i := 1; i <= policy.MaxAttempts; i++ {
		reply, raw, err := t.roundTrip(ctx, request, suffix, limit)
		if err == nil {
			return reply, raw, nil
		}
		last = err
		var p *ProtocolError
		if errors.As(err, &p) && !p.Retryable {
			return Envelope{}, nil, err
		}
		if i == policy.MaxAttempts {
			break
		}
		timer := time.NewTimer(policy.Delay(i))
		select {
		case <-ctx.Done():
			timer.Stop()
			return Envelope{}, nil, ctx.Err()
		case <-timer.C:
		}
	}
	return Envelope{}, nil, last
}
