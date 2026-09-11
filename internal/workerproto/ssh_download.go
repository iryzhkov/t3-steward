package workerproto

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// RoundTripArtifactDownloadWithRetry streams an immutable raw suffix after a
// signed request envelope. Every retry rewinds the same bounded source.
func (t *SSHTransport) RoundTripArtifactDownloadWithRetry(ctx context.Context, request Envelope, policy RetryPolicy, raw io.ReadSeeker, rawBytes int64) (Envelope, error) {
	if t == nil || raw == nil || rawBytes < 0 {
		return Envelope{}, errors.New("ssh artifact download transport: invalid raw source")
	}
	if policy.MaxAttempts < 1 {
		return Envelope{}, errors.New("ssh artifact download transport: retry attempts must be positive")
	}
	var lastErr error
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		if _, err := raw.Seek(0, io.SeekStart); err != nil {
			return Envelope{}, fmt.Errorf("ssh artifact download transport: rewind source: %w", err)
		}
		response, err := t.roundTripArtifactDownload(ctx, request, raw, rawBytes)
		if err == nil {
			return response, nil
		}
		lastErr = err
		var protocolErr *ProtocolError
		if errors.As(err, &protocolErr) && !protocolErr.Retryable {
			return Envelope{}, err
		}
		if attempt == policy.MaxAttempts {
			break
		}
		timer := time.NewTimer(policy.Delay(attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return Envelope{}, &ProtocolError{Code: ErrorCancelled, Message: ctx.Err().Error(), RequestID: request.RequestID}
		case <-timer.C:
		}
	}
	return Envelope{}, lastErr
}

func (t *SSHTransport) roundTripArtifactDownload(ctx context.Context, request Envelope, raw io.Reader, rawBytes int64) (Envelope, error) {
	var header bytes.Buffer
	if err := t.codec.Encode(&header, request); err != nil {
		return Envelope{}, err
	}
	timeout := t.config.RequestTimeout
	if !request.Deadline.IsZero() {
		untilDeadline := time.Until(request.Deadline)
		if untilDeadline <= 0 {
			return Envelope{}, &ProtocolError{Code: ErrorTimeout, Message: "request deadline expired", RequestID: request.RequestID}
		}
		if untilDeadline < timeout {
			timeout = untilDeadline
		}
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	connectSeconds := max(int(t.config.ConnectTimeout.Round(time.Second)/time.Second), 1)
	args := []string{"-oBatchMode=yes", "-oStrictHostKeyChecking=yes", "-oConnectTimeout=" + strconv.Itoa(connectSeconds), "--", t.config.Address, t.config.RemoteCommand}
	args = append(args, t.config.RemoteArguments...)
	command := t.config.Factory(requestCtx, "ssh", args...)
	limited := &io.LimitedReader{R: raw, N: rawBytes}
	command.Stdin = io.MultiReader(bytes.NewReader(header.Bytes()), limited)
	stdout := &boundedBuffer{limit: t.config.MaxMessageBytes}
	stderr := &boundedBuffer{limit: t.config.MaxStderrBytes}
	command.Stdout = stdout
	command.Stderr = stderr
	err := command.Run()
	if requestCtx.Err() != nil {
		code := ErrorCancelled
		if errors.Is(requestCtx.Err(), context.DeadlineExceeded) {
			code = ErrorTimeout
		}
		return Envelope{}, &ProtocolError{Code: code, Message: requestCtx.Err().Error(), Retryable: code == ErrorTimeout, RequestID: request.RequestID}
	}
	if errors.Is(stdout.err, errBoundedBuffer) || errors.Is(stderr.err, errBoundedBuffer) {
		return Envelope{}, &ProtocolError{Code: ErrorLimit, Message: "SSH response exceeded byte limit", RequestID: request.RequestID}
	}
	if err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail != "" {
			err = fmt.Errorf("%w: %s", err, detail)
		}
		return Envelope{}, fmt.Errorf("ssh artifact download transport: remote exchange: %w", err)
	}
	if limited.N != 0 {
		return Envelope{}, errors.New("ssh artifact download transport: remote command did not consume complete payload")
	}
	var response Envelope
	if err := t.codec.Decode(bytes.NewReader(stdout.Bytes()), &response); err != nil {
		return Envelope{}, err
	}
	if err := t.validateResponse(request, response); err != nil {
		return Envelope{}, err
	}
	return response, nil
}
