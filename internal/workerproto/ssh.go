package workerproto

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var sshTokenPattern = regexp.MustCompile(`^[A-Za-z0-9._@:/-]+$`)

type CommandFactory func(context.Context, string, ...string) *exec.Cmd

type SSHConfig struct {
	Address           string
	RemoteCommand     string
	RequestTimeout    time.Duration
	ConnectTimeout    time.Duration
	MaxMessageBytes   int64
	MaxStderrBytes    int64
	ResponsePrincipal string
	ResponseKeyID     string
	ResponseSecret    []byte
	Factory           CommandFactory
}

type SSHTransport struct {
	config SSHConfig
	codec  Codec
}

func NewSSHTransport(config SSHConfig) (*SSHTransport, error) {
	if !sshTokenPattern.MatchString(config.Address) || !sshTokenPattern.MatchString(config.RemoteCommand) {
		return nil, errors.New("ssh transport: safe address and remote command are required")
	}
	if config.RequestTimeout <= 0 || config.ConnectTimeout <= 0 || config.MaxMessageBytes <= 0 || config.MaxStderrBytes <= 0 {
		return nil, errors.New("ssh transport: positive timeouts and byte limits are required")
	}
	if config.ResponsePrincipal == "" || config.ResponseKeyID == "" || len(config.ResponseSecret) < 16 {
		return nil, errors.New("ssh transport: response authentication is required")
	}
	config.ResponseSecret = append([]byte(nil), config.ResponseSecret...)
	if config.Factory == nil {
		config.Factory = exec.CommandContext
	}
	return &SSHTransport{config: config, codec: Codec{MaxBytes: config.MaxMessageBytes}}, nil
}

func (t *SSHTransport) RoundTrip(ctx context.Context, request Envelope) (Envelope, error) {
	if t == nil {
		return Envelope{}, errors.New("ssh transport: transport is required")
	}
	var input bytes.Buffer
	if err := t.codec.Encode(&input, request); err != nil {
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

	connectSeconds := max(int(t.config.ConnectTimeout.Round(time.Second) / time.Second), 1)
	args := []string{
		"-oBatchMode=yes",
		"-oStrictHostKeyChecking=yes",
		"-oConnectTimeout=" + strconv.Itoa(connectSeconds),
		"--", t.config.Address, t.config.RemoteCommand,
	}
	command := t.config.Factory(requestCtx, "ssh", args...)
	command.Stdin = &input
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
		return Envelope{}, fmt.Errorf("ssh transport: remote exchange: %w", err)
	}
	var response Envelope
	if err := t.codec.Decode(bytes.NewReader(stdout.Bytes()), &response); err != nil {
		return Envelope{}, err
	}
	if response.InReplyTo != request.RequestID || response.SessionID != request.SessionID ||
		response.CoordinatorEpoch != request.CoordinatorEpoch || response.WorkerEpoch != request.WorkerEpoch ||
		response.Sender != request.Recipient || response.Recipient != request.Sender {
		return Envelope{}, &ProtocolError{Code: ErrorMalformed, Message: "response identity does not match request", RequestID: request.RequestID}
	}
	if response.Authentication.Principal != t.config.ResponsePrincipal ||
		response.Authentication.KeyID != t.config.ResponseKeyID {
		return Envelope{}, &ProtocolError{Code: ErrorAuthentication, Message: "response principal or key id does not match worker identity", RequestID: request.RequestID}
	}
	if err := VerifyEnvelopeSignature(response, t.config.ResponseSecret); err != nil {
		return Envelope{}, err
	}
	return response, nil
}

func (t *SSHTransport) RoundTripWithRetry(ctx context.Context, request Envelope, policy RetryPolicy) (Envelope, error) {
	if policy.MaxAttempts < 1 {
		return Envelope{}, errors.New("ssh transport: retry attempts must be positive")
	}
	var lastErr error
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		response, err := t.RoundTrip(ctx, request)
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

var errBoundedBuffer = errors.New("bounded buffer limit exceeded")

type boundedBuffer struct {
	buffer bytes.Buffer
	limit  int64
	err    error
}

func (b *boundedBuffer) Write(data []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	remaining := b.limit - int64(b.buffer.Len())
	if remaining <= 0 {
		b.err = errBoundedBuffer
		return 0, b.err
	}
	if int64(len(data)) > remaining {
		_, _ = b.buffer.Write(data[:remaining])
		b.err = errBoundedBuffer
		return int(remaining), b.err
	}
	return b.buffer.Write(data)
}

func (b *boundedBuffer) Bytes() []byte {
	return b.buffer.Bytes()
}

func (b *boundedBuffer) String() string {
	return b.buffer.String()
}

func ServeOne(reader io.Reader, writer io.Writer, codec Codec, server *Server, handler Handler) error {
	var request Envelope
	if err := codec.Decode(reader, &request); err != nil {
		return err
	}
	response, err := server.Handle(context.Background(), request, handler)
	if err != nil {
		return err
	}
	return codec.Encode(writer, response)
}
