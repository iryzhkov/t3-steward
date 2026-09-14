package backlogadmin

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// RemoteServerConfig is what the restricted SSH command needs to answer one
// exchange. Every identity in it comes from the coordinator's own validated
// configuration; nothing is taken from the SSH invocation or its environment.
type RemoteServerConfig struct {
	CoordinatorID string
	// Clients are the admin clients this coordinator accepts, by principal.
	// A frame naming a principal that is not here cannot be authenticated,
	// because there is no credential to verify it against.
	Clients            map[string]AdminCredentials
	Relay              LocalClient
	Replay             *RemoteReplayStore
	MaxRequestBytes    int64
	MaxArtifactBytes   int64
	MaxSubmissionBytes int64
	MaxClockSkew       time.Duration
	Now                func() time.Time
}

// RemoteServer answers one signed coordinator-admin exchange per process.
type RemoteServer struct {
	config RemoteServerConfig
}

func NewRemoteServer(config RemoteServerConfig) (*RemoteServer, error) {
	if config.CoordinatorID == "" || len(config.Clients) == 0 {
		return nil, errors.New("coordinator-exchange: a coordinator id and at least one admin client are required")
	}
	for principal, credentials := range config.Clients {
		if principal == "" || !credentials.Complete() {
			return nil, fmt.Errorf("coordinator-exchange: admin client %q has incomplete credentials", principal)
		}
		if credentials.ClientPrincipal != principal {
			return nil, fmt.Errorf("coordinator-exchange: admin client %q resolves to principal %q", principal, credentials.ClientPrincipal)
		}
	}
	if config.MaxRequestBytes <= 0 || config.MaxArtifactBytes <= 0 || config.MaxSubmissionBytes <= 0 {
		return nil, errors.New("coordinator-exchange: byte limits must be positive")
	}
	if config.MaxClockSkew <= 0 {
		config.MaxClockSkew = time.Minute
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &RemoteServer{config: config}, nil
}

// Serve reads one request frame, answers it, and writes one response frame.
// For an artifact read the raw content follows the response frame, exactly as
// it does on the local carrier.
//
// A refusal is reported to the remote client as a signed error frame rather
// than as a process failure, so the client can classify it. Only a failure
// that cannot be attributed to an authenticated request is returned as an
// error, because then there is nobody to sign an answer for.
func (s *RemoteServer) Serve(ctx context.Context, operation string, in io.Reader, out io.Writer) error {
	if !ValidOperation(operation) {
		return fmt.Errorf("coordinator-exchange: unknown operation %q", operation)
	}
	buffered := bufio.NewReader(in)
	frame, err := readRemoteFrame(buffered, s.config.MaxRequestBytes)
	if err != nil {
		return err
	}
	request, credentials, protocolErr := s.validate(operation, frame)
	if protocolErr != nil {
		return s.refuse(operation, protocolErr)
	}
	if s.config.Replay == nil || !mutatingOperation(operation) {
		return s.relay(ctx, frame, operation, request, credentials, buffered, out)
	}
	digest, err := frameDigest(frame)
	if err != nil {
		return s.refuse(operation, &workerproto.ProtocolError{Code: workerproto.ErrorMalformed, Message: err.Error(), RequestID: frame.RequestID})
	}
	transaction, err := s.config.Replay.Begin(credentials.ClientPrincipal, frame.RequestID, digest)
	if err != nil {
		var protocolErr *workerproto.ProtocolError
		if errors.As(err, &protocolErr) {
			return s.refuse(operation, protocolErr)
		}
		return err
	}
	defer transaction.Release()
	if cached, ok := transaction.Cached(); ok {
		// The first answer is authoritative. Repeating the request id with the
		// same content must never create a second effect.
		if _, err := out.Write(append(cached, '\n')); err != nil {
			return err
		}
		return nil
	}
	response, _, err := s.answer(ctx, frame, operation, request, credentials, buffered)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return err
	}
	if err := transaction.Complete(encoded); err != nil {
		return err
	}
	_, err = out.Write(append(encoded, '\n'))
	return err
}

// mutatingOperation reports whether repeating an operation could repeat an
// external effect. Reads are excluded: they carry no effect to deduplicate,
// and an artifact read streams bytes that no cache could replay.
func mutatingOperation(operation string) bool {
	switch operation {
	case localOperationQuery, localOperationArtifact:
		return false
	default:
		return true
	}
}

// validate checks the frame's authority and integrity before any local effect.
func (s *RemoteServer) validate(operation string, frame remoteFrame) (localRequest, AdminCredentials, *workerproto.ProtocolError) {
	var request localRequest
	var credentials AdminCredentials
	if frame.Version != RemoteTransportVersion {
		return request, credentials, &workerproto.ProtocolError{Code: workerproto.ErrorUnsupportedVersion, Message: "unsupported admin frame version", RequestID: frame.RequestID}
	}
	if frame.SessionID == "" || frame.RequestID == "" || frame.Sequence < 1 {
		return request, credentials, &workerproto.ProtocolError{Code: workerproto.ErrorMalformed, Message: "session, request and positive sequence are required", RequestID: frame.RequestID}
	}
	if frame.Operation != operation {
		return request, credentials, &workerproto.ProtocolError{Code: workerproto.ErrorMalformed, Message: "frame operation does not match the invoked operation", RequestID: frame.RequestID}
	}
	if frame.Recipient != s.config.CoordinatorID {
		return request, credentials, &workerproto.ProtocolError{Code: workerproto.ErrorAuthorization, Message: "recipient is not this coordinator", RequestID: frame.RequestID}
	}
	// The claimed principal selects which configured credential to check
	// against, exactly as a key id would. It proves nothing until the
	// signature over that credential's secret verifies.
	credentials, known := s.config.Clients[frame.Sender]
	if !known || frame.Authentication.Principal != credentials.ClientPrincipal ||
		frame.Authentication.KeyID != credentials.ClientKeyID {
		return request, AdminCredentials{}, &workerproto.ProtocolError{Code: workerproto.ErrorAuthentication, Message: "principal or key id does not match a configured admin client", RequestID: frame.RequestID}
	}
	now := s.config.Now()
	if frame.Deadline.IsZero() || !now.Before(frame.Deadline) {
		return request, credentials, &workerproto.ProtocolError{Code: workerproto.ErrorTimeout, Message: "request deadline expired", RequestID: frame.RequestID}
	}
	if frame.SentAt.IsZero() || frame.SentAt.Before(now.Add(-s.config.MaxClockSkew)) || frame.SentAt.After(now.Add(s.config.MaxClockSkew)) {
		return request, credentials, &workerproto.ProtocolError{Code: workerproto.ErrorAuthentication, Message: "request timestamp is outside the allowed skew", RequestID: frame.RequestID}
	}
	if err := verifyRemoteFrame(frame, credentials.ClientSecret); err != nil {
		var protocolErr *workerproto.ProtocolError
		if errors.As(err, &protocolErr) {
			return request, credentials, protocolErr
		}
		return request, credentials, &workerproto.ProtocolError{Code: workerproto.ErrorAuthentication, Message: err.Error(), RequestID: frame.RequestID}
	}
	decoder := json.NewDecoder(bytes.NewReader(frame.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return request, credentials, &workerproto.ProtocolError{Code: workerproto.ErrorMalformed, Message: "invalid operation envelope: " + err.Error(), RequestID: frame.RequestID}
	}
	if request.Version != LocalTransportVersion || request.Operation != operation {
		return request, credentials, &workerproto.ProtocolError{Code: workerproto.ErrorMalformed, Message: "operation envelope does not match the frame", RequestID: frame.RequestID}
	}
	// The remote client does not get to say who it is. Its claimed principal
	// is discarded and replaced by the identity the signature proved.
	if request.RemoteAdmin != nil {
		return request, credentials, &workerproto.ProtocolError{Code: workerproto.ErrorAuthorization, Message: "a remote client may not assert its own principal", RequestID: frame.RequestID}
	}
	if request.Query != nil {
		request.Query.Principal = Principal{}
	}
	if request.Mutation != nil {
		request.Mutation.Principal = Principal{}
	}
	if request.Operation == localOperationSubmission &&
		(request.SubmissionSize <= 0 || request.SubmissionSize > s.config.MaxSubmissionBytes) {
		return request, credentials, &workerproto.ProtocolError{
			Code: workerproto.ErrorLimit,
			Message: fmt.Sprintf("submission archive of %d bytes exceeds this coordinator's limit of %d bytes",
				request.SubmissionSize, s.config.MaxSubmissionBytes),
			RequestID: frame.RequestID,
		}
	}
	request.RemoteAdmin = &RemoteAdminAssertion{
		Principal:   credentials.ClientPrincipal,
		Coordinator: s.config.CoordinatorID,
		RequestID:   frame.RequestID,
	}
	return request, credentials, nil
}

// answer relays the verified request to the coordinator over its owner-only
// socket and signs whatever came back.
func (s *RemoteServer) answer(ctx context.Context, frame remoteFrame, operation string, request localRequest, credentials AdminCredentials, body io.Reader) (remoteFrame, io.ReadCloser, error) {
	response, stream, err := s.config.Relay.exchange(ctx, request, body)
	if err != nil {
		class := ClassOf(err)
		if class == "" {
			class = ClassRejected
		}
		response = localResponse{Version: LocalTransportVersion, Error: err.Error(), ErrorClass: class}
	}
	signed, signErr := s.sign(frame, operation, response, credentials)
	if signErr != nil {
		if stream != nil {
			_ = stream.Close()
		}
		return remoteFrame{}, nil, signErr
	}
	return signed, stream, nil
}

// relay answers an operation that is not replay-protected, streaming artifact
// content after the response frame.
func (s *RemoteServer) relay(ctx context.Context, frame remoteFrame, operation string, request localRequest, credentials AdminCredentials, body io.Reader, out io.Writer) error {
	response, stream, err := s.answer(ctx, frame, operation, request, credentials, body)
	if err != nil {
		return err
	}
	if stream != nil {
		defer stream.Close()
	}
	if err := writeRemoteFrame(out, response, s.config.MaxRequestBytes); err != nil {
		return err
	}
	if stream == nil {
		return nil
	}
	var size int64
	var decoded localResponse
	if err := json.Unmarshal(response.Payload, &decoded); err == nil {
		size = decoded.ArtifactSize
	}
	_, err = io.CopyN(out, stream, size)
	return err
}

func (s *RemoteServer) sign(request remoteFrame, operation string, response localResponse, credentials AdminCredentials) (remoteFrame, error) {
	frame, err := newRemoteFrame(operation, request.SessionID, "response-"+request.RequestID,
		s.config.CoordinatorID, request.Sender, request.Sequence, s.config.Now(), request.Deadline, response)
	if err != nil {
		return remoteFrame{}, err
	}
	frame.InReplyTo = request.RequestID
	if err := signRemoteFrame(&frame, credentials.CoordinatorPrincipal,
		credentials.CoordinatorKeyID, credentials.CoordinatorSecret); err != nil {
		return remoteFrame{}, err
	}
	return frame, nil
}

// refuse answers an unauthenticated or malformed request. When the frame
// itself could not be attributed the refusal goes back as a process error,
// because signing an answer for an unknown peer would be a lie.
func (s *RemoteServer) refuse(operation string, protocolErr *workerproto.ProtocolError) error {
	return fmt.Errorf("coordinator-exchange refused %s: %w", operation, protocolErr)
}
