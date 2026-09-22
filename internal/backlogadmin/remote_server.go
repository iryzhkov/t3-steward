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

// authenticationFailed is the one message every pre-verification identity
// failure produces. Distinguishing "no such principal" from "bad signature"
// would answer, for anyone holding the SSH key, the question "is this principal
// configured here", which is a principal-enumeration oracle. The coordinator's
// own log is where an operator learns which of the two it was.
const authenticationFailed = "admin frame authentication failed"

// RemoteServerConfig is what the restricted SSH command needs to answer one
// exchange. Every identity in it comes from the coordinator's own validated
// configuration; nothing is taken from the SSH invocation or its environment.
type RemoteServerConfig struct {
	CoordinatorID string
	// Clients are the admin clients this coordinator accepts, by principal.
	// A frame naming a principal that is not here cannot be authenticated,
	// because there is no credential to verify it against.
	Clients map[string]AdminCredentials
	// Supervisors are the admin clients that act as campaign overseers. A
	// principal named here is relayed under the supervisor role instead of
	// remote-admin, which is strictly narrower: bound to one run and one
	// activation epoch by the coordinator's own authorizer. An empty map is
	// the ordinary deployment, in which no client is a supervisor.
	Supervisors map[string]bool
	// Approvers are distinct signed clients whose original decision frame is
	// reverified by the coordinator process before any approver role is derived.
	Approvers          map[string]bool
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
	for principal := range config.Approvers {
		if config.Supervisors[principal] {
			return nil, fmt.Errorf("coordinator-exchange: admin client %q cannot be both supervisor and approver", principal)
		}
		if _, ok := config.Clients[principal]; !ok {
			return nil, fmt.Errorf("coordinator-exchange: approver %q is not a configured client", principal)
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
// Every outcome is reported to the remote client as a frame carrying its exact
// failure class, never as bare process failure, because an agent branches on
// that class: a rotated credential must read as permanent authentication and
// not as a transient protocol fault. A refusal after the signature verified is
// signed; a refusal before it cannot be, and the client accepts an unsigned
// frame only as a classification and never as an answer.
// pinned is the operation word the forced command fixed, or empty when it fixed
// none. An empty pin means the operation is taken from the verified frame.
//
// Reading it from the frame is not a relaxation. The operation sits inside the
// signed payload digest, so it is evidence the client's own credential vouched
// for, where a word on a command line arrives over a channel nobody signed.
// What an operator gives up by leaving the pin off is key-level narrowing; the
// narrowing that decides authority is enforced separately by the coordinator's
// authorizer, by role and by command kind, on every carrier.
//
// It exists because one coordinator client declares one ssh destination and one
// identity, so against a pinned forced command a client could reach exactly one
// operation, and "campaign submit" needs two: the readiness query it runs first
// and the submission itself.
func (s *RemoteServer) Serve(ctx context.Context, pinned string, in io.Reader, out io.Writer) error {
	if pinned != "" && !ValidOperation(pinned) {
		return fmt.Errorf("coordinator-exchange: unknown operation %q", pinned)
	}
	buffered := bufio.NewReader(in)
	frame, err := readRemoteFrame(buffered, s.config.MaxRequestBytes)
	if err != nil {
		// A frame that could not even be read has no request identity to echo,
		// so the refusal carries the class alone.
		return s.refuse(out, remoteFrame{Operation: pinned}, pinned, nil, asProtocolError(err))
	}
	request, credentials, verified, protocolErr := s.validate(pinned, frame)
	// From here on the operation is the frame's, which validate has checked is
	// a known one and, when a pin was given, equal to it.
	operation := frame.Operation
	if protocolErr != nil {
		if verified {
			return s.refuse(out, frame, operation, &credentials, protocolErr)
		}
		return s.refuse(out, frame, operation, nil, protocolErr)
	}
	if s.config.Replay == nil || !mutatingRequest(operation, request) {
		return s.relay(ctx, frame, operation, request, credentials, buffered, out)
	}
	digest, err := frameDigest(frame)
	if err != nil {
		return s.refuse(out, frame, operation, &credentials,
			&workerproto.ProtocolError{Code: workerproto.ErrorMalformed, Message: err.Error(), RequestID: frame.RequestID})
	}
	transaction, err := s.config.Replay.Begin(credentials.ClientPrincipal, frame.RequestID, digest)
	if err != nil {
		var replayErr *workerproto.ProtocolError
		if errors.As(err, &replayErr) {
			return s.refuse(out, frame, operation, &credentials, replayErr)
		}
		return err
	}
	defer transaction.Release()
	if cached, ok := transaction.Cached(); ok {
		// The first answer is authoritative: repeating the request id with the
		// same content must never create a second effect. What is cached is the
		// answer itself, not the frame that carried it, because a retry is a
		// new CLI process with a new session id and would reject a frame bound
		// to the session of the request that was lost.
		response, err := decodeCachedResponse(cached)
		if err != nil {
			return s.refuse(out, frame, operation, &credentials,
				&workerproto.ProtocolError{Code: workerproto.ErrorInternal, Message: err.Error(), RequestID: frame.RequestID})
		}
		return s.write(out, frame, operation, markReplayedAnswer(response), credentials)
	}
	response := s.respond(ctx, request, buffered, nil)
	encoded, err := json.Marshal(response)
	if err != nil {
		return err
	}
	// The effect has already happened; caching it is what lets a lost answer
	// be recovered. A crash between the two re-executes the operation, and the
	// service's own idempotency key is what makes that safe.
	if err := transaction.Complete(encoded); err != nil {
		return err
	}
	return s.write(out, frame, operation, response, credentials)
}

// markReplayedAnswer says on the answer itself that it came from the carrier's
// cache: this request identity was answered before, so the work it asked for
// happened then and not now. That is exactly what the printed "replayed" flag
// means, and without this the flag is wrong on the path the fleet actually
// uses. A "t3-steward task run" repeated with the same inputs derives the same
// idempotency key, requestIdentity turns that key into the same request id,
// and the carrier therefore answers from this cache. The submission service,
// which does set Replay on a repeat, is never asked, so the first answer's
// "replay": false was returned verbatim.
//
// The five answers named below are every one this carrier can return that
// carries such a flag: a submission, a schedule definition, a graph amendment,
// a supervision decision and an unknown-assignment recovery. A mutation answer
// and a quarantine release have no flag to set, and a quarantine release has no
// stable request id either, so neither is ever served from this cache.
//
// Nothing here is inferred. The carrier knows it served a cached answer; it
// does not conclude "replay" from a run id that happens to match. When the
// cached row has been pruned the operation re-executes and the service sets
// the same flag from its own durable record, so the two layers agree instead
// of contradicting each other, and an answer that carries no replay flag at
// all, such as a refusal, is left alone.
func markReplayedAnswer(response localResponse) localResponse {
	if response.SubmissionResponse != nil {
		replayed := *response.SubmissionResponse
		replayed.Replay = true
		response.SubmissionResponse = &replayed
	}
	if response.ScheduleDefinitionResponse != nil {
		replayed := *response.ScheduleDefinitionResponse
		replayed.Replay = true
		response.ScheduleDefinitionResponse = &replayed
	}
	if response.GraphAmendment != nil {
		replayed := *response.GraphAmendment
		replayed.Replay = true
		response.GraphAmendment = &replayed
	}
	if response.SupervisionResponse != nil {
		replayed := *response.SupervisionResponse
		replayed.Replay = true
		response.SupervisionResponse = &replayed
	}
	if response.UnknownRecoveryResponse != nil {
		// "backlog recover" derives its request id from the recovery ID, so a
		// repeat reaches this cache as the same request and is answered from it.
		// Without this line the answer said "applied" while applying nothing.
		replayed := *response.UnknownRecoveryResponse
		replayed.Replay = true
		response.UnknownRecoveryResponse = &replayed
	}
	return response
}

// decodeCachedResponse reads back a cached answer strictly, so a corrupted row
// is refused rather than replayed as something else.
func decodeCachedResponse(raw []byte) (localResponse, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var response localResponse
	if err := decoder.Decode(&response); err != nil {
		return localResponse{}, fmt.Errorf("decode cached admin response: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return localResponse{}, errors.New("cached admin response has trailing content")
	}
	if response.Version != LocalTransportVersion {
		return localResponse{}, errors.New("cached admin response has an unexpected version")
	}
	return response, nil
}

// mutatingOperation reports whether repeating an operation could repeat an
// external effect. Reads are excluded: they carry no effect to deduplicate,
// and an artifact read streams bytes that no cache could replay.
func mutatingOperation(operation string) bool {
	switch operation {
	case localOperationQuery, localOperationArtifact, localOperationSupervisionShow:
		return false
	case localOperationSupervisionDecision:
		// A supervision decision is an effect: accepting a gate releases a
		// protected task, and replaying it must return the first answer rather
		// than deciding twice. The default below already says so; naming it
		// here records that the classification was made rather than inherited.
		return true
	default:
		return true
	}
}

// mutatingRequest reports whether this request could repeat an external
// effect. It is mutatingOperation refined by what the envelope actually asks
// for, and it is consulted in place of the operation word alone because one
// operation word carries both kinds: "node-wait" carries the registrations and
// wake transitions that change the coordinator's records and the two lists that
// only read them.
//
// The distinction is not a nicety. The wait runner of every host that is not the
// coordinator lists node waits on every tick, and treating that read as a
// mutation takes the coordinator's exclusive admin-replay lock, spends a request
// identity and writes the whole answer into a store budgeted at 32 MiB and 4096
// rows. That store is the fleet's 24-hour window for recovering the lost answer
// to a submission; a read that carries no effect to deduplicate must not spend
// it.
//
// Only the two list actions are reclassified. Everything else this operation
// word carries, including the task-wake transitions and expiries the runner also
// sends, keeps its replay protection.
func mutatingRequest(operation string, request localRequest) bool {
	if !mutatingOperation(operation) {
		return false
	}
	if operation == localOperationNodeWait && request.NodeWait != nil {
		switch request.NodeWait.Action {
		case "list", "list-task":
			return false
		}
	}
	return true
}

func asProtocolError(err error) *workerproto.ProtocolError {
	var protocolErr *workerproto.ProtocolError
	if errors.As(err, &protocolErr) {
		return protocolErr
	}
	return &workerproto.ProtocolError{Code: workerproto.ErrorMalformed, Message: err.Error()}
}

// validate checks the frame's authority and integrity before any local effect.
// Its second return is the credential the signature verified against and its
// third says whether that verification succeeded, which decides whether a
// refusal can be signed.
//
// The checks that do not depend on the principal run first, so that the order
// in which a request fails says nothing about which principals are configured.
//
// pinned is the operation the forced command fixed, or empty when it fixed none.
func (s *RemoteServer) validate(pinned string, frame remoteFrame) (localRequest, AdminCredentials, bool, *workerproto.ProtocolError) {
	var request localRequest
	var credentials AdminCredentials
	refuse := func(code workerproto.ErrorCode, message string) (localRequest, AdminCredentials, bool, *workerproto.ProtocolError) {
		return request, credentials, false, &workerproto.ProtocolError{Code: code, Message: message, RequestID: frame.RequestID}
	}
	if frame.Version != RemoteTransportVersion {
		return refuse(workerproto.ErrorUnsupportedVersion, "unsupported admin frame version")
	}
	if frame.SessionID == "" || frame.RequestID == "" || frame.Sequence < 1 {
		return refuse(workerproto.ErrorMalformed, "session, request and positive sequence are required")
	}
	if !ValidOperation(frame.Operation) {
		return refuse(workerproto.ErrorMalformed, "unknown admin frame operation")
	}
	if pinned != "" && frame.Operation != pinned {
		return refuse(workerproto.ErrorMalformed, "frame operation does not match the invoked operation")
	}
	if frame.Recipient != s.config.CoordinatorID {
		return refuse(workerproto.ErrorAuthorization, "recipient is not this coordinator")
	}
	now := s.config.Now()
	if frame.Deadline.IsZero() || !now.Before(frame.Deadline) {
		return refuse(workerproto.ErrorTimeout, "request deadline expired")
	}
	if frame.SentAt.IsZero() || frame.SentAt.Before(now.Add(-s.config.MaxClockSkew)) || frame.SentAt.After(now.Add(s.config.MaxClockSkew)) {
		return refuse(workerproto.ErrorAuthentication, "request timestamp is outside the allowed skew")
	}
	// The claimed principal selects which configured credential to check
	// against, exactly as a key id would. It proves nothing until the
	// signature over that credential's secret verifies, and an unknown
	// principal and a bad signature are reported identically.
	known, exists := s.config.Clients[frame.Sender]
	if !exists || frame.Authentication.Principal != known.ClientPrincipal ||
		frame.Authentication.KeyID != known.ClientKeyID {
		return refuse(workerproto.ErrorAuthentication, authenticationFailed)
	}
	if err := verifyRemoteFrame(frame, known.ClientSecret); err != nil {
		var verifyErr *workerproto.ProtocolError
		if errors.As(err, &verifyErr) && verifyErr.Code == workerproto.ErrorMalformed {
			// A payload checksum mismatch is not an identity question.
			return refuse(workerproto.ErrorMalformed, verifyErr.Message)
		}
		return refuse(workerproto.ErrorAuthentication, authenticationFailed)
	}
	// Everything from here on is a refusal the coordinator can sign.
	credentials = known
	signedRefusal := func(code workerproto.ErrorCode, message string) (localRequest, AdminCredentials, bool, *workerproto.ProtocolError) {
		return request, credentials, true, &workerproto.ProtocolError{Code: code, Message: message, RequestID: frame.RequestID}
	}
	decoder := json.NewDecoder(bytes.NewReader(frame.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return signedRefusal(workerproto.ErrorMalformed, "invalid operation envelope: "+err.Error())
	}
	if request.Version != LocalTransportVersion || request.Operation != frame.Operation {
		return signedRefusal(workerproto.ErrorMalformed, "operation envelope does not match the frame")
	}
	// The remote client does not get to say who it is. Its claimed principal
	// is discarded and replaced by the identity the signature proved.
	if request.RemoteAdmin != nil {
		return signedRefusal(workerproto.ErrorAuthorization, "a remote client may not assert its own principal")
	}
	if request.Query != nil {
		request.Query.Principal = Principal{}
	}
	if request.Mutation != nil {
		request.Mutation.Principal = Principal{}
	}
	if request.Operation == localOperationSubmission {
		if request.SubmissionSize <= 0 || request.SubmissionSize > s.config.MaxSubmissionBytes {
			return signedRefusal(workerproto.ErrorLimit, fmt.Sprintf(
				"submission archive of %d bytes exceeds this coordinator's limit of %d bytes",
				request.SubmissionSize, s.config.MaxSubmissionBytes))
		}
		if request.Submission == nil || request.Submission.ArchiveSHA256 == "" {
			return signedRefusal(workerproto.ErrorMalformed,
				"a remote submission must declare its archive digest")
		}
	}
	assertion := &RemoteAdminAssertion{
		Principal:   credentials.ClientPrincipal,
		Coordinator: s.config.CoordinatorID,
		RequestID:   frame.RequestID,
	}
	if s.config.Supervisors[credentials.ClientPrincipal] {
		assertion.Role = SupervisorRole
	}
	request.RemoteAdmin = assertion
	if s.config.Approvers[credentials.ClientPrincipal] && request.NodeWait != nil && request.NodeWait.Action == "decide-attention" {
		proof := frame
		request.ApprovalFrame = &proof
	}
	return request, credentials, true, nil
}

// respond relays the verified request to the coordinator over its owner-only
// socket and turns whatever came back into one answer payload. When stream is
// non-nil it receives the artifact content the caller must read and close.
func (s *RemoteServer) respond(ctx context.Context, request localRequest, body io.Reader, stream *io.ReadCloser) localResponse {
	response, content, err := s.config.Relay.exchange(ctx, request, body)
	if err != nil {
		class := ClassOf(err)
		if class == "" {
			class = ClassRejected
		}
		return localResponse{Version: LocalTransportVersion, Error: err.Error(), ErrorClass: class}
	}
	if content != nil {
		if stream == nil {
			_ = content.Close()
		} else {
			*stream = content
		}
	}
	return response
}

// relay answers an operation that is not replay-protected, streaming artifact
// content after the response frame.
func (s *RemoteServer) relay(ctx context.Context, frame remoteFrame, operation string, request localRequest, credentials AdminCredentials, body io.Reader, out io.Writer) error {
	var stream io.ReadCloser
	response := s.respond(ctx, request, body, &stream)
	if stream != nil {
		defer stream.Close()
	}
	if err := s.write(out, frame, operation, response, credentials); err != nil {
		return err
	}
	if stream == nil {
		return nil
	}
	_, err := io.CopyN(out, stream, response.ArtifactSize)
	return err
}

// write signs one answer for the request in hand and emits it. The frame is
// always built here rather than reused, so a replayed answer is bound to the
// session and request id of the retry that asked for it.
func (s *RemoteServer) write(out io.Writer, request remoteFrame, operation string, response localResponse, credentials AdminCredentials) error {
	frame, err := s.sign(request, operation, response, credentials)
	if err != nil {
		return err
	}
	return writeRemoteFrame(out, frame, s.config.MaxRequestBytes)
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

// refuse answers a request the coordinator will not serve. When the signature
// verified, credentials is non-nil and the refusal is signed like any other
// answer. When it did not, the refusal is unsigned: there is no verified peer
// to sign for. The class still travels, because an agent has to tell a rotated
// credential from a momentary outage, and an unsigned frame can only ever be
// read as a failure, never as a result.
func (s *RemoteServer) refuse(out io.Writer, request remoteFrame, operation string, credentials *AdminCredentials, protocolErr *workerproto.ProtocolError) error {
	response := localResponse{
		Version:    LocalTransportVersion,
		Error:      protocolErr.Message,
		ErrorClass: classOfProtocolError(protocolErr.Code),
	}
	if credentials != nil {
		if err := s.write(out, request, operation, response, *credentials); err != nil {
			return err
		}
		return fmt.Errorf("coordinator-exchange refused %s: %w", operation, protocolErr)
	}
	frame, err := newRemoteFrame(operation, request.SessionID, "response-"+request.RequestID,
		s.config.CoordinatorID, request.Sender, request.Sequence, s.config.Now(), request.Deadline, response)
	if err != nil {
		return err
	}
	frame.InReplyTo = request.RequestID
	if err := writeRemoteFrame(out, frame, s.config.MaxRequestBytes); err != nil {
		return err
	}
	return fmt.Errorf("coordinator-exchange refused %s: %w", operation, protocolErr)
}
