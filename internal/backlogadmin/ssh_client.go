package backlogadmin

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// sshTokenPattern is the argv gate. Every token this client puts on an ssh
// command line must match it, and none may start with a dash, so no
// configuration value and no request field can become an ssh option or a shell
// fragment. It is the same gate the worker transport applies.
var sshTokenPattern = regexp.MustCompile(`^[A-Za-z0-9._@:/-]+$`)

// CoordinatorExchangeCommand is the subcommand word of the forced command.
const CoordinatorExchangeCommand = "coordinator-exchange"

// SSHClientConfig describes the remote coordinator this host talks to.
type SSHClientConfig struct {
	CoordinatorID      string
	Address            string
	RemoteCommand      string
	Credentials        AdminCredentials
	RequestTimeout     time.Duration
	ConnectTimeout     time.Duration
	MaxResponseBytes   int64
	MaxArtifactBytes   int64
	MaxSubmissionBytes int64
	MaxStderrBytes     int64
	SessionID          string
	Now                func() time.Time
	Factory            workerproto.CommandFactory
}

// SSHClient is the remote carrier: one SSH session per request, invoking the
// restricted forced command on the coordinator host.
type SSHClient struct {
	config SSHClientConfig
}

func NewSSHClient(config SSHClientConfig) (*SSHClient, error) {
	if config.CoordinatorID == "" {
		return nil, configurationError("backlog_v2.coordinator_client.coordinator_id is required")
	}
	if !sshTokenPattern.MatchString(config.Address) || strings.HasPrefix(config.Address, "-") {
		return nil, configurationError("backlog_v2.coordinator_client.address must be a safe ssh destination")
	}
	if !sshTokenPattern.MatchString(config.RemoteCommand) || strings.HasPrefix(config.RemoteCommand, "-") {
		return nil, configurationError("backlog_v2.coordinator_client.remote_command must be a safe command path")
	}
	if !config.Credentials.Complete() {
		return nil, configurationError("backlog_v2.coordinator_client.credential did not resolve to a complete admin identity")
	}
	if config.RequestTimeout <= 0 || config.MaxResponseBytes <= 0 ||
		config.MaxArtifactBytes <= 0 || config.MaxSubmissionBytes <= 0 {
		return nil, configurationError("backlog_v2.coordinator_client timeouts and byte limits must be positive")
	}
	if config.ConnectTimeout <= 0 {
		config.ConnectTimeout = 10 * time.Second
	}
	if config.MaxStderrBytes <= 0 {
		config.MaxStderrBytes = 64 << 10
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.Factory == nil {
		config.Factory = exec.CommandContext
	}
	if config.SessionID == "" {
		id, err := randomIdentifier()
		if err != nil {
			return nil, err
		}
		config.SessionID = "admin-" + id
	}
	config.Credentials.ClientSecret = append([]byte(nil), config.Credentials.ClientSecret...)
	config.Credentials.CoordinatorSecret = append([]byte(nil), config.Credentials.CoordinatorSecret...)
	return &SSHClient{config: config}, nil
}

func configurationError(message string) error {
	return &TransportError{Class: ClassClientConfiguration, Err: errors.New(message)}
}

func (c *SSHClient) fail(class TransportClass, operation string, err error) error {
	return classify(class, operation, c.config.CoordinatorID, err)
}

// Describe names the coordinator and the destination, never the credential.
func (c *SSHClient) Describe() TransportDescription {
	return TransportDescription{
		Carrier:       CarrierRemote,
		CoordinatorID: c.config.CoordinatorID,
		Endpoint:      c.config.Address + " " + c.config.RemoteCommand + " " + CoordinatorExchangeCommand,
	}
}

// arguments builds the exact ssh argv. Nothing here is interpolated from a
// request: the operation is one of nine fixed words and everything else is
// configuration that NewSSHClient already gated.
func (c *SSHClient) arguments(operation string) ([]string, error) {
	if !ValidOperation(operation) {
		return nil, configurationError(fmt.Sprintf("unknown coordinator-admin operation %q", operation))
	}
	connectSeconds := max(int(c.config.ConnectTimeout.Round(time.Second)/time.Second), 1)
	return []string{
		"-oBatchMode=yes",
		"-oStrictHostKeyChecking=yes",
		"-oConnectTimeout=" + strconv.Itoa(connectSeconds),
		"--", c.config.Address, c.config.RemoteCommand, CoordinatorExchangeCommand, operation,
	}, nil
}

// requestIdentity derives the request id from whatever idempotency key the
// operation already carries, so that retrying a lost answer with the same key
// reaches the coordinator's replay store as the same request. Reads, which
// carry no effect to deduplicate, get a fresh identifier.
func requestIdentity(request localRequest) (string, error) {
	stable := func(prefix, id string) (string, error) {
		if strings.TrimSpace(id) == "" {
			return randomRequestID()
		}
		return prefix + "/" + id, nil
	}
	switch request.Operation {
	case localOperationSubmission:
		if request.Submission != nil {
			return stable(localOperationSubmission, request.Submission.IdempotencyKey)
		}
	case localOperationScheduleDefinition:
		if request.ScheduleDefinition != nil {
			return stable(localOperationScheduleDefinition, request.ScheduleDefinition.RequestID)
		}
	case localOperationUnknownRecovery:
		if request.UnknownRecovery != nil {
			return stable(localOperationUnknownRecovery, request.UnknownRecovery.ID)
		}
	case localOperationMutation:
		if request.Mutation != nil {
			return stable(localOperationMutation, request.Mutation.ID)
		}
	case localOperationGraphAmendment:
		if request.GraphAmendment != nil {
			return stable(localOperationGraphAmendment, request.GraphAmendment.ID)
		}
	case localOperationWorkerEnrollment:
		if request.WorkerEnrollment != nil {
			return stable(localOperationWorkerEnrollment, request.WorkerEnrollment.ID)
		}
	case localOperationNodeWait:
		if request.NodeWait != nil && request.NodeWait.Action == "register" {
			return stable(localOperationNodeWait, request.NodeWait.Request.ID)
		}
	}
	return randomRequestID()
}

func randomRequestID() (string, error) {
	id, err := randomIdentifier()
	if err != nil {
		return "", err
	}
	return "req/" + id, nil
}

func randomIdentifier() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate admin request identity: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

// remoteExchange is one live SSH session. Its stdout stays open while an
// artifact streams, which is why the session is not closed by roundTrip.
type remoteExchange struct {
	command *exec.Cmd
	stdout  *bufio.Reader
	stderr  *boundedStderr
	cancel  context.CancelFunc
}

// Close waits for the session to finish before releasing the request context.
// Cancelling first would kill a coordinator that is still writing its answer.
func (e *remoteExchange) Close() error {
	err := e.command.Wait()
	if e.cancel != nil {
		e.cancel()
	}
	return err
}

// roundTrip signs one request, runs the restricted command and authenticates
// the response frame. body carries the submission archive; the returned
// exchange is open only when the caller asked for the raw stream that follows
// an artifact response.
func (c *SSHClient) roundTrip(
	ctx context.Context,
	request localRequest,
	body io.Reader,
	stream bool,
) (localResponse, *remoteExchange, error) {
	operation := request.Operation
	arguments, err := c.arguments(operation)
	if err != nil {
		return localResponse{}, nil, err
	}
	if request.RemoteAdmin != nil {
		return localResponse{}, nil, c.fail(ClassClientConfiguration, operation,
			errors.New("the client may not assert its own remote principal"))
	}
	requestID, err := requestIdentity(request)
	if err != nil {
		return localResponse{}, nil, c.fail(ClassClientConfiguration, operation, err)
	}
	now := c.config.Now()
	deadline := now.Add(c.config.RequestTimeout)
	// One SSH session per request means there is no durable session to order,
	// so the sequence is constant and the coordinator's replay store, not the
	// sequence, is what refuses a repeated request id.
	frame, err := newRemoteFrame(operation, c.config.SessionID, requestID,
		c.config.Credentials.ClientPrincipal, c.config.CoordinatorID, 1, now, deadline, request)
	if err != nil {
		return localResponse{}, nil, c.fail(ClassProtocol, operation, err)
	}
	if err := signRemoteFrame(&frame, c.config.Credentials.ClientPrincipal,
		c.config.Credentials.ClientKeyID, c.config.Credentials.ClientSecret); err != nil {
		return localResponse{}, nil, c.fail(ClassAuthentication, operation, err)
	}

	requestCtx, cancel := context.WithDeadline(ctx, deadline)
	command := c.config.Factory(requestCtx, "ssh", arguments...)
	stdin, err := command.StdinPipe()
	if err != nil {
		cancel()
		return localResponse{}, nil, c.fail(ClassUnavailable, operation, err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		cancel()
		return localResponse{}, nil, c.fail(ClassUnavailable, operation, err)
	}
	stderr := &boundedStderr{limit: c.config.MaxStderrBytes}
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		// A deadline that had already passed when the session was about to
		// start is a timeout, not an outage: nothing was unreachable.
		class := contextClass(requestCtx.Err())
		cancel()
		return localResponse{}, nil, c.fail(class, operation, fmt.Errorf("start coordinator exchange: %w", err))
	}
	exchange := &remoteExchange{command: command, stdout: bufio.NewReader(stdout), stderr: stderr, cancel: cancel}
	written := make(chan error, 1)
	go func() {
		defer stdin.Close()
		if err := writeRemoteFrame(stdin, frame, c.config.MaxResponseBytes); err != nil {
			written <- err
			return
		}
		if operation == localOperationSubmission && request.SubmissionSize > 0 {
			if _, err := io.CopyN(stdin, body, request.SubmissionSize); err != nil {
				written <- err
				return
			}
		}
		written <- nil
	}()

	response, err := c.readResponse(exchange, frame)
	if writeErr := <-written; writeErr != nil && err == nil {
		err = c.fail(ClassUnavailable, operation, fmt.Errorf("write coordinator exchange request: %w", writeErr))
	}
	if err != nil {
		// The request context is read before Close, which releases it: a
		// deadline that expired must be reported as a deadline, not as the
		// cancellation that tidying up produced.
		causeCtx := requestCtx.Err()
		_ = exchange.Close()
		return localResponse{}, nil, c.exchangeError(causeCtx, operation, exchange, err)
	}
	if err := c.validate(operation, response); err != nil {
		_ = exchange.Close()
		return localResponse{}, nil, err
	}
	if !stream {
		causeCtx := requestCtx.Err()
		if err := exchange.Close(); err != nil {
			return localResponse{}, nil, c.exchangeError(causeCtx, operation, exchange,
				c.fail(ClassUnavailable, operation, fmt.Errorf("coordinator exchange: %w", err)))
		}
		return response, nil, nil
	}
	return response, exchange, nil
}

// exchangeError explains a failed session with the remote stderr and the
// deadline, so a timeout is reported as a timeout rather than as a decode
// error on an empty stream.
func (c *SSHClient) exchangeError(ctxErr error, operation string, exchange *remoteExchange, err error) error {
	if ctxErr != nil {
		return c.fail(contextClass(ctxErr), operation, fmt.Errorf("coordinator exchange: %w", ctxErr))
	}
	detail := strings.TrimSpace(exchange.stderr.String())
	if detail == "" {
		return err
	}
	cause := errors.Unwrap(err)
	if cause == nil {
		cause = err
	}
	return classify(ClassOf(err), operation, c.config.CoordinatorID, fmt.Errorf("%w: %s", cause, detail))
}

func (c *SSHClient) readResponse(exchange *remoteExchange, request remoteFrame) (localResponse, error) {
	frame, err := readRemoteFrame(exchange.stdout, c.config.MaxResponseBytes)
	if err != nil {
		return localResponse{}, c.protocolFailure(request.Operation, err)
	}
	var response localResponse
	if err := json.Unmarshal(frame.Payload, &response); err != nil {
		return localResponse{}, c.fail(ClassProtocol, request.Operation,
			fmt.Errorf("decode coordinator response: %w", err))
	}
	if frame.Authentication.Signature == "" {
		return localResponse{}, c.unsignedRefusal(request.Operation, response)
	}
	if frame.Version != RemoteTransportVersion || frame.InReplyTo != request.RequestID ||
		frame.SessionID != request.SessionID || frame.Operation != request.Operation ||
		frame.Sender != c.config.CoordinatorID || frame.Recipient != request.Sender {
		return localResponse{}, c.fail(ClassProtocol, request.Operation,
			errors.New("coordinator response identity does not match the request"))
	}
	if frame.Authentication.Principal != c.config.Credentials.CoordinatorPrincipal ||
		frame.Authentication.KeyID != c.config.Credentials.CoordinatorKeyID {
		return localResponse{}, c.fail(ClassAuthentication, request.Operation,
			errors.New("coordinator response principal or key id does not match the configured credential"))
	}
	if err := verifyRemoteFrame(frame, c.config.Credentials.CoordinatorSecret); err != nil {
		return localResponse{}, c.protocolFailure(request.Operation, err)
	}
	return response, nil
}

// unsignedRefusal reads an unsigned frame strictly as a failure classification.
//
// The coordinator cannot sign a refusal it produced before verifying who was
// asking, and an agent still has to tell a rotated credential from a momentary
// outage. So the class is allowed through while the frame's identity fields and
// its payload are trusted for nothing else: an unsigned frame that does not
// carry an error is refused outright, so it can never become an answer, and the
// class is narrowed to the three a pre-verification refusal can legitimately
// have. Substituting one of these for a real answer turns a success into a
// reported failure, which the caller recovers by retrying with the same
// idempotency key; it cannot turn a refusal into a success.
func (c *SSHClient) unsignedRefusal(operation string, response localResponse) error {
	if response.Error == "" {
		return c.fail(ClassProtocol, operation,
			errors.New("unsigned coordinator frame is not an answer"))
	}
	switch response.ErrorClass {
	case ClassAuthentication, ClassProtocol, ClassTimeout:
		return c.fail(response.ErrorClass, operation, errors.New(response.Error))
	default:
		return c.fail(ClassProtocol, operation, errors.New(response.Error))
	}
}

// contextClass distinguishes a request whose deadline expired from one that was
// cancelled, because an agent retries the first and stops for the second.
func contextClass(err error) TransportClass {
	if errors.Is(err, context.DeadlineExceeded) {
		return ClassTimeout
	}
	return ClassUnavailable
}

// protocolFailure maps a wire error code onto the shared taxonomy.
func (c *SSHClient) protocolFailure(operation string, err error) error {
	var protocolErr *workerproto.ProtocolError
	if errors.As(err, &protocolErr) {
		return c.fail(classOfProtocolError(protocolErr.Code), operation, err)
	}
	return c.fail(ClassProtocol, operation, err)
}

func (c *SSHClient) validate(operation string, response localResponse) error {
	if response.Version != LocalTransportVersion {
		return c.fail(ClassProtocol, operation, errors.New("invalid coordinator admin response version"))
	}
	if response.Error == "" {
		return nil
	}
	class := response.ErrorClass
	if class == "" {
		class = ClassRejected
	}
	// The supervision class rides only on a verified coordinator answer, which
	// this is: an unsigned refusal never reaches here.
	return classifySupervision(class, response.SupervisionClass, operation, c.config.CoordinatorID, errors.New(response.Error))
}

func (c *SSHClient) Query(ctx context.Context, query Query) (Response, error) {
	response, _, err := c.roundTrip(ctx, localRequest{
		Version: LocalTransportVersion, Operation: localOperationQuery, Query: &query,
	}, nil, false)
	if err != nil {
		return Response{}, err
	}
	if response.Response == nil {
		return Response{}, c.fail(ClassProtocol, localOperationQuery, errors.New("coordinator query returned no response"))
	}
	return *response.Response, nil
}

func (c *SSHClient) Mutate(ctx context.Context, mutation Mutation) (MutationResponse, error) {
	response, _, err := c.roundTrip(ctx, localRequest{
		Version: LocalTransportVersion, Operation: localOperationMutation, Mutation: &mutation,
	}, nil, false)
	if err != nil {
		return MutationResponse{}, err
	}
	if response.MutationResponse == nil {
		return MutationResponse{}, c.fail(ClassProtocol, localOperationMutation, errors.New("coordinator mutation returned no response"))
	}
	return *response.MutationResponse, nil
}

func (c *SSHClient) RecoverUnknown(ctx context.Context, _ Principal, request UnknownRecoveryRequest) (domain.UnknownAssignmentRecoveryDecision, error) {
	response, _, err := c.roundTrip(ctx, localRequest{
		Version: LocalTransportVersion, Operation: localOperationUnknownRecovery, UnknownRecovery: &request,
	}, nil, false)
	if err != nil {
		return domain.UnknownAssignmentRecoveryDecision{}, err
	}
	if response.UnknownRecoveryResponse == nil {
		return domain.UnknownAssignmentRecoveryDecision{}, c.fail(ClassProtocol, localOperationUnknownRecovery, errors.New("coordinator unknown recovery returned no response"))
	}
	return *response.UnknownRecoveryResponse, nil
}

func (c *SSHClient) ReleaseQuarantine(ctx context.Context, _ Principal, request QuarantineReleaseRequest) (domain.QuarantineRelease, error) {
	response, _, err := c.roundTrip(ctx, localRequest{
		Version: LocalTransportVersion, Operation: localOperationQuarantineRelease, QuarantineRelease: &request,
	}, nil, false)
	if err != nil {
		return domain.QuarantineRelease{}, err
	}
	if response.QuarantineReleaseResponse == nil {
		return domain.QuarantineRelease{}, c.fail(ClassProtocol, localOperationQuarantineRelease, errors.New("coordinator quarantine release returned no response"))
	}
	return *response.QuarantineReleaseResponse, nil
}

func (c *SSHClient) PutSchedule(ctx context.Context, _ Principal, definition LocalScheduleDefinitionRequest) (LocalScheduleDefinitionResponse, error) {
	response, _, err := c.roundTrip(ctx, localRequest{
		Version: LocalTransportVersion, Operation: localOperationScheduleDefinition, ScheduleDefinition: &definition,
	}, nil, false)
	if err != nil {
		return LocalScheduleDefinitionResponse{}, err
	}
	if response.ScheduleDefinitionResponse == nil {
		return LocalScheduleDefinitionResponse{}, c.fail(ClassProtocol, localOperationScheduleDefinition, errors.New("coordinator schedule definition returned no response"))
	}
	return *response.ScheduleDefinitionResponse, nil
}

func (c *SSHClient) NodeWait(ctx context.Context, operation NodeWaitOperation) (NodeWaitResponse, error) {
	response, _, err := c.roundTrip(ctx, localRequest{
		Version: LocalTransportVersion, Operation: localOperationNodeWait, NodeWait: &operation,
	}, nil, false)
	if err != nil {
		return NodeWaitResponse{}, err
	}
	if response.NodeWait == nil {
		return NodeWaitResponse{}, c.fail(ClassProtocol, localOperationNodeWait, errors.New("coordinator node wait returned no response"))
	}
	return *response.NodeWait, nil
}

func (c *SSHClient) AmendGraph(ctx context.Context, amendment domain.GraphAmendment) (domain.GraphAmendmentResult, error) {
	response, _, err := c.roundTrip(ctx, localRequest{
		Version: LocalTransportVersion, Operation: localOperationGraphAmendment, GraphAmendment: &amendment,
	}, nil, false)
	if err != nil {
		return domain.GraphAmendmentResult{}, err
	}
	if response.GraphAmendment == nil {
		return domain.GraphAmendmentResult{}, c.fail(ClassProtocol, localOperationGraphAmendment, errors.New("coordinator graph amendment returned no response"))
	}
	return *response.GraphAmendment, nil
}

// Supervise sends one supervision operation under the word its own operation
// selects, so that a key pinned to supervision-show cannot carry a decision.
func (c *SSHClient) Supervise(ctx context.Context, request SupervisionRequest) (SupervisionResponse, error) {
	operation := localOperationSupervisionShow
	if request.Operation.Mutating() {
		operation = localOperationSupervisionDecision
	}
	response, _, err := c.roundTrip(ctx, localRequest{
		Version: LocalTransportVersion, Operation: operation, Supervision: &request,
	}, nil, false)
	if err != nil {
		return SupervisionResponse{}, err
	}
	if response.SupervisionResponse == nil {
		return SupervisionResponse{}, c.fail(ClassProtocol, operation, errors.New("coordinator supervision returned no response"))
	}
	return *response.SupervisionResponse, nil
}

func (c *SSHClient) EnrollWorker(ctx context.Context, request domain.WorkerEnrollmentRequest) (domain.WorkerEnrollment, error) {
	response, _, err := c.roundTrip(ctx, localRequest{
		Version: LocalTransportVersion, Operation: localOperationWorkerEnrollment, WorkerEnrollment: &request,
	}, nil, false)
	if err != nil {
		return domain.WorkerEnrollment{}, err
	}
	if response.WorkerEnrollment == nil {
		return domain.WorkerEnrollment{}, c.fail(ClassProtocol, localOperationWorkerEnrollment, errors.New("coordinator worker enrollment returned no response"))
	}
	return *response.WorkerEnrollment, nil
}

// SubmitArchive sends the bundle with a digest of its bytes in the request, so
// that the coordinator's replay store refuses the same idempotency key carrying
// different content instead of answering it from the cache.
//
// Computing the digest means holding the archive in memory. That is bounded by
// the same configured submission limit the coordinator enforces, four MiB by
// default, and a submission that does not fit is refused before it is read.
func (c *SSHClient) SubmitArchive(ctx context.Context, request LocalSubmissionRequest, archive io.Reader, size int64) (LocalSubmissionResponse, error) {
	if archive == nil {
		return LocalSubmissionResponse{}, c.fail(ClassClientConfiguration, localOperationSubmission, errors.New("submission archive is required"))
	}
	if size <= 0 || size > c.config.MaxSubmissionBytes {
		return LocalSubmissionResponse{}, c.fail(ClassClientConfiguration, localOperationSubmission,
			fmt.Errorf("submission archive of %d bytes exceeds the configured limit of %d bytes", size, c.config.MaxSubmissionBytes))
	}
	raw := make([]byte, size)
	if _, err := io.ReadFull(archive, raw); err != nil {
		return LocalSubmissionResponse{}, c.fail(ClassClientConfiguration, localOperationSubmission,
			fmt.Errorf("read submission archive: %w", err))
	}
	sum := sha256.Sum256(raw)
	request.ArchiveSHA256 = hex.EncodeToString(sum[:])
	response, _, err := c.roundTrip(ctx, localRequest{
		Version: LocalTransportVersion, Operation: localOperationSubmission,
		Submission: &request, SubmissionSize: size,
	}, bytes.NewReader(raw), false)
	if err != nil {
		return LocalSubmissionResponse{}, err
	}
	if response.SubmissionResponse == nil {
		return LocalSubmissionResponse{}, c.fail(ClassProtocol, localOperationSubmission, errors.New("coordinator submission returned no response"))
	}
	return *response.SubmissionResponse, nil
}

// OpenArtifact keeps the SSH session open and hands back exactly the declared
// number of raw bytes that follow the response frame.
func (c *SSHClient) OpenArtifact(ctx context.Context, _ Principal, artifactID string) (ArtifactContent, error) {
	response, exchange, err := c.roundTrip(ctx, localRequest{
		Version: LocalTransportVersion, Operation: localOperationArtifact, ArtifactID: artifactID,
	}, nil, true)
	if err != nil {
		return ArtifactContent{}, err
	}
	if response.ArtifactMetadata == nil || response.ArtifactSize < 0 ||
		response.ArtifactSize != response.ArtifactMetadata.Size ||
		response.ArtifactSize > c.config.MaxArtifactBytes {
		_ = exchange.Close()
		return ArtifactContent{}, c.fail(ClassProtocol, localOperationArtifact, errors.New("invalid coordinator artifact response"))
	}
	return ArtifactContent{Metadata: *response.ArtifactMetadata, Content: &exactReadCloser{
		reader: exchange.stdout, closer: exchange, remaining: response.ArtifactSize,
	}}, nil
}

// boundedStderr keeps a remote diagnostic without letting it grow unbounded.
type boundedStderr struct {
	data  []byte
	limit int64
}

func (b *boundedStderr) Write(data []byte) (int, error) {
	remaining := b.limit - int64(len(b.data))
	if remaining > 0 {
		if int64(len(data)) > remaining {
			b.data = append(b.data, data[:remaining]...)
		} else {
			b.data = append(b.data, data...)
		}
	}
	return len(data), nil
}

func (b *boundedStderr) String() string {
	return string(b.data)
}

var _ CoordinatorAdminTransport = (*SSHClient)(nil)
