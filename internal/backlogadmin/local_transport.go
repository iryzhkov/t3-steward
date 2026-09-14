package backlogadmin

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const LocalTransportVersion = "backlog.admin.local/v1"

const (
	localOperationQuery              = "query"
	localOperationMutation           = "mutation"
	localOperationArtifact           = "artifact"
	localOperationSubmission         = "submission"
	localOperationScheduleDefinition = "schedule-definition"
	localOperationUnknownRecovery    = "unknown-recovery"
	localOperationNodeWait           = "node-wait"
	localOperationGraphAmendment     = "graph-amendment"
	localOperationWorkerEnrollment   = "worker-enrollment"
)

// Operations is the complete coordinator-admin operation vocabulary, in the
// order the H1 contract lists it. The restricted SSH command accepts exactly
// these words and nothing else.
func Operations() []string {
	return []string{
		localOperationQuery,
		localOperationMutation,
		localOperationArtifact,
		localOperationSubmission,
		localOperationScheduleDefinition,
		localOperationUnknownRecovery,
		localOperationNodeWait,
		localOperationGraphAmendment,
		localOperationWorkerEnrollment,
	}
}

// ValidOperation reports whether name is one of the nine operations.
func ValidOperation(name string) bool {
	for _, operation := range Operations() {
		if operation == name {
			return true
		}
	}
	return false
}

type LocalService interface {
	Query(context.Context, Query) (Response, error)
	Mutate(context.Context, Mutation) (MutationResponse, error)
	OpenArtifact(context.Context, Principal, string) (ArtifactContent, error)
	SubmitArchive(context.Context, Principal, LocalSubmissionRequest, io.Reader) (LocalSubmissionResponse, error)
	PutSchedule(context.Context, Principal, LocalScheduleDefinitionRequest) (LocalScheduleDefinitionResponse, error)
	RecoverUnknown(context.Context, Principal, UnknownRecoveryRequest) (domain.UnknownAssignmentRecoveryDecision, error)
}

type LocalSubmissionRequest struct {
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
	// Unverified says the client skipped its own live readiness check, and
	// UnverifiedReason says why. They exist so that the escape hatch is loud in
	// the coordinator's audit record rather than invisible once the run exists.
	// Skipping the client check never skips the coordinator's own permanent
	// validation at acceptance.
	Unverified       bool   `json:"unverified,omitempty"`
	UnverifiedReason string `json:"unverifiedReason,omitempty"`
	// Principal is the identity the client claims. The coordinator overwrites
	// it with the principal its own authentication produced, exactly as it does
	// for every other operation, so a claimed identity is never trusted.
	Principal string `json:"principal,omitempty"`
}

type LocalSubmissionResponse struct {
	Key        string `json:"key"`
	Digest     string `json:"digest"`
	WorkflowID string `json:"workflowId"`
	RunID      string `json:"runId"`
	State      string `json:"state"`
	AcceptedAt string `json:"acceptedAt"`
	Replay     bool   `json:"replay"`
}

type LocalScheduleDefinitionRequest struct {
	RequestID        string                       `json:"requestId"`
	ID               string                       `json:"id"`
	Name             string                       `json:"name"`
	WorkflowID       string                       `json:"workflowId"`
	Expression       string                       `json:"expression"`
	Timezone         string                       `json:"timezone"`
	AfterFailure     domain.ScheduleFailurePolicy `json:"afterFailure,omitempty"`
	Enabled          bool                         `json:"enabled"`
	ExpectedRevision int64                        `json:"expectedRevision"`
	Reason           string                       `json:"reason"`
}

type LocalScheduleDefinitionResponse struct {
	Schedule domain.Schedule `json:"schedule"`
	Replay   bool            `json:"replay"`
}

// RemoteAdminAssertion is what the restricted SSH command states about the
// remote client whose signed frame it has already verified. It is accepted
// only over the owner-only socket, from a peer that already holds full local
// admin authority, and it can only narrow that authority to the remote-admin
// role. It never carries a credential or a credential reference.
type RemoteAdminAssertion struct {
	Principal   string `json:"principal"`
	Coordinator string `json:"coordinator"`
	RequestID   string `json:"requestId"`
}

type localRequest struct {
	RemoteAdmin        *RemoteAdminAssertion           `json:"remoteAdmin,omitempty"`
	WorkerEnrollment   *domain.WorkerEnrollmentRequest `json:"workerEnrollment,omitempty"`
	GraphAmendment     *domain.GraphAmendment          `json:"graphAmendment,omitempty"`
	NodeWait           *NodeWaitOperation              `json:"nodeWait,omitempty"`
	Version            string                          `json:"version"`
	Operation          string                          `json:"operation"`
	Query              *Query                          `json:"query,omitempty"`
	Mutation           *Mutation                       `json:"mutation,omitempty"`
	ArtifactID         string                          `json:"artifactId,omitempty"`
	Submission         *LocalSubmissionRequest         `json:"submission,omitempty"`
	SubmissionSize     int64                           `json:"submissionSize,omitempty"`
	ScheduleDefinition *LocalScheduleDefinitionRequest `json:"scheduleDefinition,omitempty"`
	UnknownRecovery    *UnknownRecoveryRequest         `json:"unknownRecovery,omitempty"`
}

type localResponse struct {
	WorkerEnrollment           *domain.WorkerEnrollment                  `json:"workerEnrollment,omitempty"`
	GraphAmendment             *domain.GraphAmendmentResult              `json:"graphAmendment,omitempty"`
	NodeWait                   *NodeWaitResponse                         `json:"nodeWait,omitempty"`
	Version                    string                                    `json:"version"`
	Response                   *Response                                 `json:"response,omitempty"`
	MutationResponse           *MutationResponse                         `json:"mutationResponse,omitempty"`
	ArtifactMetadata           *ArtifactMetadata                         `json:"artifactMetadata,omitempty"`
	ArtifactSize               int64                                     `json:"artifactSize,omitempty"`
	SubmissionResponse         *LocalSubmissionResponse                  `json:"submissionResponse,omitempty"`
	ScheduleDefinitionResponse *LocalScheduleDefinitionResponse          `json:"scheduleDefinitionResponse,omitempty"`
	UnknownRecoveryResponse    *domain.UnknownAssignmentRecoveryDecision `json:"unknownRecoveryResponse,omitempty"`
	Error                      string                                    `json:"error,omitempty"`
	// ErrorClass lets the server say whether it refused the principal, the
	// frame or the request itself, so the client does not have to guess a
	// class by matching prose. An absent class means the coordinator answered
	// and refused the request.
	ErrorClass TransportClass `json:"errorClass,omitempty"`
}

// LocalServer serves one bounded request per authenticated Unix connection.
type LocalServer struct {
	Listener           *net.UnixListener
	Service            LocalService
	AllowedUID         uint32
	MaxRequestBytes    int64
	MaxArtifactBytes   int64
	MaxSubmissionBytes int64
	RequestTimeout     time.Duration
	MaxConcurrent      int
}

func ListenLocal(path string) (*net.UnixListener, error) {
	if strings.TrimSpace(path) != path || path == "" || !filepath.IsAbs(path) {
		return nil, errors.New("admin socket path must be absolute and trimmed")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create admin socket directory: %w", err)
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("admin socket path exists and is not a socket: %s", path)
		}
		conn, dialErr := net.Dial("unix", path)
		if dialErr == nil {
			conn.Close()
			return nil, fmt.Errorf("admin socket is already accepting connections: %s", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("remove stale admin socket: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect admin socket: %w", err)
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("listen on admin socket: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		listener.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("protect admin socket: %w", err)
	}
	return listener, nil
}

func (s *LocalServer) Serve(ctx context.Context) error {
	if s == nil || s.Listener == nil || s.Service == nil {
		return errors.New("local admin server requires listener and service")
	}
	if s.MaxRequestBytes <= 0 || s.MaxArtifactBytes <= 0 || s.MaxSubmissionBytes <= 0 ||
		s.RequestTimeout <= 0 || s.MaxConcurrent <= 0 {
		return errors.New("local admin server byte, timeout, and concurrency limits must be positive")
	}
	var workers sync.WaitGroup
	active := make(chan struct{}, s.MaxConcurrent)
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = s.Listener.Close()
		case <-stop:
		}
	}()
	defer close(stop)
	defer workers.Wait()
	for {
		conn, err := s.Listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept admin connection: %w", err)
		}
		_ = conn.SetDeadline(time.Now().Add(s.RequestTimeout))
		select {
		case active <- struct{}{}:
		default:
			_ = writeLocalResponse(conn, localResponse{Version: LocalTransportVersion, Error: "local admin backpressure: concurrency limit reached"})
			_ = conn.Close()
			continue
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() { <-active }()
			defer conn.Close()
			stopWatch := watchConnection(ctx, conn)
			defer stopWatch()
			s.serveConnection(ctx, conn)
		}()
	}
}

func (s *LocalServer) serveConnection(ctx context.Context, conn *net.UnixConn) {
	var request localRequest
	if err := readLocalJSON(conn, s.MaxRequestBytes, &request); err != nil {
		_ = writeLocalResponse(conn, localResponse{Version: LocalTransportVersion, Error: err.Error(), ErrorClass: ClassProtocol})
		return
	}
	principal, err := localPeerPrincipal(conn, s.AllowedUID)
	if err != nil {
		_ = writeLocalResponse(conn, localResponse{Version: LocalTransportVersion, Error: err.Error(), ErrorClass: ClassAuthentication})
		return
	}
	if request.Version != LocalTransportVersion {
		_ = writeLocalResponse(conn, localResponse{Version: LocalTransportVersion, Error: "unsupported local admin transport version", ErrorClass: ClassProtocol})
		return
	}
	// The restricted SSH command relays a request it has already authenticated
	// by signature, and says so here. The assertion can only narrow authority:
	// the peer that made it already holds full local-admin rights through its
	// UID, and what it gets instead is the weaker remote-admin role under the
	// remote client's own name. The principal the request claimed is
	// overwritten either way, on both carriers.
	if request.RemoteAdmin != nil {
		if request.RemoteAdmin.Principal == "" || request.RemoteAdmin.RequestID == "" {
			_ = writeLocalResponse(conn, localResponse{Version: LocalTransportVersion, Error: "remote admin assertion requires a principal and a request id", ErrorClass: ClassAuthentication})
			return
		}
		principal = Principal{ID: "remote:" + request.RemoteAdmin.Principal, Roles: []string{RemoteAdminRole}}
		request.RemoteAdmin = nil
	}
	dispatch := adminDispatch{
		service:            s.Service,
		maxArtifactBytes:   s.MaxArtifactBytes,
		maxSubmissionBytes: s.MaxSubmissionBytes,
	}
	response, artifact := dispatch.handle(ctx, principal, request, conn)
	if artifact != nil {
		defer artifact.Content.Close()
		if err := writeLocalResponse(conn, response); err != nil {
			return
		}
		_, _ = io.CopyN(conn, artifact.Content, response.ArtifactSize)
		return
	}
	_ = writeLocalJSON(conn, response)
}

func readLocalJSON(reader io.Reader, maxBytes int64, destination any) error {
	var sizeBytes [4]byte
	if _, err := io.ReadFull(reader, sizeBytes[:]); err != nil {
		return fmt.Errorf("read local admin frame: %w", err)
	}
	size := int64(binary.BigEndian.Uint32(sizeBytes[:]))
	if size <= 0 || size > maxBytes {
		return errors.New("local admin frame exceeds configured limit")
	}
	decoder := json.NewDecoder(io.LimitReader(reader, size))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode local admin frame: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return fmt.Errorf("decode local admin frame trailing data: %w", err)
	}
	return nil
}

func writeLocalJSON(writer io.Writer, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(raw) > int(^uint32(0)) {
		return errors.New("local admin frame is too large")
	}
	var sizeBytes [4]byte
	binary.BigEndian.PutUint32(sizeBytes[:], uint32(len(raw)))
	buffered := bufio.NewWriter(writer)
	if _, err := buffered.Write(sizeBytes[:]); err != nil {
		return err
	}
	if _, err := buffered.Write(raw); err != nil {
		return err
	}
	return buffered.Flush()
}

func writeLocalResponse(writer io.Writer, response localResponse) error {
	return writeLocalJSON(writer, response)
}

// LocalClient sends coordinator admin requests without opening coordinator state.
type LocalClient struct {
	Path               string
	CoordinatorID      string
	MaxResponseBytes   int64
	MaxArtifactBytes   int64
	MaxSubmissionBytes int64
	RequestTimeout     time.Duration
}

// Describe reports the owner-only socket this client dials. The local carrier
// has no credential to leak here: file mode 0600 is its whole gate.
func (c LocalClient) Describe() TransportDescription {
	return TransportDescription{Carrier: CarrierLocal, CoordinatorID: c.CoordinatorID, Endpoint: c.Path}
}

func (c LocalClient) Query(ctx context.Context, query Query) (Response, error) {
	request := localRequest{Version: LocalTransportVersion, Operation: localOperationQuery, Query: &query}
	var response localResponse
	if err := c.call(ctx, request, &response); err != nil {
		return Response{}, err
	}
	if response.Response == nil {
		return Response{}, errors.New("local admin query returned no response")
	}
	return *response.Response, nil
}

func (c LocalClient) Mutate(ctx context.Context, mutation Mutation) (MutationResponse, error) {
	request := localRequest{Version: LocalTransportVersion, Operation: localOperationMutation, Mutation: &mutation}
	var response localResponse
	if err := c.call(ctx, request, &response); err != nil {
		return MutationResponse{}, err
	}
	if response.MutationResponse == nil {
		return MutationResponse{}, errors.New("local admin mutation returned no response")
	}
	return *response.MutationResponse, nil
}

func (c LocalClient) RecoverUnknown(ctx context.Context, _ Principal, request UnknownRecoveryRequest) (domain.UnknownAssignmentRecoveryDecision, error) {
	local := localRequest{Version: LocalTransportVersion, Operation: localOperationUnknownRecovery, UnknownRecovery: &request}
	var response localResponse
	if err := c.call(ctx, local, &response); err != nil {
		return domain.UnknownAssignmentRecoveryDecision{}, err
	}
	if response.UnknownRecoveryResponse == nil {
		return domain.UnknownAssignmentRecoveryDecision{}, errors.New("local unknown recovery returned no response")
	}
	return *response.UnknownRecoveryResponse, nil
}

func (c LocalClient) PutSchedule(
	ctx context.Context,
	_ Principal,
	definition LocalScheduleDefinitionRequest,
) (LocalScheduleDefinitionResponse, error) {
	request := localRequest{
		Version: LocalTransportVersion, Operation: localOperationScheduleDefinition,
		ScheduleDefinition: &definition,
	}
	var response localResponse
	if err := c.call(ctx, request, &response); err != nil {
		return LocalScheduleDefinitionResponse{}, err
	}
	if response.ScheduleDefinitionResponse == nil {
		return LocalScheduleDefinitionResponse{}, errors.New("local schedule definition returned no response")
	}
	return *response.ScheduleDefinitionResponse, nil
}

func (c LocalClient) SubmitArchive(
	ctx context.Context,
	request LocalSubmissionRequest,
	archive io.Reader,
	size int64,
) (LocalSubmissionResponse, error) {
	if archive == nil {
		return LocalSubmissionResponse{}, c.fail(ClassClientConfiguration, localOperationSubmission, errors.New("local submission archive is required"))
	}
	if c.MaxSubmissionBytes <= 0 || size <= 0 || size > c.MaxSubmissionBytes {
		return LocalSubmissionResponse{}, c.fail(ClassClientConfiguration, localOperationSubmission, errors.New("local submission archive exceeds configured limit"))
	}
	envelope := localRequest{
		Version: LocalTransportVersion, Operation: localOperationSubmission,
		Submission: &request, SubmissionSize: size,
	}
	conn, err := c.dialOperation(ctx, localOperationSubmission)
	if err != nil {
		return LocalSubmissionResponse{}, err
	}
	defer conn.Close()
	stopWatch := watchConnection(ctx, conn)
	defer stopWatch()
	if err := writeLocalJSON(conn, envelope); err != nil {
		return LocalSubmissionResponse{}, c.fail(ClassUnavailable, localOperationSubmission, fmt.Errorf("write local submission request: %w", err))
	}
	if _, err := io.CopyN(conn, archive, size); err != nil {
		return LocalSubmissionResponse{}, c.fail(ClassUnavailable, localOperationSubmission, fmt.Errorf("write local submission archive: %w", err))
	}
	var response localResponse
	if err := readLocalJSON(conn, c.MaxResponseBytes, &response); err != nil {
		return LocalSubmissionResponse{}, c.fail(ClassProtocol, localOperationSubmission, err)
	}
	if err := c.validate(localOperationSubmission, response); err != nil {
		return LocalSubmissionResponse{}, err
	}
	if response.SubmissionResponse == nil {
		return LocalSubmissionResponse{}, c.fail(ClassProtocol, localOperationSubmission, errors.New("local submission returned no response"))
	}
	return *response.SubmissionResponse, nil
}

func (c LocalClient) OpenArtifact(ctx context.Context, _ Principal, artifactID string) (ArtifactContent, error) {
	request := localRequest{Version: LocalTransportVersion, Operation: localOperationArtifact, ArtifactID: artifactID}
	conn, err := c.dialOperation(ctx, localOperationArtifact)
	if err != nil {
		return ArtifactContent{}, err
	}
	stopWatch := watchConnection(ctx, conn)
	closeOnError := func() {
		stopWatch()
		_ = conn.Close()
	}
	if err := writeLocalJSON(conn, request); err != nil {
		closeOnError()
		return ArtifactContent{}, c.fail(ClassUnavailable, localOperationArtifact, fmt.Errorf("write local admin request: %w", err))
	}
	var response localResponse
	if err := readLocalJSON(conn, c.MaxResponseBytes, &response); err != nil {
		closeOnError()
		return ArtifactContent{}, c.fail(ClassProtocol, localOperationArtifact, err)
	}
	if err := c.validate(localOperationArtifact, response); err != nil {
		closeOnError()
		return ArtifactContent{}, err
	}
	if response.ArtifactMetadata == nil || response.ArtifactSize < 0 ||
		response.ArtifactSize != response.ArtifactMetadata.Size ||
		response.ArtifactSize > c.MaxArtifactBytes {
		closeOnError()
		return ArtifactContent{}, c.fail(ClassProtocol, localOperationArtifact, errors.New("invalid local admin artifact response"))
	}
	return ArtifactContent{Metadata: *response.ArtifactMetadata, Content: &exactReadCloser{
		reader: conn, closer: conn, remaining: response.ArtifactSize, stopWatch: stopWatch,
	}}, nil
}

// exchange runs one prepared request over the local socket without choosing an
// operation of its own. The restricted SSH command uses it to relay a verified
// remote request: body carries the submission archive, and the returned stream
// is the artifact content the caller must read and close.
//
// A refusal the coordinator wrote is returned inside the response, not as an
// error, so the relay can hand it back to the remote client verbatim.
func (c LocalClient) exchange(ctx context.Context, request localRequest, body io.Reader) (localResponse, io.ReadCloser, error) {
	conn, err := c.dialOperation(ctx, request.Operation)
	if err != nil {
		return localResponse{}, nil, err
	}
	stopWatch := watchConnection(ctx, conn)
	closeAll := func() {
		stopWatch()
		_ = conn.Close()
	}
	if err := writeLocalJSON(conn, request); err != nil {
		closeAll()
		return localResponse{}, nil, c.fail(ClassUnavailable, request.Operation, fmt.Errorf("write local admin request: %w", err))
	}
	if request.Operation == localOperationSubmission && request.SubmissionSize > 0 {
		if body == nil {
			closeAll()
			return localResponse{}, nil, c.fail(ClassClientConfiguration, request.Operation, errors.New("local submission archive is required"))
		}
		if _, err := io.CopyN(conn, body, request.SubmissionSize); err != nil {
			closeAll()
			return localResponse{}, nil, c.fail(ClassUnavailable, request.Operation, fmt.Errorf("write local submission archive: %w", err))
		}
	}
	var response localResponse
	if err := readLocalJSON(conn, c.MaxResponseBytes, &response); err != nil {
		closeAll()
		return localResponse{}, nil, c.fail(ClassProtocol, request.Operation, err)
	}
	if response.Error != "" || request.Operation != localOperationArtifact {
		closeAll()
		return response, nil, nil
	}
	if response.ArtifactMetadata == nil || response.ArtifactSize < 0 ||
		response.ArtifactSize != response.ArtifactMetadata.Size ||
		response.ArtifactSize > c.MaxArtifactBytes {
		closeAll()
		return localResponse{}, nil, c.fail(ClassProtocol, request.Operation, errors.New("invalid local admin artifact response"))
	}
	return response, &exactReadCloser{
		reader: conn, closer: conn, remaining: response.ArtifactSize, stopWatch: stopWatch,
	}, nil
}

func (c LocalClient) call(ctx context.Context, request localRequest, destination *localResponse) error {
	conn, err := c.dial(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	stopWatch := watchConnection(ctx, conn)
	defer stopWatch()
	if err := writeLocalJSON(conn, request); err != nil {
		return c.fail(ClassUnavailable, request.Operation, fmt.Errorf("write local admin request: %w", err))
	}
	if err := readLocalJSON(conn, c.MaxResponseBytes, destination); err != nil {
		return c.fail(ClassProtocol, request.Operation, err)
	}
	return c.validate(request.Operation, *destination)
}

func (c LocalClient) dial(ctx context.Context) (net.Conn, error) {
	return c.dialOperation(ctx, "")
}

func (c LocalClient) dialOperation(ctx context.Context, operation string) (net.Conn, error) {
	if strings.TrimSpace(c.Path) != c.Path || c.Path == "" || !filepath.IsAbs(c.Path) {
		return nil, c.fail(ClassClientConfiguration, operation, errors.New("local admin socket path must be absolute and trimmed"))
	}
	if c.MaxResponseBytes <= 0 || c.MaxArtifactBytes <= 0 || c.RequestTimeout <= 0 {
		return nil, c.fail(ClassClientConfiguration, operation, errors.New("local admin client byte and timeout limits must be positive"))
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", c.Path)
	if err != nil {
		return nil, c.fail(ClassUnavailable, operation, fmt.Errorf("connect to backlog-v2 coordinator: %w", err))
	}
	if err := conn.SetDeadline(time.Now().Add(c.RequestTimeout)); err != nil {
		_ = conn.Close()
		return nil, c.fail(ClassClientConfiguration, operation, fmt.Errorf("bound local admin request: %w", err))
	}
	return conn, nil
}

func (c LocalClient) fail(class TransportClass, operation string, err error) error {
	return classify(class, operation, c.CoordinatorID, err)
}

// validate turns a decoded response into a classified error. A version
// mismatch is a protocol failure; a refusal the coordinator itself wrote is a
// rejection, except when the coordinator refused the principal.
func (c LocalClient) validate(operation string, response localResponse) error {
	if response.Version != LocalTransportVersion {
		return c.fail(ClassProtocol, operation, errors.New("invalid local admin response version"))
	}
	if response.Error == "" {
		return nil
	}
	class := response.ErrorClass
	if class == "" {
		class = ClassRejected
	}
	return c.fail(class, operation, errors.New(response.Error))
}

func watchConnection(ctx context.Context, conn io.Closer) func() {
	done := make(chan struct{})
	var once sync.Once
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	return func() {
		once.Do(func() {
			close(done)
		})
	}
}

type exactReadCloser struct {
	reader    io.Reader
	closer    io.Closer
	remaining int64
	stopWatch func()
}

func (r *exactReadCloser) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.reader.Read(p)
	r.remaining -= int64(n)
	if errors.Is(err, io.EOF) && r.remaining > 0 {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

func (r *exactReadCloser) Close() error {
	if r.stopWatch != nil {
		r.stopWatch()
	}
	return r.closer.Close()
}
