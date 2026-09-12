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
)

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

type localRequest struct {
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
	Version                    string                                    `json:"version"`
	Response                   *Response                                 `json:"response,omitempty"`
	MutationResponse           *MutationResponse                         `json:"mutationResponse,omitempty"`
	ArtifactMetadata           *ArtifactMetadata                         `json:"artifactMetadata,omitempty"`
	ArtifactSize               int64                                     `json:"artifactSize,omitempty"`
	SubmissionResponse         *LocalSubmissionResponse                  `json:"submissionResponse,omitempty"`
	ScheduleDefinitionResponse *LocalScheduleDefinitionResponse          `json:"scheduleDefinitionResponse,omitempty"`
	UnknownRecoveryResponse    *domain.UnknownAssignmentRecoveryDecision `json:"unknownRecoveryResponse,omitempty"`
	Error                      string                                    `json:"error,omitempty"`
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
		_ = writeLocalResponse(conn, localResponse{Version: LocalTransportVersion, Error: err.Error()})
		return
	}
	principal, err := localPeerPrincipal(conn, s.AllowedUID)
	if err != nil {
		_ = writeLocalResponse(conn, localResponse{Version: LocalTransportVersion, Error: err.Error()})
		return
	}
	if request.Version != LocalTransportVersion {
		_ = writeLocalResponse(conn, localResponse{Version: LocalTransportVersion, Error: "unsupported local admin transport version"})
		return
	}
	response := localResponse{Version: LocalTransportVersion}
	switch request.Operation {
	case localOperationQuery:
		if request.Query == nil || request.Mutation != nil || request.ArtifactID != "" ||
			request.Submission != nil || request.SubmissionSize != 0 || request.ScheduleDefinition != nil || request.UnknownRecovery != nil {
			response.Error = "malformed local admin query"
			break
		}
		request.Query.Principal = principal
		value, queryErr := s.Service.Query(ctx, *request.Query)
		if queryErr != nil {
			response.Error = queryErr.Error()
		} else {
			response.Response = &value
		}
	case localOperationMutation:
		if request.Mutation == nil || request.Query != nil || request.ArtifactID != "" ||
			request.Submission != nil || request.SubmissionSize != 0 || request.ScheduleDefinition != nil || request.UnknownRecovery != nil {
			response.Error = "malformed local admin mutation"
			break
		}
		request.Mutation.Principal = principal
		value, mutationErr := s.Service.Mutate(ctx, *request.Mutation)
		if mutationErr != nil {
			response.Error = mutationErr.Error()
		} else {
			response.MutationResponse = &value
		}
	case localOperationArtifact:
		if request.ArtifactID == "" || request.Query != nil || request.Mutation != nil ||
			request.Submission != nil || request.SubmissionSize != 0 || request.ScheduleDefinition != nil || request.UnknownRecovery != nil {
			response.Error = "malformed local admin artifact request"
			break
		}
		value, artifactErr := s.Service.OpenArtifact(ctx, principal, request.ArtifactID)
		if artifactErr != nil {
			response.Error = artifactErr.Error()
			break
		}
		defer value.Content.Close()
		if value.Metadata.Size < 0 || value.Metadata.Size > s.MaxArtifactBytes {
			response.Error = "artifact exceeds local transport limit"
			break
		}
		response.ArtifactMetadata = &value.Metadata
		response.ArtifactSize = value.Metadata.Size
		if err := writeLocalResponse(conn, response); err != nil {
			return
		}
		_, _ = io.CopyN(conn, value.Content, value.Metadata.Size)
		return
	case localOperationSubmission:
		if request.Submission == nil || request.Query != nil || request.Mutation != nil ||
			request.ArtifactID != "" || request.SubmissionSize <= 0 ||
			request.SubmissionSize > s.MaxSubmissionBytes || request.ScheduleDefinition != nil || request.UnknownRecovery != nil {
			response.Error = "malformed or oversized local submission request"
			break
		}
		archive := &io.LimitedReader{R: conn, N: request.SubmissionSize}
		value, submissionErr := s.Service.SubmitArchive(ctx, principal, *request.Submission, archive)
		if submissionErr != nil {
			response.Error = submissionErr.Error()
		} else if archive.N != 0 {
			response.Error = "submission archive ended before its declared size"
		} else {
			response.SubmissionResponse = &value
		}
	case localOperationScheduleDefinition:
		if request.ScheduleDefinition == nil || request.Query != nil || request.Mutation != nil ||
			request.ArtifactID != "" || request.Submission != nil || request.SubmissionSize != 0 || request.UnknownRecovery != nil {
			response.Error = "malformed local schedule definition request"
			break
		}
		value, definitionErr := s.Service.PutSchedule(ctx, principal, *request.ScheduleDefinition)
		if definitionErr != nil {
			response.Error = definitionErr.Error()
		} else {
			response.ScheduleDefinitionResponse = &value
		}
	case localOperationUnknownRecovery:
		if request.UnknownRecovery == nil || request.Query != nil || request.Mutation != nil ||
			request.ArtifactID != "" || request.Submission != nil || request.SubmissionSize != 0 || request.ScheduleDefinition != nil {
			response.Error = "malformed local unknown recovery request"
			break
		}
		value, recoveryErr := s.Service.RecoverUnknown(ctx, principal, *request.UnknownRecovery)
		if recoveryErr != nil {
			response.Error = recoveryErr.Error()
		} else {
			response.UnknownRecoveryResponse = &value
		}
	default:
		response.Error = "unknown local admin operation"
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
	MaxResponseBytes   int64
	MaxArtifactBytes   int64
	MaxSubmissionBytes int64
	RequestTimeout     time.Duration
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
		return LocalSubmissionResponse{}, errors.New("local submission archive is required")
	}
	if c.MaxSubmissionBytes <= 0 || size <= 0 || size > c.MaxSubmissionBytes {
		return LocalSubmissionResponse{}, errors.New("local submission archive exceeds configured limit")
	}
	envelope := localRequest{
		Version: LocalTransportVersion, Operation: localOperationSubmission,
		Submission: &request, SubmissionSize: size,
	}
	conn, err := c.dial(ctx)
	if err != nil {
		return LocalSubmissionResponse{}, err
	}
	defer conn.Close()
	stopWatch := watchConnection(ctx, conn)
	defer stopWatch()
	if err := writeLocalJSON(conn, envelope); err != nil {
		return LocalSubmissionResponse{}, fmt.Errorf("write local submission request: %w", err)
	}
	if _, err := io.CopyN(conn, archive, size); err != nil {
		return LocalSubmissionResponse{}, fmt.Errorf("write local submission archive: %w", err)
	}
	var response localResponse
	if err := readLocalJSON(conn, c.MaxResponseBytes, &response); err != nil {
		return LocalSubmissionResponse{}, err
	}
	if err := validateLocalResponse(response); err != nil {
		return LocalSubmissionResponse{}, err
	}
	if response.SubmissionResponse == nil {
		return LocalSubmissionResponse{}, errors.New("local submission returned no response")
	}
	return *response.SubmissionResponse, nil
}

func (c LocalClient) OpenArtifact(ctx context.Context, _ Principal, artifactID string) (ArtifactContent, error) {
	request := localRequest{Version: LocalTransportVersion, Operation: localOperationArtifact, ArtifactID: artifactID}
	conn, err := c.dial(ctx)
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
		return ArtifactContent{}, fmt.Errorf("write local admin request: %w", err)
	}
	var response localResponse
	if err := readLocalJSON(conn, c.MaxResponseBytes, &response); err != nil {
		closeOnError()
		return ArtifactContent{}, err
	}
	if err := validateLocalResponse(response); err != nil {
		closeOnError()
		return ArtifactContent{}, err
	}
	if response.ArtifactMetadata == nil || response.ArtifactSize < 0 ||
		response.ArtifactSize != response.ArtifactMetadata.Size ||
		response.ArtifactSize > c.MaxArtifactBytes {
		closeOnError()
		return ArtifactContent{}, errors.New("invalid local admin artifact response")
	}
	return ArtifactContent{Metadata: *response.ArtifactMetadata, Content: &exactReadCloser{
		reader: conn, closer: conn, remaining: response.ArtifactSize, stopWatch: stopWatch,
	}}, nil
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
		return fmt.Errorf("write local admin request: %w", err)
	}
	if err := readLocalJSON(conn, c.MaxResponseBytes, destination); err != nil {
		return err
	}
	return validateLocalResponse(*destination)
}

func (c LocalClient) dial(ctx context.Context) (net.Conn, error) {
	if strings.TrimSpace(c.Path) != c.Path || c.Path == "" || !filepath.IsAbs(c.Path) {
		return nil, errors.New("local admin socket path must be absolute and trimmed")
	}
	if c.MaxResponseBytes <= 0 || c.MaxArtifactBytes <= 0 || c.RequestTimeout <= 0 {
		return nil, errors.New("local admin client byte and timeout limits must be positive")
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", c.Path)
	if err != nil {
		return nil, fmt.Errorf("connect to backlog-v2 coordinator: %w", err)
	}
	if err := conn.SetDeadline(time.Now().Add(c.RequestTimeout)); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("bound local admin request: %w", err)
	}
	return conn, nil
}

func validateLocalResponse(response localResponse) error {
	if response.Version != LocalTransportVersion {
		return errors.New("invalid local admin response version")
	}
	if response.Error != "" {
		return errors.New(response.Error)
	}
	return nil
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
