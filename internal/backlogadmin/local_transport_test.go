package backlogadmin

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type localTransportService struct {
	mu                  sync.Mutex
	queryPrincipal      Principal
	mutationPrincipal   Principal
	artifactPrincipal   Principal
	submissionPrincipal Principal
	schedulePrincipal   Principal
	recoveryPrincipal   Principal
	submissionRequest   LocalSubmissionRequest
	submissionArchive   []byte
	scheduleRequest     LocalScheduleDefinitionRequest
	recoveryRequest     UnknownRecoveryRequest
}

func (s *localTransportService) Query(_ context.Context, query Query) (Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queryPrincipal = query.Principal
	return Response{Version: Version, Kind: query.Kind}, nil
}

func (s *localTransportService) Mutate(_ context.Context, mutation Mutation) (MutationResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mutationPrincipal = mutation.Principal
	return MutationResponse{Version: Version, Command: Command{ID: mutation.ID}}, nil
}

func (s *localTransportService) OpenArtifact(_ context.Context, principal Principal, artifactID string) (ArtifactContent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.artifactPrincipal = principal
	raw := []byte("artifact:" + artifactID)
	return ArtifactContent{
		Metadata: ArtifactMetadata{ID: artifactID, Size: int64(len(raw))},
		Content:  io.NopCloser(bytes.NewReader(raw)),
	}, nil
}

func (s *localTransportService) SubmitArchive(
	_ context.Context,
	principal Principal,
	request LocalSubmissionRequest,
	archive io.Reader,
) (LocalSubmissionResponse, error) {
	raw, err := io.ReadAll(archive)
	if err != nil {
		return LocalSubmissionResponse{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.submissionPrincipal = principal
	s.submissionRequest = request
	s.submissionArchive = raw
	return LocalSubmissionResponse{
		Key: request.IdempotencyKey, Digest: "digest", WorkflowID: "workflow-1",
		RunID: "run-1", State: "accepted", AcceptedAt: "2026-09-10T12:00:00Z",
	}, nil
}

func (s *localTransportService) PutSchedule(
	_ context.Context,
	principal Principal,
	request LocalScheduleDefinitionRequest,
) (LocalScheduleDefinitionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schedulePrincipal = principal
	s.scheduleRequest = request
	return LocalScheduleDefinitionResponse{
		Schedule: domain.Schedule{
			ID: request.ID, Name: request.Name, WorkflowID: request.WorkflowID,
			Expression: request.Expression, Timezone: request.Timezone,
			AfterFailure: request.AfterFailure, Enabled: request.Enabled,
			Revision: request.ExpectedRevision + 1,
		},
	}, nil
}

func (s *localTransportService) RecoverUnknown(
	_ context.Context,
	principal Principal,
	request UnknownRecoveryRequest,
) (domain.UnknownAssignmentRecoveryDecision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recoveryPrincipal = principal
	s.recoveryRequest = request
	return domain.UnknownAssignmentRecoveryDecision{
		Recovery: domain.UnknownAssignmentRecovery{
			ID: request.ID, AssignmentID: request.AssignmentID, Outcome: request.Outcome,
		},
		Assignment: domain.Assignment{ID: request.AssignmentID},
	}, nil
}

func startLocalTransport(t *testing.T, allowedUID uint32, service LocalService) (LocalClient, context.CancelFunc, <-chan error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "admin.sock")
	listener, err := ListenLocal(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	server := &LocalServer{
		Listener: listener, Service: service, AllowedUID: allowedUID,
		MaxRequestBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
		RequestTimeout: time.Second, MaxConcurrent: 8,
	}
	go func() { done <- server.Serve(ctx) }()
	client := LocalClient{
		Path: path, MaxResponseBytes: 1 << 20,
		MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
		RequestTimeout: time.Second,
	}
	return client, cancel, done
}

func stopLocalTransport(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("local transport did not stop")
	}
}

func TestLocalTransportAuthenticatesPeerAndIgnoresClaimedPrincipal(t *testing.T) {
	service := &localTransportService{}
	client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), service)
	defer stopLocalTransport(t, cancel, done)

	ctx := context.Background()
	spoofed := Principal{ID: "remote:spoofed", Roles: []string{"root"}}
	if _, err := client.Query(ctx, Query{Version: Version, Kind: QueryStatus, Principal: spoofed}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Mutate(ctx, Mutation{
		Version: Version, Principal: spoofed, ID: "command-1", Kind: "pause",
		WorkflowRunID: "run-1", ExpectedRevision: 1, Reason: "test",
	}); err != nil {
		t.Fatal(err)
	}
	scheduleRequest := LocalScheduleDefinitionRequest{
		RequestID: "schedule-request-1", ID: "nightly", Name: "Nightly",
		WorkflowID: "workflow-1", Expression: "0 2 * * *", Timezone: "UTC",
		AfterFailure: domain.ScheduleFailureHold, Enabled: true, Reason: "create",
	}
	schedule, err := client.PutSchedule(ctx, spoofed, scheduleRequest)
	if err != nil {
		t.Fatal(err)
	}
	if schedule.Schedule.ID != "nightly" || schedule.Schedule.Revision != 1 {
		t.Fatalf("schedule response = %+v", schedule)
	}
	recoveryRequest := UnknownRecoveryRequest{
		ID: "recovery-1", AssignmentID: "assignment-1", CoordinatorEpoch: 2,
		ExpectedAssignmentEpoch: 3, ExpectedAttemptRevision: 4,
		Outcome: domain.UnknownRecoveryStopped, EvidenceID: "incident-1",
		EvidenceSHA256: strings.Repeat("a", 64), Reason: "verified stopped",
	}
	recovery, err := client.RecoverUnknown(ctx, spoofed, recoveryRequest)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.Recovery.ID != "recovery-1" || recovery.Assignment.ID != "assignment-1" {
		t.Fatalf("recovery response = %+v", recovery)
	}
	submissionRaw := []byte("submission archive")
	submission, err := client.SubmitArchive(
		ctx,
		LocalSubmissionRequest{IdempotencyKey: "submission-1"},
		bytes.NewReader(submissionRaw),
		int64(len(submissionRaw)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if submission.Key != "submission-1" || submission.WorkflowID != "workflow-1" {
		t.Fatalf("submission response = %+v", submission)
	}
	content, err := client.OpenArtifact(ctx, spoofed, "artifact-1")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(content.Content)
	if err != nil {
		t.Fatal(err)
	}
	if err := content.Content.Close(); err != nil {
		t.Fatal(err)
	}
	if got := string(raw); got != "artifact:artifact-1" {
		t.Fatalf("artifact = %q", got)
	}

	wantID := "local:" + strings.TrimSpace(strings.TrimPrefix(service.queryPrincipal.ID, "local:"))
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.submissionRequest.IdempotencyKey != "submission-1" ||
		!bytes.Equal(service.submissionArchive, submissionRaw) {
		t.Fatalf("submission request = %+v, archive = %q", service.submissionRequest, service.submissionArchive)
	}
	if service.scheduleRequest != scheduleRequest {
		t.Fatalf("schedule request = %+v, want %+v", service.scheduleRequest, scheduleRequest)
	}
	if service.recoveryRequest != recoveryRequest {
		t.Fatalf("recovery request = %+v, want %+v", service.recoveryRequest, recoveryRequest)
	}
	for label, principal := range map[string]Principal{
		"query": service.queryPrincipal, "mutation": service.mutationPrincipal,
		"artifact": service.artifactPrincipal, "submission": service.submissionPrincipal,
		"schedule": service.schedulePrincipal,
		"recovery": service.recoveryPrincipal,
	} {
		if principal.ID != wantID || principal.ID == spoofed.ID || len(principal.Roles) != 1 || principal.Roles[0] != "local-admin" {
			t.Fatalf("%s principal = %+v", label, principal)
		}
	}
}

func TestLocalTransportRejectsUnauthorizedUIDBeforeService(t *testing.T) {
	service := &localTransportService{}
	client, cancel, done := startLocalTransport(t, uint32(os.Getuid()+1), service)
	defer stopLocalTransport(t, cancel, done)

	_, err := client.Query(context.Background(), Query{Version: Version, Kind: QueryStatus})
	if err == nil || !strings.Contains(err.Error(), "uid is not authorized") {
		t.Fatalf("error = %v", err)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.queryPrincipal.ID != "" {
		t.Fatalf("service was reached with %+v", service.queryPrincipal)
	}
}

func TestLocalTransportRejectsOversizedAndMalformedFrames(t *testing.T) {
	service := &localTransportService{}
	client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), service)
	defer stopLocalTransport(t, cancel, done)

	conn, err := net.Dial("unix", client.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	frame := make([]byte, 4)
	frame[3] = 1
	if _, err := conn.Write(frame); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("{")); err != nil {
		t.Fatal(err)
	}
	var response localResponse
	if err := readLocalJSON(conn, 1<<20, &response); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(response.Error, "decode local admin frame") {
		t.Fatalf("response = %+v", response)
	}

	tiny := client
	tiny.MaxResponseBytes = 1
	if _, err := tiny.Query(context.Background(), Query{Version: Version, Kind: QueryStatus}); err == nil ||
		!strings.Contains(err.Error(), "exceeds configured limit") {
		t.Fatalf("response limit error = %v", err)
	}
}

func TestListenLocalProtectsPathAndRefusesNonSocket(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "admin.sock")
	listener, err := ListenLocal(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 || info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("mode = %v", info.Mode())
	}
	if _, err := ListenLocal(path); err == nil || !strings.Contains(err.Error(), "already accepting") {
		t.Fatalf("second listener error = %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	regular := filepath.Join(root, "regular")
	if err := os.WriteFile(regular, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ListenLocal(regular); err == nil || !strings.Contains(err.Error(), "not a socket") {
		t.Fatalf("regular path error = %v", err)
	}
	raw, err := os.ReadFile(regular)
	if err != nil || string(raw) != "keep" {
		t.Fatalf("regular file changed: %q %v", raw, err)
	}
}

func TestLocalTransportShutdownClosesIdleConnection(t *testing.T) {
	service := &localTransportService{}
	client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), service)
	conn, err := net.Dial("unix", client.Path)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer conn.Close()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server shutdown blocked on idle connection")
	}
}

func TestLocalTransportBoundsIdleClientsAndBackpressure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admin.sock")
	listener, err := ListenLocal(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	server := &LocalServer{
		Listener: listener, Service: &localTransportService{}, AllowedUID: uint32(os.Getuid()),
		MaxRequestBytes: 1024, MaxArtifactBytes: 1024, MaxSubmissionBytes: 1024,
		RequestTimeout: 150 * time.Millisecond, MaxConcurrent: 1,
	}
	go func() { done <- server.Serve(ctx) }()
	defer stopLocalTransport(t, cancel, done)

	idle, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	if _, err := idle.Write([]byte{0, 0, 0, 100, '{'}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)

	rejected, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer rejected.Close()
	if err := rejected.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var response localResponse
	if err := readLocalJSON(rejected, 1024, &response); err != nil {
		t.Fatalf("read backpressure response: %v", err)
	}
	if !strings.Contains(response.Error, "backpressure") {
		t.Fatalf("backpressure response = %+v", response)
	}
	if err := idle.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err := idle.Read(one[:]); err == nil {
		t.Fatal("idle connection survived request timeout")
	}
}

func TestLocalSubmissionTransportEnforcesDeclaredSize(t *testing.T) {
	service := &localTransportService{}
	client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), service)
	defer stopLocalTransport(t, cancel, done)

	oversized := client
	oversized.MaxSubmissionBytes = 3
	if _, err := oversized.SubmitArchive(
		context.Background(),
		LocalSubmissionRequest{IdempotencyKey: "large"},
		strings.NewReader("four"),
		4,
	); err == nil || !strings.Contains(err.Error(), "exceeds configured limit") {
		t.Fatalf("oversized error = %v", err)
	}

	if _, err := client.SubmitArchive(
		context.Background(),
		LocalSubmissionRequest{IdempotencyKey: "short"},
		strings.NewReader("short"),
		10,
	); err == nil || !strings.Contains(err.Error(), "write local submission archive") {
		t.Fatalf("short archive error = %v", err)
	}
}

func TestLocalSubmissionTransportRejectsOversizedHeaderBeforeReadingBody(t *testing.T) {
	service := &localTransportService{}
	client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), service)
	defer stopLocalTransport(t, cancel, done)

	conn, err := net.Dial("unix", client.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := writeLocalJSON(conn, localRequest{
		Version: LocalTransportVersion, Operation: localOperationSubmission,
		Submission:     &LocalSubmissionRequest{IdempotencyKey: "oversized"},
		SubmissionSize: client.MaxSubmissionBytes + 1,
	}); err != nil {
		t.Fatal(err)
	}
	var response localResponse
	if err := readLocalJSON(conn, client.MaxResponseBytes, &response); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(response.Error, "oversized") {
		t.Fatalf("response = %+v", response)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.submissionPrincipal.ID != "" || len(service.submissionArchive) != 0 {
		t.Fatalf("oversized request reached service: %+v", service)
	}
}

func TestReadLocalJSONRejectsTrailingFrameData(t *testing.T) {
	raw := []byte(`{"version":"backlog.admin.local/v1"}{}`)
	frame := make([]byte, 4+len(raw))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(raw)))
	copy(frame[4:], raw)
	var request localRequest
	err := readLocalJSON(bytes.NewReader(frame), 1<<20, &request)
	if err == nil || !strings.Contains(err.Error(), "trailing data") {
		t.Fatalf("trailing frame error = %v", err)
	}
}
