package backlogadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// testAdminCredentials is one fixed admin identity shared by the parent test
// and its helper process.
func testAdminCredentials() AdminCredentials {
	return AdminCredentials{
		ClientPrincipal:      "admin:omarchy-pc",
		ClientKeyID:          "admin-key-1",
		ClientSecret:         []byte("0123456789abcdef-client"),
		CoordinatorPrincipal: "coordinator:normandy",
		CoordinatorKeyID:     "coordinator-key-1",
		CoordinatorSecret:    []byte("0123456789abcdef-coord"),
	}
}

// testWorkerSecret stands in for a worker protocol credential, which must never
// be usable as an admin credential.
var testWorkerSecret = []byte("0123456789abcdef-worker")

const testCoordinatorID = "normandy-coordinator"

// teachesRemoteShell reports whether a message would teach an agent to run
// "ssh <coordinator> t3-steward ...", which is the authority story this
// transport exists to remove. No help text, example or error may do it.
func teachesRemoteShell(message string) bool {
	return strings.Contains(message, "ssh ") && strings.Contains(message, "t3-steward")
}

// remoteFakeService records the principal every operation ran under and counts
// the submissions it accepted, which is how the replay tests prove that a
// repeated request produced exactly one run.
type remoteFakeService struct {
	mu                sync.Mutex
	principals        []Principal
	submissions       int
	submittedArchives []string
	artifact          []byte
}

func (s *remoteFakeService) record(principal Principal) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.principals = append(s.principals, principal)
}

func (s *remoteFakeService) lastPrincipal() Principal {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.principals) == 0 {
		return Principal{}
	}
	return s.principals[len(s.principals)-1]
}

func (s *remoteFakeService) Query(_ context.Context, query Query) (Response, error) {
	s.record(query.Principal)
	return Response{Version: Version, Kind: query.Kind, Status: &Status{
		Runtime: RuntimeStatus{Owner: "igor", Epoch: 7, Health: "ready", Release: "0.11.0"},
	}}, nil
}

func (s *remoteFakeService) Mutate(_ context.Context, mutation Mutation) (MutationResponse, error) {
	s.record(mutation.Principal)
	return MutationResponse{Version: Version, Command: Command{ID: mutation.ID}}, nil
}

func (s *remoteFakeService) OpenArtifact(_ context.Context, principal Principal, artifactID string) (ArtifactContent, error) {
	s.record(principal)
	raw := s.artifact
	if raw == nil {
		raw = []byte("artifact:" + artifactID)
	}
	return ArtifactContent{
		Metadata: ArtifactMetadata{ID: artifactID, Size: int64(len(raw))},
		Content:  io.NopCloser(bytes.NewReader(raw)),
	}, nil
}

func (s *remoteFakeService) SubmitArchive(_ context.Context, principal Principal, request LocalSubmissionRequest, archive io.Reader) (LocalSubmissionResponse, error) {
	raw, err := io.ReadAll(archive)
	if err != nil {
		return LocalSubmissionResponse{}, err
	}
	s.record(principal)
	s.mu.Lock()
	s.submissions++
	s.submittedArchives = append(s.submittedArchives, string(raw))
	runID := fmt.Sprintf("run-%d", s.submissions)
	s.mu.Unlock()
	return LocalSubmissionResponse{
		Key: request.IdempotencyKey, Digest: "digest", WorkflowID: "workflow-1",
		RunID: runID, State: "accepted", AcceptedAt: "2026-09-14T12:00:00Z",
	}, nil
}

func (s *remoteFakeService) PutSchedule(_ context.Context, principal Principal, request LocalScheduleDefinitionRequest) (LocalScheduleDefinitionResponse, error) {
	s.record(principal)
	return LocalScheduleDefinitionResponse{Schedule: domain.Schedule{ID: request.ID}}, nil
}

func (s *remoteFakeService) RecoverUnknown(_ context.Context, principal Principal, request UnknownRecoveryRequest) (domain.UnknownAssignmentRecoveryDecision, error) {
	s.record(principal)
	return domain.UnknownAssignmentRecoveryDecision{
		Recovery: domain.UnknownAssignmentRecovery{ID: request.ID},
	}, nil
}

// The optional operations the dispatch reaches through interface assertions.

func (s *remoteFakeService) NodeWait(_ context.Context, principal Principal, op NodeWaitOperation) (NodeWaitResponse, error) {
	s.record(principal)
	return NodeWaitResponse{Waits: []domain.NodeWait{{Request: domain.NodeWaitRequest{ID: op.Request.ID}}}}, nil
}

func (s *remoteFakeService) AmendGraph(_ context.Context, principal Principal, amendment domain.GraphAmendment) (domain.GraphAmendmentResult, error) {
	s.record(principal)
	return domain.GraphAmendmentResult{Run: domain.WorkflowRun{GraphRevision: amendment.ExpectedRevision + 1}}, nil
}

func (s *remoteFakeService) EnrollWorker(_ context.Context, principal Principal, request domain.WorkerEnrollmentRequest) (domain.WorkerEnrollment, error) {
	s.record(principal)
	return domain.WorkerEnrollment{Request: request, Actor: principal.ID}, nil
}

// Environment names the helper process reads. The helper is this test binary
// re-executed, which is how the SSH hop is simulated without an ssh daemon.
const (
	helperEnabled     = "T3_STEWARD_TEST_COORDINATOR_EXCHANGE"
	helperSocket      = "T3_STEWARD_TEST_ADMIN_SOCKET"
	helperReplayRoot  = "T3_STEWARD_TEST_ADMIN_REPLAY"
	helperOperation   = "T3_STEWARD_TEST_ADMIN_OPERATION"
	helperCoordinator = "T3_STEWARD_TEST_ADMIN_COORDINATOR"
)

// TestCoordinatorExchangeHelperProcess is not a test. It is the coordinator
// side of one exchange, run as a child process so that the client really
// speaks to a separate process over pipes, as it does over SSH.
func TestCoordinatorExchangeHelperProcess(t *testing.T) {
	if os.Getenv(helperEnabled) != "1" {
		t.Skip("helper process")
	}
	var replay *RemoteReplayStore
	if root := os.Getenv(helperReplayRoot); root != "" {
		store, err := OpenRemoteReplayStore(root, os.Getenv(helperCoordinator))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(3)
		}
		replay = store
	}
	server, err := NewRemoteServer(RemoteServerConfig{
		CoordinatorID: os.Getenv(helperCoordinator),
		Clients:       map[string]AdminCredentials{testAdminCredentials().ClientPrincipal: testAdminCredentials()},
		Replay:        replay,
		Relay: LocalClient{
			Path: os.Getenv(helperSocket), CoordinatorID: os.Getenv(helperCoordinator),
			MaxResponseBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
			RequestTimeout: 10 * time.Second,
		},
		MaxRequestBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	if err := server.Serve(context.Background(), os.Getenv(helperOperation), os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(4)
	}
	os.Exit(0)
}

// remoteHarness wires a real local admin socket, a helper coordinator process
// and an SSH client whose command factory records the argv it was given.
type remoteHarness struct {
	client  *SSHClient
	service *remoteFakeService
	argv    *[][]string
}

func newRemoteHarness(t *testing.T, replay bool) remoteHarness {
	t.Helper()
	service := &remoteFakeService{}
	socketPath := filepath.Join(shortTempRoot(t), "admin.sock")
	listener, err := ListenLocal(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	server := &LocalServer{
		Listener: listener, Service: service, AllowedUID: uint32(os.Getuid()),
		MaxRequestBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
		RequestTimeout: 10 * time.Second, MaxConcurrent: 8,
	}
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() { stopLocalTransport(t, cancel, done) })

	replayRoot := ""
	if replay {
		replayRoot = t.TempDir()
	}
	recorded := &[][]string{}
	factory := func(ctx context.Context, name string, args ...string) *exec.Cmd {
		*recorded = append(*recorded, append([]string{name}, args...))
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=TestCoordinatorExchangeHelperProcess")
		command.Env = append(os.Environ(),
			helperEnabled+"=1",
			helperSocket+"="+socketPath,
			helperReplayRoot+"="+replayRoot,
			helperCoordinator+"="+testCoordinatorID,
			// The operation the parent put last on the ssh argv is the one
			// the coordinator side must serve.
			helperOperation+"="+args[len(args)-1],
		)
		return command
	}
	client, err := NewSSHClient(SSHClientConfig{
		CoordinatorID:    testCoordinatorID,
		Address:          "normandy",
		RemoteCommand:    "t3-steward",
		Credentials:      testAdminCredentials(),
		RequestTimeout:   30 * time.Second,
		MaxResponseBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
		Factory: factory,
	})
	if err != nil {
		t.Fatal(err)
	}
	return remoteHarness{client: client, service: service, argv: recorded}
}

func TestRemoteCarrierRoundTripAndPrincipalOverwrite(t *testing.T) {
	harness := newRemoteHarness(t, false)
	// A claimed principal on the request must be discarded on both carriers.
	response, err := harness.client.Query(context.Background(), Query{
		Version: Version, Kind: QueryStatus,
		Principal: Principal{ID: "root", Roles: []string{"local-admin", "superuser"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Kind != QueryStatus || response.Status == nil {
		t.Fatalf("response = %+v", response)
	}
	principal := harness.service.lastPrincipal()
	if principal.ID != "remote:"+testAdminCredentials().ClientPrincipal {
		t.Fatalf("principal id = %q", principal.ID)
	}
	if len(principal.Roles) != 1 || principal.Roles[0] != RemoteAdminRole {
		t.Fatalf("roles = %v, want only %q", principal.Roles, RemoteAdminRole)
	}
}

func TestRemoteCarrierArgvIsFixed(t *testing.T) {
	harness := newRemoteHarness(t, false)
	if _, err := harness.client.Query(context.Background(), Query{Version: Version, Kind: QueryStatus}); err != nil {
		t.Fatal(err)
	}
	if len(*harness.argv) != 1 {
		t.Fatalf("invocations = %d", len(*harness.argv))
	}
	want := []string{
		"ssh", "-oBatchMode=yes", "-oStrictHostKeyChecking=yes", "-oConnectTimeout=10",
		"--", "normandy", "t3-steward", "coordinator-exchange", "query",
	}
	got := (*harness.argv)[0]
	if len(got) != len(want) {
		t.Fatalf("argv = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("argv = %v, want %v", got, want)
		}
	}
	// Nothing after the option terminator may be interpreted as an ssh option
	// or a shell fragment, and the request never contributes a token.
	for _, token := range got[5:] {
		if strings.HasPrefix(token, "-") || strings.ContainsAny(token, " \t;|&$`\n") {
			t.Fatalf("unsafe argv token %q", token)
		}
	}
}

func TestRemoteCarrierStreamsArtifactBytesAfterTheFrame(t *testing.T) {
	harness := newRemoteHarness(t, false)
	// Bytes that are neither valid JSON nor newline-free, so a carrier that
	// quietly re-encoded the stream instead of copying it would be caught.
	harness.service.artifact = []byte("{\"not\":\"json\"\n\x00\xff raw bytes \n\n")
	content, err := harness.client.OpenArtifact(context.Background(), Principal{}, "artifact-1")
	if err != nil {
		t.Fatal(err)
	}
	defer content.Content.Close()
	if content.Metadata.Size != int64(len(harness.service.artifact)) {
		t.Fatalf("declared size = %d, want %d", content.Metadata.Size, len(harness.service.artifact))
	}
	raw, err := io.ReadAll(content.Content)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, harness.service.artifact) {
		t.Fatalf("artifact bytes = %q, want %q", raw, harness.service.artifact)
	}
}

func TestRemoteCarrierStreamsSubmissionBytesAfterTheFrame(t *testing.T) {
	harness := newRemoteHarness(t, false)
	archive := "tar-bytes\n\x00\xfe{\"looks\":\"like json\"}\n"
	response, err := harness.client.SubmitArchive(context.Background(),
		LocalSubmissionRequest{IdempotencyKey: "key-1"}, strings.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatal(err)
	}
	if response.RunID != "run-1" {
		t.Fatalf("run id = %q", response.RunID)
	}
	if len(harness.service.submittedArchives) != 1 || harness.service.submittedArchives[0] != archive {
		t.Fatalf("archive = %q, want %q", harness.service.submittedArchives, archive)
	}
}

func TestRemoteCarrierReplayProducesExactlyOneRun(t *testing.T) {
	harness := newRemoteHarness(t, true)
	archive := "campaign-bundle"
	submit := func() (LocalSubmissionResponse, error) {
		return harness.client.SubmitArchive(context.Background(),
			LocalSubmissionRequest{IdempotencyKey: "key-1"}, strings.NewReader(archive), int64(len(archive)))
	}
	first, err := submit()
	if err != nil {
		t.Fatal(err)
	}
	// The caller lost the first answer and retried with the same idempotency
	// key. It must get the first answer back, not a second run.
	second, err := submit()
	if err != nil {
		t.Fatal(err)
	}
	if first.RunID != second.RunID {
		t.Fatalf("run ids = %q and %q", first.RunID, second.RunID)
	}
	if harness.service.submissions != 1 {
		t.Fatalf("submissions = %d, want exactly 1", harness.service.submissions)
	}
}

func TestRemoteCarrierRefusesReusedRequestIDWithDifferentContent(t *testing.T) {
	harness := newRemoteHarness(t, true)
	if _, err := harness.client.SubmitArchive(context.Background(),
		LocalSubmissionRequest{IdempotencyKey: "key-1"}, strings.NewReader("first"), 5); err != nil {
		t.Fatal(err)
	}
	_, err := harness.client.SubmitArchive(context.Background(),
		LocalSubmissionRequest{IdempotencyKey: "key-1"}, strings.NewReader("second"), 6)
	if err == nil || !strings.Contains(err.Error(), "reused with different content") {
		t.Fatalf("error = %v", err)
	}
	if harness.service.submissions != 1 {
		t.Fatalf("submissions = %d, want exactly 1", harness.service.submissions)
	}
}

func TestRemoteCarrierRefusesOversizedSubmission(t *testing.T) {
	harness := newRemoteHarness(t, false)
	_, err := harness.client.SubmitArchive(context.Background(),
		LocalSubmissionRequest{IdempotencyKey: "key-1"}, strings.NewReader("x"), (1<<20)+1)
	if ClassOf(err) != ClassClientConfiguration {
		t.Fatalf("class = %q (%v)", ClassOf(err), err)
	}
	if !strings.Contains(err.Error(), "exceeds the configured limit") {
		t.Fatalf("error = %v", err)
	}
}

func TestRemoteCarrierTimesOut(t *testing.T) {
	harness := newRemoteHarness(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)
	_, err := harness.client.Query(ctx, Query{Version: Version, Kind: QueryStatus})
	if class := ClassOf(err); class != ClassTimeout && class != ClassUnavailable {
		t.Fatalf("class = %q (%v)", class, err)
	}
}

func TestSSHClientRefusesUnsafeConfiguration(t *testing.T) {
	base := func() SSHClientConfig {
		return SSHClientConfig{
			CoordinatorID: testCoordinatorID, Address: "normandy", RemoteCommand: "t3-steward",
			Credentials: testAdminCredentials(), RequestTimeout: time.Second,
			MaxResponseBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
		}
	}
	for name, mutate := range map[string]func(*SSHClientConfig){
		"option address":        func(c *SSHClientConfig) { c.Address = "-oProxyCommand=touch /tmp/pwned" },
		"shell address":         func(c *SSHClientConfig) { c.Address = "normandy; rm -rf /" },
		"spaced address":        func(c *SSHClientConfig) { c.Address = "normandy extra" },
		"substituted address":   func(c *SSHClientConfig) { c.Address = "$(whoami)" },
		"option command":        func(c *SSHClientConfig) { c.RemoteCommand = "-oProxyCommand=x" },
		"shell command":         func(c *SSHClientConfig) { c.RemoteCommand = "t3-steward && id" },
		"empty coordinator":     func(c *SSHClientConfig) { c.CoordinatorID = "" },
		"incomplete credential": func(c *SSHClientConfig) { c.Credentials.ClientSecret = []byte("short") },
		"zero timeout":          func(c *SSHClientConfig) { c.RequestTimeout = 0 },
		"zero limit":            func(c *SSHClientConfig) { c.MaxResponseBytes = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			config := base()
			mutate(&config)
			client, err := NewSSHClient(config)
			if err == nil || client != nil {
				t.Fatalf("accepted unsafe configuration: %v", err)
			}
			if ClassOf(err) != ClassClientConfiguration {
				t.Fatalf("class = %q (%v)", ClassOf(err), err)
			}
			if teachesRemoteShell(err.Error()) {
				t.Fatalf("error teaches a remote shell: %v", err)
			}
		})
	}
}

func TestSSHClientRefusesUnknownOperationOnArgv(t *testing.T) {
	client, err := NewSSHClient(SSHClientConfig{
		CoordinatorID: testCoordinatorID, Address: "normandy", RemoteCommand: "t3-steward",
		Credentials: testAdminCredentials(), RequestTimeout: time.Second,
		MaxResponseBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"", "query; id", "--config=/etc/shadow", "status"} {
		if _, err := client.arguments(operation); err == nil {
			t.Fatalf("accepted operation %q", operation)
		}
	}
	for _, operation := range Operations() {
		if _, err := client.arguments(operation); err != nil {
			t.Fatalf("refused operation %q: %v", operation, err)
		}
	}
}

func TestRemoteServerRefusesForeignCredentials(t *testing.T) {
	credentials := testAdminCredentials()
	server, err := NewRemoteServer(RemoteServerConfig{
		CoordinatorID:   testCoordinatorID,
		Clients:         map[string]AdminCredentials{credentials.ClientPrincipal: credentials},
		MaxRequestBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	signed := func(secret []byte, sender string) []byte {
		t.Helper()
		request := localRequest{Version: LocalTransportVersion, Operation: localOperationQuery, Query: &Query{Version: Version, Kind: QueryStatus}}
		frame, err := newRemoteFrame(localOperationQuery, "session-1", "req/1", sender, testCoordinatorID, 1,
			time.Now(), time.Now().Add(time.Minute), request)
		if err != nil {
			t.Fatal(err)
		}
		if err := signRemoteFrame(&frame, sender, credentials.ClientKeyID, secret); err != nil {
			t.Fatal(err)
		}
		var buffer bytes.Buffer
		if err := writeRemoteFrame(&buffer, frame, 1<<20); err != nil {
			t.Fatal(err)
		}
		return buffer.Bytes()
	}
	// A worker protocol secret cannot authorize an admin operation.
	var out bytes.Buffer
	err = server.Serve(context.Background(), localOperationQuery,
		bytes.NewReader(signed(testWorkerSecret, credentials.ClientPrincipal)), &out)
	if err == nil || !strings.Contains(err.Error(), "signature mismatch") {
		t.Fatalf("worker secret error = %v", err)
	}
	// An unknown principal is refused before any secret is consulted.
	out.Reset()
	err = server.Serve(context.Background(), localOperationQuery,
		bytes.NewReader(signed(credentials.ClientSecret, "worker:omarchy-pc")), &out)
	if err == nil || !strings.Contains(err.Error(), "does not match a configured admin client") {
		t.Fatalf("unknown principal error = %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("refusal wrote %d bytes of answer", out.Len())
	}
}

func TestAdminFrameSignatureIsDomainSeparatedFromWorkerProtocol(t *testing.T) {
	secret := []byte("0123456789abcdef-shared")
	// Even with one shared secret, a worker envelope's signature must not
	// verify an admin frame with the same field values, and the reverse.
	sentAt := time.Now().UTC().Truncate(time.Second)
	deadline := sentAt.Add(time.Minute)
	admin, err := newRemoteFrame(localOperationQuery, "session-1", "req/1", "admin:a", "normandy", 1, sentAt, deadline, map[string]string{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	if err := signRemoteFrame(&admin, "admin:a", "key-1", secret); err != nil {
		t.Fatal(err)
	}
	worker, err := workerproto.NewEnvelope(workerproto.MessageSnapshot, "session-1", "req/1",
		"admin:a", "normandy", 1, "worker-1", 1, sentAt, deadline, map[string]string{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	if err := workerproto.SignEnvelope(&worker, "admin:a", "key-1", secret); err != nil {
		t.Fatal(err)
	}
	if admin.Authentication.Signature == worker.Authentication.Signature {
		t.Fatal("admin and worker signatures collide over the same fields")
	}
	// Transplanting the worker signature onto the admin frame must fail.
	forged := admin
	forged.Authentication.Signature = worker.Authentication.Signature
	if err := verifyRemoteFrame(forged, secret); err == nil {
		t.Fatal("an admin frame accepted a worker protocol signature")
	}
	// And the reverse: an admin signature must not verify a worker envelope.
	forgedWorker := worker
	forgedWorker.Authentication.Signature = admin.Authentication.Signature
	if err := workerproto.VerifyEnvelopeSignature(forgedWorker, secret); err == nil {
		t.Fatal("a worker envelope accepted an admin frame signature")
	}
}

func TestAdminCredentialNamespaceIsDisjointFromWorkerNamespace(t *testing.T) {
	if err := ValidateAdminCredentialReference("secretref:f02-protocol/omarchy-pc"); err == nil ||
		!strings.Contains(err.Error(), "worker protocol credential") {
		t.Fatalf("worker reference accepted as admin: %v", err)
	}
	if err := ValidateWorkerCredentialReference("secretref:f03-admin/omarchy-pc"); err == nil ||
		!strings.Contains(err.Error(), "coordinator admin credential") {
		t.Fatalf("admin reference accepted as worker: %v", err)
	}
	if err := ValidateAdminCredentialReference("secretref:f03-admin/omarchy-pc"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateWorkerCredentialReference("secretref:f02-protocol/omarchy-pc"); err != nil {
		t.Fatal(err)
	}
}

func TestEnvironmentAdminCredentialResolver(t *testing.T) {
	credentials := testAdminCredentials()
	raw, err := json.Marshal(credentials)
	if err != nil {
		t.Fatal(err)
	}
	resolver := EnvironmentAdminCredentialResolver{
		Lookup: func(name string) (string, bool) {
			if name == "T3_STEWARD_CREDENTIAL_SECRETREF_F03_ADMIN_OMARCHY_PC" {
				return string(raw), true
			}
			return "", false
		},
	}
	resolved, err := resolver.ResolveAdmin("secretref:f03-admin/omarchy-pc")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.ClientPrincipal != credentials.ClientPrincipal || !resolved.Complete() {
		t.Fatalf("resolved = %+v", resolved)
	}
	if _, err := resolver.ResolveAdmin("secretref:f03-admin/absent"); err == nil {
		t.Fatal("resolved an absent reference")
	}
	if _, err := resolver.ResolveAdmin("secretref:f02-protocol/omarchy-pc"); err == nil {
		t.Fatal("resolved a worker reference")
	}
}

func TestRemoteFrameBoundsAndDeadline(t *testing.T) {
	credentials := testAdminCredentials()
	build := func(sentAt, deadline time.Time) []byte {
		t.Helper()
		request := localRequest{Version: LocalTransportVersion, Operation: localOperationQuery, Query: &Query{Version: Version, Kind: QueryStatus}}
		frame, err := newRemoteFrame(localOperationQuery, "session-1", "req/1",
			credentials.ClientPrincipal, testCoordinatorID, 1, sentAt, deadline, request)
		if err != nil {
			t.Fatal(err)
		}
		if err := signRemoteFrame(&frame, credentials.ClientPrincipal, credentials.ClientKeyID, credentials.ClientSecret); err != nil {
			t.Fatal(err)
		}
		var buffer bytes.Buffer
		if err := writeRemoteFrame(&buffer, frame, 1<<20); err != nil {
			t.Fatal(err)
		}
		return buffer.Bytes()
	}
	server, err := NewRemoteServer(RemoteServerConfig{
		CoordinatorID:   testCoordinatorID,
		Clients:         map[string]AdminCredentials{credentials.ClientPrincipal: credentials},
		MaxRequestBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	var out bytes.Buffer
	if err := server.Serve(context.Background(), localOperationQuery,
		bytes.NewReader(build(now, now.Add(-time.Second))), &out); err == nil ||
		!strings.Contains(err.Error(), "deadline expired") {
		t.Fatalf("expired deadline error = %v", err)
	}
	out.Reset()
	if err := server.Serve(context.Background(), localOperationQuery,
		bytes.NewReader(build(now.Add(-time.Hour), now.Add(time.Hour))), &out); err == nil ||
		!strings.Contains(err.Error(), "outside the allowed skew") {
		t.Fatalf("skewed timestamp error = %v", err)
	}
	// A frame larger than the limit is refused as a bound, not as a decode error.
	out.Reset()
	tiny, err := NewRemoteServer(RemoteServerConfig{
		CoordinatorID:   testCoordinatorID,
		Clients:         map[string]AdminCredentials{credentials.ClientPrincipal: credentials},
		MaxRequestBytes: 32, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = tiny.Serve(context.Background(), localOperationQuery, bytes.NewReader(build(now, now.Add(time.Minute))), &out)
	var protocolErr *workerproto.ProtocolError
	if !errors.As(err, &protocolErr) || protocolErr.Code != workerproto.ErrorLimit {
		t.Fatalf("oversized frame error = %v", err)
	}
	if !strings.Contains(err.Error(), "configured message limit") {
		t.Fatalf("bound refusal does not state the limit: %v", err)
	}
}

func TestRemoteServerRefusesUnknownOperation(t *testing.T) {
	credentials := testAdminCredentials()
	server, err := NewRemoteServer(RemoteServerConfig{
		CoordinatorID:   testCoordinatorID,
		Clients:         map[string]AdminCredentials{credentials.ClientPrincipal: credentials},
		MaxRequestBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Serve(context.Background(), "status", bytes.NewReader(nil), io.Discard); err == nil ||
		!strings.Contains(err.Error(), "unknown operation") {
		t.Fatalf("error = %v", err)
	}
}
