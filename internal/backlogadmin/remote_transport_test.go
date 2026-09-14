package backlogadmin

import (
	"bufio"
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

func (s *remoteFakeService) ReleaseQuarantine(_ context.Context, principal Principal, request QuarantineReleaseRequest) (domain.QuarantineRelease, error) {
	s.record(principal)
	return domain.QuarantineRelease{Key: request.Key, Released: true}, nil
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
//
// newClient builds a further client against the same coordinator, which is what
// a retry from a new CLI invocation actually is: a fresh session id.
type remoteHarness struct {
	client    *SSHClient
	service   *remoteFakeService
	argv      *[][]string
	newClient func(*testing.T) *SSHClient
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
		CoordinatorID:   testCoordinatorID,
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
	newClient := func(t *testing.T) *SSHClient {
		t.Helper()
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
		return client
	}
	return remoteHarness{client: newClient(t), service: service, argv: recorded, newClient: newClient}
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

// A real retry is a new CLI process, so it is a new client with a new session
// id. Reusing one client would hide the defect this test exists to catch: an
// answer replayed inside the frame that carried the first one is bound to the
// session of the request that was lost, and the retry rejects it.
func TestRemoteCarrierReplayProducesExactlyOneRunAcrossProcesses(t *testing.T) {
	harness := newRemoteHarness(t, true)
	archive := "campaign-bundle"
	submit := func(client *SSHClient) (LocalSubmissionResponse, error) {
		return client.SubmitArchive(context.Background(),
			LocalSubmissionRequest{IdempotencyKey: "key-1"}, strings.NewReader(archive), int64(len(archive)))
	}
	first, err := submit(harness.client)
	if err != nil {
		t.Fatal(err)
	}
	// The caller lost the first answer and retried with the same idempotency
	// key from a fresh invocation. It must get the first answer back.
	retry := harness.newClient(t)
	if retry.config.SessionID == harness.client.config.SessionID {
		t.Fatal("the retry reused the first client's session id; it is not a retry")
	}
	second, err := submit(retry)
	if err != nil {
		t.Fatalf("retry from a new client: %v", err)
	}
	if first.RunID != second.RunID {
		t.Fatalf("run ids = %q and %q", first.RunID, second.RunID)
	}
	if harness.service.submissions != 1 {
		t.Fatalf("submissions = %d, want exactly 1", harness.service.submissions)
	}
}

// Equal-length different content is the case a digest over the request envelope
// alone cannot see, and it is exactly the case an agent hits when it edits one
// character of a prompt and resubmits under the same key.
func TestRemoteCarrierRefusesReusedKeyWithEqualLengthDifferentContent(t *testing.T) {
	harness := newRemoteHarness(t, true)
	if _, err := harness.client.SubmitArchive(context.Background(),
		LocalSubmissionRequest{IdempotencyKey: "key-1"}, strings.NewReader("AAAAA"), 5); err != nil {
		t.Fatal(err)
	}
	_, err := harness.client.SubmitArchive(context.Background(),
		LocalSubmissionRequest{IdempotencyKey: "key-1"}, strings.NewReader("BBBBB"), 5)
	if err == nil || !strings.Contains(err.Error(), "reused with different content") {
		t.Fatalf("error = %v", err)
	}
	if harness.service.submissions != 1 {
		t.Fatalf("submissions = %d, want exactly 1", harness.service.submissions)
	}
	// The refusal is signed, so it carries its exact class rather than the
	// coarse one an unsigned frame would allow.
	if ClassOf(err) != ClassRejected {
		t.Fatalf("class = %q (%v)", ClassOf(err), err)
	}
}

// A second request that arrives while the first is still in flight must not
// start a second effect. It is told to wait instead.
//
// The two are serialised through the durable pending row rather than through
// the interprocess lock, because that is what a real second SSH session sees:
// the first process holds the lock only while it touches the store, not for as
// long as its handler runs.
func TestRemoteCarrierRefusesAConcurrentDuplicateRequestID(t *testing.T) {
	root := t.TempDir()
	open := func() *RemoteReplayStore {
		t.Helper()
		store, err := OpenRemoteReplayStore(root, testCoordinatorID)
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	inFlight, err := open().Begin("admin:omarchy-pc", "submission/key-1", "digest-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := inFlight.Release(); err != nil {
		t.Fatal(err)
	}
	_, err = open().Begin("admin:omarchy-pc", "submission/key-1", "digest-1")
	var protocolErr *workerproto.ProtocolError
	if !errors.As(err, &protocolErr) || protocolErr.Code != workerproto.ErrorBackpressure {
		t.Fatalf("concurrent duplicate error = %v", err)
	}
	if !protocolErr.Retryable {
		t.Fatal("a duplicate that should be retried was not marked retryable")
	}
	// The same identity carrying different content is refused outright rather
	// than told to wait, because no wait would make it the same request.
	_, err = open().Begin("admin:omarchy-pc", "submission/key-1", "digest-2")
	if !errors.As(err, &protocolErr) || protocolErr.Code != workerproto.ErrorReplay {
		t.Fatalf("conflicting duplicate error = %v", err)
	}
}

// A handler killed after the relay but before the cache write is the case the
// transport store cannot cover. The operation re-executes, and what makes that
// safe is the service's own idempotency key, not this store. Pinning the real
// behaviour keeps the documentation honest.
func TestRemoteCarrierReExecutesWhenTheAnswerWasNeverCached(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	open := func() *RemoteReplayStore {
		store, err := OpenRemoteReplayStore(root, testCoordinatorID)
		if err != nil {
			t.Fatal(err)
		}
		store.now = func() time.Time { return now }
		return store
	}
	// The handler claimed the request and died: the row stays pending and no
	// answer was cached.
	transaction, err := open().Begin("admin:omarchy-pc", "submission/key-1", "digest-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.Release(); err != nil {
		t.Fatal(err)
	}
	// Immediately afterwards the identity is still held, so a retry waits.
	if _, err := open().Begin("admin:omarchy-pc", "submission/key-1", "digest-1"); err == nil {
		t.Fatal("a retry claimed a pending request identity")
	}
	// After the abandoned interval the row is reclaimed and the operation runs
	// again. This is the transport's real guarantee: a short-window shield,
	// not exactly-once execution.
	now = now.Add(remoteReplayAbandonedAge + time.Minute)
	recovered, err := open().Begin("admin:omarchy-pc", "submission/key-1", "digest-1")
	if err != nil {
		t.Fatalf("the abandoned identity was never reclaimed: %v", err)
	}
	if cached, ok := recovered.Cached(); ok || cached != nil {
		t.Fatal("a request that never completed reported a cached answer")
	}
	if err := recovered.Release(); err != nil {
		t.Fatal(err)
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

// A deadline that expires is a timeout, exit 6, and not an outage: an agent
// retries a timeout and reconfigures for an outage, so the two must not be
// interchangeable.
func TestRemoteCarrierTimesOut(t *testing.T) {
	harness := newRemoteHarness(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)
	_, err := harness.client.Query(ctx, Query{Version: Version, Kind: QueryStatus})
	if ClassOf(err) != ClassTimeout {
		t.Fatalf("class = %q (%v)", ClassOf(err), err)
	}
	if ExitCodeFor(err) != 6 {
		t.Fatalf("exit code = %d, want 6", ExitCodeFor(err))
	}
}

// Every refusal must reach the client as its own class. Before this was true
// they all arrived as protocol/exit 7, so a rotated credential looked like a
// transient fault and an agent would have retried it forever.
func TestRemoteCarrierClassifiesEveryServerRefusal(t *testing.T) {
	tests := map[string]struct {
		mutate func(*SSHClientConfig)
		want   TransportClass
		exit   int
	}{
		"wrong client secret": {
			mutate: func(c *SSHClientConfig) { c.Credentials.ClientSecret = []byte("0123456789abcdef-wrong") },
			want:   ClassAuthentication, exit: 4,
		},
		"unknown principal": {
			mutate: func(c *SSHClientConfig) { c.Credentials.ClientPrincipal = "admin:stranger" },
			want:   ClassAuthentication, exit: 4,
		},
		"wrong key id": {
			mutate: func(c *SSHClientConfig) { c.Credentials.ClientKeyID = "admin-key-2" },
			want:   ClassAuthentication, exit: 4,
		},
		"wrong coordinator": {
			mutate: func(c *SSHClientConfig) { c.CoordinatorID = "someone-else" },
			want:   ClassAuthentication, exit: 4,
		},
		"expired deadline": {
			mutate: func(c *SSHClientConfig) {
				// Sent from a clock far in the past, so the coordinator sees a
				// deadline that has already gone by.
				c.Now = func() time.Time { return time.Now().Add(-2 * time.Hour) }
			},
			want: ClassTimeout, exit: 6,
		},
		"unsupported frame limit": {
			mutate: func(c *SSHClientConfig) { c.MaxResponseBytes = 48 },
			want:   ClassProtocol, exit: 7,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			harness := newRemoteHarness(t, false)
			config := harness.client.config
			test.mutate(&config)
			client, err := NewSSHClient(config)
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.Query(context.Background(), Query{Version: Version, Kind: QueryStatus})
			if err == nil {
				t.Fatal("the coordinator answered a request it should have refused")
			}
			if ClassOf(err) != test.want {
				t.Fatalf("class = %q, want %q (%v)", ClassOf(err), test.want, err)
			}
			if ExitCodeFor(err) != test.exit {
				t.Fatalf("exit code = %d, want %d", ExitCodeFor(err), test.exit)
			}
		})
	}
}

// The refusal text must not say whether the principal was configured. That
// difference would answer, for anyone holding the SSH key, the question "who
// else administers this coordinator".
func TestRemoteCarrierDoesNotDistinguishUnknownPrincipalFromBadSignature(t *testing.T) {
	harness := newRemoteHarness(t, false)
	message := func(mutate func(*SSHClientConfig)) string {
		t.Helper()
		config := harness.client.config
		mutate(&config)
		client, err := NewSSHClient(config)
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.Query(context.Background(), Query{Version: Version, Kind: QueryStatus})
		if err == nil {
			t.Fatal("the coordinator answered a request it should have refused")
		}
		var transportErr *TransportError
		if !errors.As(err, &transportErr) {
			t.Fatalf("unclassified error %v", err)
		}
		return transportErr.Err.Error()
	}
	unknown := message(func(c *SSHClientConfig) { c.Credentials.ClientPrincipal = "admin:stranger" })
	forged := message(func(c *SSHClientConfig) { c.Credentials.ClientSecret = []byte("0123456789abcdef-wrong") })
	if unknown != forged {
		t.Fatalf("refusals differ: %q and %q", unknown, forged)
	}
	if !strings.Contains(unknown, authenticationFailed) {
		t.Fatalf("refusal = %q", unknown)
	}
}

// An unsigned frame can classify a failure and nothing else. One that claims
// success, or a class only a verified coordinator could know, is refused.
func TestClientAcceptsAnUnsignedFrameOnlyAsAClassification(t *testing.T) {
	client, err := NewSSHClient(SSHClientConfig{
		CoordinatorID: testCoordinatorID, Address: "normandy", RemoteCommand: "t3-steward",
		Credentials: testAdminCredentials(), RequestTimeout: time.Second,
		MaxResponseBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	answer := Response{Version: Version, Kind: QueryStatus}
	for name, test := range map[string]struct {
		response localResponse
		want     TransportClass
	}{
		"authentication passes through": {
			response: localResponse{Version: LocalTransportVersion, Error: authenticationFailed, ErrorClass: ClassAuthentication},
			want:     ClassAuthentication,
		},
		"timeout passes through": {
			response: localResponse{Version: LocalTransportVersion, Error: "request deadline expired", ErrorClass: ClassTimeout},
			want:     ClassTimeout,
		},
		"a claimed rejection is narrowed": {
			// Only a verified coordinator can say it considered the request
			// and refused it on its merits.
			response: localResponse{Version: LocalTransportVersion, Error: "no", ErrorClass: ClassRejected},
			want:     ClassProtocol,
		},
		"an answer without an error is not an answer": {
			response: localResponse{Version: LocalTransportVersion, Response: &answer},
			want:     ClassProtocol,
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := client.unsignedRefusal(localOperationQuery, test.response)
			if ClassOf(err) != test.want {
				t.Fatalf("class = %q, want %q (%v)", ClassOf(err), test.want, err)
			}
		})
	}
}

// A client that tries to name its own principal is refused; the coordinator
// derives the principal from the signature and nothing else.
func TestRemoteServerRefusesAClientSuppliedAssertion(t *testing.T) {
	credentials := testAdminCredentials()
	server, err := NewRemoteServer(RemoteServerConfig{
		CoordinatorID:   testCoordinatorID,
		Clients:         map[string]AdminCredentials{credentials.ClientPrincipal: credentials},
		MaxRequestBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := localRequest{
		Version: LocalTransportVersion, Operation: localOperationQuery,
		Query:       &Query{Version: Version, Kind: QueryStatus},
		RemoteAdmin: &RemoteAdminAssertion{Principal: "admin:root", Coordinator: testCoordinatorID, RequestID: "req/1"},
	}
	frame, err := newRemoteFrame(localOperationQuery, "session-1", "req/1",
		credentials.ClientPrincipal, testCoordinatorID, 1, time.Now(), time.Now().Add(time.Minute), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := signRemoteFrame(&frame, credentials.ClientPrincipal, credentials.ClientKeyID, credentials.ClientSecret); err != nil {
		t.Fatal(err)
	}
	var input, out bytes.Buffer
	if err := writeRemoteFrame(&input, frame, 1<<20); err != nil {
		t.Fatal(err)
	}
	err = server.Serve(context.Background(), localOperationQuery, &input, &out)
	if err == nil || !strings.Contains(err.Error(), "may not assert its own principal") {
		t.Fatalf("error = %v", err)
	}
}

// The local socket refuses a relayed assertion that names another coordinator,
// so a client configured for one coordinator cannot have its request replayed
// into a second one.
func TestLocalServerRefusesAnAssertionForAnotherCoordinator(t *testing.T) {
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
		CoordinatorID:   testCoordinatorID,
		MaxRequestBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
		RequestTimeout: 5 * time.Second, MaxConcurrent: 4,
	}
	go func() { done <- server.Serve(ctx) }()
	defer stopLocalTransport(t, cancel, done)
	client := LocalClient{
		Path: socketPath, MaxResponseBytes: 1 << 20, MaxArtifactBytes: 1 << 20,
		MaxSubmissionBytes: 1 << 20, RequestTimeout: 5 * time.Second,
	}
	for name, assertion := range map[string]*RemoteAdminAssertion{
		"another coordinator": {Principal: "admin:omarchy-pc", Coordinator: "someone-else", RequestID: "req/1"},
		"no coordinator":      {Principal: "admin:omarchy-pc", RequestID: "req/1"},
		"no principal":        {Coordinator: testCoordinatorID, RequestID: "req/1"},
		"no request id":       {Principal: "admin:omarchy-pc", Coordinator: testCoordinatorID},
	} {
		t.Run(name, func(t *testing.T) {
			var response localResponse
			err := client.call(context.Background(), localRequest{
				Version: LocalTransportVersion, Operation: localOperationQuery,
				Query: &Query{Version: Version, Kind: QueryStatus}, RemoteAdmin: assertion,
			}, &response)
			if ClassOf(err) != ClassAuthentication {
				t.Fatalf("class = %q (%v)", ClassOf(err), err)
			}
		})
	}
	// The well-formed assertion for this coordinator is accepted and narrows
	// the peer's authority to the remote-admin role.
	var response localResponse
	if err := client.call(context.Background(), localRequest{
		Version: LocalTransportVersion, Operation: localOperationQuery,
		Query: &Query{Version: Version, Kind: QueryStatus},
		RemoteAdmin: &RemoteAdminAssertion{
			Principal: "admin:omarchy-pc", Coordinator: testCoordinatorID, RequestID: "req/1",
		},
	}, &response); err != nil {
		t.Fatal(err)
	}
	principal := service.lastPrincipal()
	if principal.ID != "remote:admin:omarchy-pc" ||
		len(principal.Roles) != 1 || principal.Roles[0] != RemoteAdminRole {
		t.Fatalf("principal = %+v", principal)
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
	if err == nil || !strings.Contains(err.Error(), authenticationFailed) {
		t.Fatalf("worker secret error = %v", err)
	}
	// An unknown principal is refused identically, so the refusal says nothing
	// about which principals this coordinator accepts.
	out.Reset()
	err = server.Serve(context.Background(), localOperationQuery,
		bytes.NewReader(signed(credentials.ClientSecret, "worker:omarchy-pc")), &out)
	if err == nil || !strings.Contains(err.Error(), authenticationFailed) {
		t.Fatalf("unknown principal error = %v", err)
	}
	// The refusal is written back so the client can classify it, and it is
	// unsigned, because there is no verified peer to sign for.
	if out.Len() == 0 {
		t.Fatal("an unauthenticated refusal wrote nothing back")
	}
	frame, err := readRemoteFrame(bufio.NewReader(bytes.NewReader(out.Bytes())), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if frame.Authentication.Signature != "" {
		t.Fatal("an unauthenticated refusal was signed")
	}
	var response localResponse
	if err := json.Unmarshal(frame.Payload, &response); err != nil {
		t.Fatal(err)
	}
	if response.ErrorClass != ClassAuthentication || response.Response != nil {
		t.Fatalf("refusal payload = %+v", response)
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

// The other direction, an admin reference offered where a worker credential is
// expected, is enforced and tested in internal/config, which is where worker
// credentials are declared.
func TestAdminCredentialNamespaceIsDisjointFromWorkerNamespace(t *testing.T) {
	if err := ValidateAdminCredentialReference("secretref:f02-protocol/omarchy-pc"); err == nil ||
		!strings.Contains(err.Error(), "worker protocol credential") {
		t.Fatalf("worker reference accepted as admin: %v", err)
	}
	if err := ValidateAdminCredentialReference("secretref:f03-admin/omarchy-pc"); err != nil {
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
