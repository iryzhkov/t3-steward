package backlogadmin

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/testtiming"
)

type leaseTestStore struct {
	request domain.LeaseRequest
	calls   int
}

func (s *leaseTestStore) ExecuteLease(_ context.Context, r domain.LeaseRequest) (domain.LeaseResponse, error) {
	s.request = r
	s.calls++
	return domain.LeaseResponse{Lease: &domain.Lease{Name: r.Name, OwnerThread: r.OwnerThread, Principal: r.Principal, Token: 7}, Code: 0}, nil
}

type leaseTestService struct {
	localTransportService
	store leaseTestStore
}

func (s *leaseTestService) Lease(ctx context.Context, p Principal, r domain.LeaseRequest) (domain.LeaseResponse, error) {
	return executeLease(ctx, &s.store, p, r)
}
func leaseTestRequest() domain.LeaseRequest {
	return domain.LeaseRequest{Action: "acquire", Name: "repo:steward/main", OwnerThread: "11111111-1111-4111-8111-111111111111", Principal: "spoof", TTL: 2 * time.Hour, Reason: "integrate", RequestID: "lease-one"}
}
func TestLeaseLocalAndSSHCarriers(t *testing.T) {
	service := &leaseTestService{}
	client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), service)
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(func() { stopLocalTransport(t, cancel, done) }) }
	defer stop()
	request := leaseTestRequest()
	got, err := client.Lease(context.Background(), request)
	if err != nil || got.Lease == nil || got.Lease.Token != 7 || got.Lease.Principal == "spoof" {
		t.Fatalf("local: %+v %v", got, err)
	}
	// The helper executes the real remote server in a child process and relays to the local socket.
	serverID := testCoordinatorID
	// Remote assertions are bound to this identity by the local coordinator.
	stop()
	path := client.Path
	listener, err := ListenLocal(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	server := &LocalServer{Listener: listener, Service: service, AllowedUID: uint32(os.Getuid()), CoordinatorID: serverID, MaxRequestBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20, RequestTimeout: testtiming.Bound(time.Second), MaxConcurrent: 8}
	go func() { done2 <- server.Serve(ctx) }()
	defer stopLocalTransport(t, cancel2, done2)
	replayRoot := t.TempDir()
	factory := func(ctx context.Context, _ string, args ...string) *exec.Cmd {
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=TestCoordinatorExchangeHelperProcess")
		command.Env = append(os.Environ(), helperEnabled+"=1", helperReplayRoot+"="+replayRoot, helperSocket+"="+path, helperCoordinator+"="+serverID, helperOperation+"="+args[len(args)-1])
		return command
	}
	remote, err := NewSSHClient(SSHClientConfig{CoordinatorID: serverID, Address: "normandy", RemoteCommand: "t3-steward", Credentials: testAdminCredentials(), RequestTimeout: testtiming.Bound(30 * time.Second), MaxResponseBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20, Factory: factory})
	if err != nil {
		t.Fatal(err)
	}
	got, err = remote.Lease(context.Background(), request)
	if err != nil || got.Lease == nil || got.Lease.Principal != RelayedPrincipalID(testAdminCredentials().ClientPrincipal) {
		t.Fatalf("ssh: %+v %v", got, err)
	}
	replayed, err := remote.Lease(context.Background(), request)
	if err != nil || !replayed.Replay || replayed.Lease == nil || replayed.Lease.Token != got.Lease.Token || service.store.calls != 2 {
		t.Fatalf("SSH replay: %+v err=%v calls=%d", replayed, err, service.store.calls)
	}
	request.Reason = "changed digest"
	if _, err := remote.Lease(context.Background(), request); err == nil {
		t.Fatal("carrier accepted conflicting mutation request id")
	}
	for _, action := range []string{"check", "show", "list"} {
		request.Action = action
		if action == "list" {
			request.Name = ""
		}
		first, err := remote.Lease(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		second, err := remote.Lease(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if first.Replay || second.Replay {
			t.Fatalf("%s read used replay store", action)
		}
	}
	if service.store.calls != 8 {
		t.Fatalf("reads not delivered fresh: %d", service.store.calls)
	}
}
func TestLeaseOlderCoordinatorHelperProcess(t *testing.T) {
	mode := os.Getenv("T3_LEASE_OLD_COORDINATOR")
	if mode != "1" && mode != "unsigned" && mode != "pinned" {
		t.Skip("helper process")
	}
	if mode == "pinned" {
		// Behind a forced command that runs the client's operation word, the
		// base coordinator refuses the unknown word before reading any frame.
		fmt.Fprintln(os.Stderr, `error: coordinator-exchange: unknown operation "lease"`)
		os.Exit(1)
	}
	request, err := readRemoteFrame(bufio.NewReader(os.Stdin), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if mode == "unsigned" {
		// The base coordinator rejects an unknown operation in validate, before
		// verifying the signature, so it refuses without credentials: an
		// unsigned frame on stdout, the returned error on stderr, and exit 1.
		response, err := newRemoteFrame(request.Operation, request.SessionID, "response-"+request.RequestID, testCoordinatorID, request.Sender, request.Sequence, time.Now(), request.Deadline, localResponse{Version: LocalTransportVersion, Error: "unknown admin frame operation", ErrorClass: ClassProtocol})
		if err != nil {
			t.Fatal(err)
		}
		response.InReplyTo = request.RequestID
		if err := writeRemoteFrame(os.Stdout, response, 1<<20); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintln(os.Stderr, "error: coordinator-exchange refused lease: malformed: unknown admin frame operation")
		os.Exit(1)
	}
	credentials := testAdminCredentials()
	response, err := newRemoteFrame(request.Operation, request.SessionID, "response-"+request.RequestID, testCoordinatorID, request.Sender, request.Sequence, time.Now(), request.Deadline, localResponse{Version: LocalTransportVersion, Error: "unknown admin frame operation", ErrorClass: ClassProtocol})
	if err != nil {
		t.Fatal(err)
	}
	response.InReplyTo = request.RequestID
	if err := signRemoteFrame(&response, credentials.CoordinatorPrincipal, credentials.CoordinatorKeyID, credentials.CoordinatorSecret); err != nil {
		t.Fatal(err)
	}
	if err := writeRemoteFrame(os.Stdout, response, 1<<20); err != nil {
		t.Fatal(err)
	}
}
func TestLeaseOlderCoordinatorCarrierMessages(t *testing.T) {
	path := filepath.Join(shortTempRoot(t), "old.sock")
	listener, err := ListenLocal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan error, 1)
	go func() {
		conn, err := listener.AcceptUnix()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		// The base coordinator strictly decodes a request type without Lease
		// before dispatching, so it refuses the new field as a protocol error.
		var request struct {
			Version   string `json:"version"`
			Operation string `json:"operation"`
		}
		err = readLocalJSON(conn, 1<<20, &request)
		if err == nil {
			done <- errors.New("old coordinator unexpectedly decoded lease field")
			return
		}
		done <- writeLocalResponse(conn, localResponse{Version: LocalTransportVersion, Error: err.Error(), ErrorClass: ClassProtocol})
	}()
	local := LocalClient{Path: path, MaxResponseBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20, RequestTimeout: testtiming.Bound(time.Second)}
	_, err = local.Lease(context.Background(), leaseTestRequest())
	if ClassOf(err) != ClassProtocol || !strings.Contains(err.Error(), "the coordinator does not support leases; upgrade it") {
		t.Fatalf("old local: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"1", "unsigned", "pinned"} {
		factory := func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=TestLeaseOlderCoordinatorHelperProcess")
			command.Env = append(os.Environ(), "T3_LEASE_OLD_COORDINATOR="+mode)
			return command
		}
		remote, err := NewSSHClient(SSHClientConfig{CoordinatorID: testCoordinatorID, Address: "normandy", RemoteCommand: "t3-steward", Credentials: testAdminCredentials(), RequestTimeout: testtiming.Bound(30 * time.Second), MaxResponseBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20, Factory: factory})
		if err != nil {
			t.Fatal(err)
		}
		_, err = remote.Lease(context.Background(), leaseTestRequest())
		if ClassOf(err) != ClassProtocol || !strings.Contains(err.Error(), "the coordinator does not support leases; upgrade it") {
			t.Fatalf("old SSH (%s): %v", mode, err)
		}
	}
}

func TestLeaseAuthorityAndEnvelope(t *testing.T) {
	store := &leaseTestStore{}
	r := leaseTestRequest()
	for _, p := range []Principal{{ID: "supervisor", Roles: []string{SupervisorRole}}, {ID: "supervisor", Roles: []string{SupervisorRole, LocalAdminRole}}, {ID: "worker", Roles: []string{"worker"}}} {
		got, err := executeLease(context.Background(), store, p, r)
		if err == nil && got.Code != 10 {
			t.Fatalf("authorized %+v: %+v", p, got)
		}
	}
	if store.calls != 0 {
		t.Fatal("unauthorized operation reached store")
	}
	r.Action = "show"
	read, err := executeLease(context.Background(), store, Principal{ID: "supervisor", Roles: []string{SupervisorRole}}, r)
	if err != nil || read.Code != 0 || store.request.Principal != "supervisor" {
		t.Fatalf("supervisor read: %+v %v", read, err)
	}
	r = leaseTestRequest()
	service := &leaseTestService{}
	for _, request := range []localRequest{
		{Operation: localOperationQuery, Query: &Query{}, Lease: &r},
		{Operation: localOperationLease, Lease: &r, Query: &Query{}},
		{Operation: localOperationLease, Lease: &r, RecoveryRetry: &domain.RecoveryRetryRequest{}},
		{Operation: localOperationLease},
	} {
		got, _ := (adminDispatch{service: service}).handle(context.Background(), Principal{ID: "operator", Roles: []string{LocalAdminRole}}, request, nil)
		if got.Error == "" {
			t.Fatalf("accepted malformed envelope: %+v", request)
		}
	}
}
func TestLeaseReadClassificationIdentityAndCompatibility(t *testing.T) {
	for _, action := range []string{"check", "show", "list", "acquire", "renew", "release"} {
		r := leaseTestRequest()
		r.Action = action
		envelope := localRequest{Operation: localOperationLease, Lease: &r}
		want := action == "acquire" || action == "renew" || action == "release"
		if mutatingRequest(localOperationLease, envelope) != want {
			t.Fatalf("%s classification", action)
		}
		first, err := requestIdentity(envelope)
		if err != nil {
			t.Fatal(err)
		}
		second, err := requestIdentity(envelope)
		if err != nil {
			t.Fatal(err)
		}
		if want && (first != second || first != "lease/lease-one") {
			t.Fatalf("identity %s %s", first, second)
		}
		if !want && first == second {
			t.Fatal("read used stable identity")
		}
	}
	original := &TransportError{Class: ClassProtocol, Operation: "lease", Coordinator: "old", Err: errors.New("unknown admin frame operation")}
	upgraded := leaseCompatibilityError(original)
	if ClassOf(upgraded) != ClassProtocol || !strings.Contains(upgraded.Error(), "the coordinator does not support leases; upgrade it") {
		t.Fatalf("upgrade: %v", upgraded)
	}
	for _, message := range []string{"unknown local admin operation", "coordinator-exchange: unknown operation \"lease\""} {
		if !strings.Contains(leaseCompatibilityError(errors.New(message)).Error(), "upgrade it") {
			t.Fatalf("not mapped: %s", message)
		}
	}
	unavailable := &TransportError{Class: ClassUnavailable, Err: errors.New("unreachable")}
	if leaseCompatibilityError(unavailable) != unavailable {
		t.Fatal("changed unrelated error")
	}
}
