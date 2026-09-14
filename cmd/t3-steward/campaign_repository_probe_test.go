package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

const (
	probeCatalogDigest = "3333333333333333333333333333333333333333333333333333333333333333"
	probeRepository    = "ssh://git@git.ryzhkov.dev/igor/dev-fleet.git"
	probeCredentialRef = "secretref:f03-admin/homelab"
)

// probeProcessRunner answers the one command the probe runs. It records what it
// was asked so a test can assert the argument vector reached the far end intact.
type probeProcessRunner struct {
	calls  []backlog.ProcessRequest
	output string
	exit   int
	err    error
}

func (r *probeProcessRunner) Run(_ context.Context, request backlog.ProcessRequest) (backlog.ProcessResult, error) {
	r.calls = append(r.calls, request)
	return backlog.ProcessResult{Output: r.output, ExitCode: r.exit}, r.err
}

// probeLoopback carries one signed envelope from the coordinator client into a
// real worker protocol server and back. Nothing about the exchange is stubbed:
// the envelope is signed, the message kind is checked against the worker's
// allowlist, and the payload is decoded by the worker before the probe runs.
type probeLoopback struct {
	server *workerproto.Server
	driver *workerruntime.LocalDriver
}

func (l probeLoopback) RoundTripWithRetry(ctx context.Context, request workerproto.Envelope, _ workerproto.RetryPolicy) (workerproto.Envelope, error) {
	return l.server.Handle(ctx, request, func(ctx context.Context, envelope workerproto.Envelope) (workerproto.MessageType, any, error) {
		var probe workerproto.RepositoryProbeRequest
		if err := workerproto.DecodePayload(envelope, workerproto.MessageRepositoryProbe, &probe); err != nil {
			return "", nil, err
		}
		observation, err := l.driver.ObserveRepository(ctx, probe)
		return workerproto.MessageRepositoryObservation, observation, err
	})
}

// probeWorker is one in-process worker: a real local driver with a scripted
// process runner and a credential checker that answers availability only.
func probeWorker(t *testing.T, workerID string, runner backlog.PreflightRunner, available ...string) repositoryProbeClient {
	t.Helper()
	resolvable := make(map[string]bool, len(available))
	for _, reference := range available {
		resolvable[reference] = true
	}
	driver := &workerruntime.LocalDriver{
		Config:      workerruntime.LocalDriverConfig{RunsRoot: t.TempDir()},
		Preflight:   runner,
		Credentials: probeCredentialChecker(resolvable),
		Now:         time.Now,
	}
	server, err := workerproto.NewServer(workerproto.ServerConfig{
		CoordinatorID: "coordinator", WorkerID: workerID, CoordinatorEpoch: 7, WorkerEpoch: "worker-1",
		PeerPrincipal: "ssh:coordinator", PeerKeyID: "coordinator-key",
		PeerSecret:      []byte("coordinator-secret-value"),
		SignerPrincipal: "ssh:" + workerID, SignerKeyID: "worker-key",
		SignerSecret: []byte("worker-secret-value"),
		Allowed:      map[workerproto.MessageType]bool{workerproto.MessageRepositoryProbe: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := workerproto.NewClient(workerproto.ClientConfig{
		CoordinatorID: "coordinator", WorkerID: workerID,
		CoordinatorEpoch: 7, WorkerEpoch: "worker-1", SessionID: "probe-" + workerID,
		RequestTimeout:  time.Minute,
		SignerPrincipal: "ssh:coordinator", SignerKeyID: "coordinator-key",
		SignerSecret: []byte("coordinator-secret-value"),
		RetryPolicy:  workerproto.RetryPolicy{MaxAttempts: 1, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
		Transport:    probeLoopback{server: server, driver: driver},
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// probeCredentialChecker reports availability and never a value.
type probeCredentialChecker map[string]bool

func (c probeCredentialChecker) Require(_ context.Context, references []string) error {
	for _, reference := range references {
		if !c[reference] {
			return errors.New("resolve credentials: reference " + reference + " is unavailable")
		}
	}
	return nil
}

// probeObserver builds the coordinator's observer with its transport replaced by
// the in-process workers above. A worker that is not listed cannot be reached,
// which is how "no worker answers" is stated.
func probeObserver(clients map[string]repositoryProbeClient) *coordinatorRepositoryObserver {
	observer := newCoordinatorRepositoryObserver(coordinatorProbeSettings(), nil, 7, nil)
	observer.dial = func(_ context.Context, workerID string) (repositoryProbeClient, func() error, error) {
		client, reachable := clients[workerID]
		if !reachable {
			return nil, nil, errors.New("worker " + workerID + " did not answer")
		}
		return client, nil, nil
	}
	return observer
}

func probeKey(workerID string) backlog.RepositoryProbeKey {
	return backlog.RepositoryProbeKey{
		WorkerID: workerID, CatalogDigest: probeCatalogDigest,
		Repository: probeRepository, Ref: "main",
		CredentialRefs: []string{probeCredentialRef},
	}
}

// reachableOutput and absentOutput are verbatim shapes of real git ls-remote
// runs, so the classification under test is the measured one.
const (
	reachableOutput = "a90c0978567908625629368af0749936c9ff6b92\trefs/heads/main\n"
	absentOutput    = "remote: Repository not found.\nfatal: repository 'https://github.com/owner/absent/' not found\n"
	refusedOutput   = "git@github.com: Permission denied (publickey).\nfatal: Could not read from remote repository.\n"
)

func reachableWorker(t *testing.T, workerID string) repositoryProbeClient {
	return probeWorker(t, workerID, &probeProcessRunner{output: reachableOutput}, probeCredentialRef)
}

func absentRepositoryWorker(t *testing.T, workerID string) repositoryProbeClient {
	return probeWorker(t, workerID,
		&probeProcessRunner{output: absentOutput, exit: 128, err: &backlog.ProcessExitError{ExitCode: 128}},
		probeCredentialRef)
}

func unauthenticatedWorker(t *testing.T, workerID string) repositoryProbeClient {
	return probeWorker(t, workerID,
		&probeProcessRunner{output: refusedOutput, exit: 128, err: &backlog.ProcessExitError{ExitCode: 128}},
		probeCredentialRef)
}

// TestRepositoryProbeIsDispatchedToTheWorker states the whole point of the seam:
// the coordinator asks the worker, over the worker protocol, and gets back a
// classified answer it did not compute itself.
func TestRepositoryProbeIsDispatchedToTheWorker(t *testing.T) {
	runner := &probeProcessRunner{output: reachableOutput}
	observer := probeObserver(map[string]repositoryProbeClient{
		"homelab": probeWorker(t, "homelab", runner, probeCredentialRef),
	})
	observation, err := observer.ObserveRepository(context.Background(), probeKey("homelab"))
	if err != nil {
		t.Fatal(err)
	}
	if observation.Class != backlog.RepositoryAuthenticatedOK {
		t.Fatalf("class = %q", observation.Class)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("the worker ran the probe %d time(s), want 1", len(runner.calls))
	}
	want := []string{"ls-remote", "--exit-code", "--", probeRepository, "main"}
	if runner.calls[0].Program != "git" ||
		strings.Join(runner.calls[0].Args, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("argv = %s %v", runner.calls[0].Program, runner.calls[0].Args)
	}
	if runner.calls[0].MaxOutputBytes <= 0 {
		t.Fatal("the dispatched probe ran without an accumulation bound")
	}
	if strings.Contains(observation.Detail, "secretref:") {
		t.Fatalf("detail named a credential reference: %q", observation.Detail)
	}
}

// TestRepositoryProbeEvidenceIsRetainedAndInvalidated states the short-TTL
// evidence contract on the dispatched path, including the one invalidation that
// matters most: a rotated credential reference.
func TestRepositoryProbeEvidenceIsRetainedAndInvalidated(t *testing.T) {
	runner := &probeProcessRunner{output: reachableOutput}
	observer := probeObserver(map[string]repositoryProbeClient{
		"homelab": probeWorker(t, "homelab", runner, probeCredentialRef, "secretref:f03-admin/rotated"),
	})
	now := probeNow
	observer.now = func() time.Time { return now }

	key := probeKey("homelab")
	for range 2 {
		if _, err := observer.ObserveRepository(context.Background(), key); err != nil {
			t.Fatal(err)
		}
	}
	if len(runner.calls) != 1 {
		t.Fatalf("retained evidence was not reused: %d dispatches", len(runner.calls))
	}

	rotated := key
	rotated.CredentialRefs = []string{"secretref:f03-admin/rotated"}
	if _, err := observer.ObserveRepository(context.Background(), rotated); err != nil {
		t.Fatal(err)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("a changed credential reference reused evidence: %d dispatches", len(runner.calls))
	}

	now = now.Add(backlog.RepositoryProbeEvidenceTTL)
	if _, err := observer.ObserveRepository(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if len(runner.calls) != 3 {
		t.Fatalf("evidence outlived its TTL: %d dispatches", len(runner.calls))
	}
}

// TestRepositoryProbeRefusesArgumentInjectionBeforeDispatch states that an
// option-shaped value never becomes a message at all.
func TestRepositoryProbeRefusesArgumentInjectionBeforeDispatch(t *testing.T) {
	dialled := 0
	observer := newCoordinatorRepositoryObserver(coordinatorProbeSettings(), nil, 7, nil)
	observer.dial = func(context.Context, string) (repositoryProbeClient, func() error, error) {
		dialled++
		return nil, nil, errors.New("a refused argument reached the transport")
	}
	for _, test := range []struct {
		name       string
		repository string
		ref        string
	}{
		{name: "option shaped repository", repository: "--upload-pack=touch /tmp/pwned", ref: "main"},
		{name: "option shaped ref", repository: probeRepository, ref: "--upload-pack=touch /tmp/pwned"},
		{name: "scheme not allowed", repository: "ext::sh -c touch% /tmp/pwned", ref: "main"},
		{name: "embedded credentials", repository: "https://user:secret@example.invalid/x.git", ref: "main"},
	} {
		t.Run(test.name, func(t *testing.T) {
			key := probeKey("homelab")
			key.Repository, key.Ref = test.repository, test.ref
			if _, err := observer.ObserveRepository(context.Background(), key); err == nil {
				t.Fatal("the coordinator dispatched an argument it must refuse")
			}
		})
	}
	if dialled != 0 {
		t.Fatalf("a refused argument opened %d session(s)", dialled)
	}
}

// unknownClassClient answers with a classification this coordinator does not
// know, which is what a worker running a future build would do.
type unknownClassClient struct{}

func (unknownClassClient) ObserveRepository(context.Context, workerproto.RepositoryProbeRequest) (workerproto.RepositoryObservation, error) {
	return workerproto.RepositoryObservation{Class: "repository-on-fire"}, nil
}

// TestRepositoryProbeRefusesAnUnknownClassification states that a class outside
// the closed set becomes an unobserved reachability rather than a verdict.
func TestRepositoryProbeRefusesAnUnknownClassification(t *testing.T) {
	observer := probeObserver(map[string]repositoryProbeClient{"homelab": unknownClassClient{}})
	_, err := observer.ObserveRepository(context.Background(), probeKey("homelab"))
	if err == nil || !strings.Contains(err.Error(), "unknown classification") {
		t.Fatalf("error = %v", err)
	}
	if _, found := observer.cache.Lookup(probeKey("homelab")); found {
		t.Fatal("an unusable answer was retained as evidence")
	}
}
