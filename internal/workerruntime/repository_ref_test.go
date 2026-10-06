package workerruntime

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

const (
	refResolveObjectID = "a90c0978567908625629368af0749936c9ff6b92"
	refResolveRef      = "refs/heads/steward/run-x/task-y/cp-1"
)

func refResolveRequest() workerproto.RepositoryRefRequest {
	return workerproto.RepositoryRefRequest{
		Repository: "https://github.com/owner/project",
		Ref:        refResolveRef,
	}
}

// TestWorkerResolvesAnExactRef is the happy path on the worker: one fixed
// argument vector, the worker's own runner and runs root, the credential
// references checked first, and the object ID of the identical ref name only.
func TestWorkerResolvesAnExactRef(t *testing.T) {
	runner := &repositoryProbeRunner{output: "b90c0978567908625629368af0749936c9ff6b92\trefs/heads/x/" + refResolveRef + "\n" +
		refResolveObjectID + "\t" + refResolveRef + "\n"}
	credentials := &repositoryProbeCredentials{available: map[string]bool{"secretref:f03-admin/homelab": true}}
	driver := repositoryProbeDriver(t, runner, credentials)
	request := refResolveRequest()
	request.CredentialRefs = []string{"secretref:f03-admin/homelab"}

	resolution, err := driver.ResolveRepositoryRef(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Status != workerproto.RefResolutionResolved || resolution.ObjectID != refResolveObjectID ||
		resolution.Ref != refResolveRef || !resolution.CredentialsResolved || !resolution.ObservedAt.Equal(runtimeTestNow) {
		t.Fatalf("resolution = %+v", resolution)
	}
	if err := workerproto.ValidateRepositoryRefResolution(request, resolution); err != nil {
		t.Fatal(err)
	}
	if len(credentials.asked) != 1 || len(runner.calls) != 1 {
		t.Fatalf("credentials asked %d times, runs %d", len(credentials.asked), len(runner.calls))
	}
	call := runner.calls[0]
	want := []string{"ls-remote", "--exit-code", "--", request.Repository, request.Ref}
	if call.Program != "git" || strings.Join(call.Args, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("argv = %s %v, want git %v", call.Program, call.Args, want)
	}
	if call.Dir != driver.Config.RunsRoot || call.MaxOutputBytes != workerproto.MaxRepositoryProbeOutputBytes {
		t.Fatalf("ran in %q with bound %d", call.Dir, call.MaxOutputBytes)
	}
}

// TestWorkerRefResolutionStructuredOutcomes states the closed answer set: a
// missing ref, an unreachable remote, a timeout and an invalid name are each a
// structured answer and never an object ID.
func TestWorkerRefResolutionStructuredOutcomes(t *testing.T) {
	tests := []struct {
		name    string
		runner  *repositoryProbeRunner
		ref     string
		timeout int
		status  workerproto.RefResolutionStatus
		class   backlog.RepositoryReachability
		runs    int
	}{
		{
			name:   "only a similar ref is a missing ref",
			runner: &repositoryProbeRunner{output: "b90c0978567908625629368af0749936c9ff6b92\trefs/heads/x/" + refResolveRef + "\n"},
			ref:    refResolveRef, status: workerproto.RefResolutionNotFound, class: backlog.RepositoryRefNotFound, runs: 1,
		},
		{
			name:   "exit two is a missing ref",
			runner: &repositoryProbeRunner{exit: 2, err: &backlog.ProcessExitError{ExitCode: 2}},
			ref:    refResolveRef, status: workerproto.RefResolutionNotFound, class: backlog.RepositoryRefNotFound, runs: 1,
		},
		{
			name: "an unresolvable host is unreachable",
			runner: &repositoryProbeRunner{exit: 128, err: &backlog.ProcessExitError{ExitCode: 128},
				output: "fatal: unable to access 'https://github.com/owner/project/': Could not resolve host: github.com\n"},
			ref: refResolveRef, status: workerproto.RefResolutionUnreachable, class: backlog.RepositoryDNSFailure, runs: 1,
		},
		{
			name: "a missing repository is unreachable, not a missing ref",
			runner: &repositoryProbeRunner{exit: 128, err: &backlog.ProcessExitError{ExitCode: 128},
				output: "remote: Repository not found.\nfatal: repository 'https://github.com/owner/project/' not found\n"},
			ref: refResolveRef, status: workerproto.RefResolutionUnreachable, class: backlog.RepositoryNotFound, runs: 1,
		},
		{
			name:   "a remote that never answers times out",
			runner: &repositoryProbeRunner{block: true}, timeout: 1,
			ref: refResolveRef, status: workerproto.RefResolutionUnreachable, class: backlog.RepositoryProbeTimeout, runs: 1,
		},
		{
			name:   "a glob is an invalid ref",
			runner: &repositoryProbeRunner{},
			ref:    "refs/heads/steward/*", status: workerproto.RefResolutionInvalidRef,
		},
		{
			name:   "a short name is an invalid ref",
			runner: &repositoryProbeRunner{},
			ref:    "main", status: workerproto.RefResolutionInvalidRef,
		},
		{
			name:   "a lock component is an invalid ref",
			runner: &repositoryProbeRunner{},
			ref:    "refs/heads/a.lock/b", status: workerproto.RefResolutionInvalidRef,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			driver := repositoryProbeDriver(t, test.runner, nil)
			request := refResolveRequest()
			request.Ref = test.ref
			request.TimeoutSeconds = test.timeout
			started := time.Now()
			resolution, err := driver.ResolveRepositoryRef(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if time.Since(started) > 30*time.Second {
				t.Fatal("resolution was not bounded")
			}
			if resolution.Status != test.status || resolution.ObjectID != "" || resolution.Ref != test.ref {
				t.Fatalf("resolution = %+v, want status %q", resolution, test.status)
			}
			if test.class != "" && resolution.Class != string(test.class) {
				t.Fatalf("class = %q, want %q", resolution.Class, test.class)
			}
			if len(test.runner.calls) != test.runs {
				t.Fatalf("runs = %d, want %d", len(test.runner.calls), test.runs)
			}
			if err := workerproto.ValidateRepositoryRefResolution(request, resolution); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestWorkerRefResolutionRefusesWithoutItsCredentialReferences mirrors the
// probe: a worker that cannot present the project's references answers nothing
// rather than an object ID observed under some other identity.
func TestWorkerRefResolutionRefusesWithoutItsCredentialReferences(t *testing.T) {
	runner := &repositoryProbeRunner{output: refResolveObjectID + "\t" + refResolveRef + "\n"}
	request := refResolveRequest()
	request.CredentialRefs = []string{"secretref:f03-admin/homelab"}
	for _, credentials := range []CredentialChecker{&repositoryProbeCredentials{available: map[string]bool{}}, nil} {
		driver := repositoryProbeDriver(t, runner, credentials)
		if _, err := driver.ResolveRepositoryRef(context.Background(), request); err == nil {
			t.Fatal("a worker without the credential reference resolved a ref")
		}
	}
	if len(runner.calls) != 0 {
		t.Fatalf("resolution ran %d time(s) without its credential references", len(runner.calls))
	}
}

// TestWorkerRefResolutionRefusesOptionShapedValues states that an option-shaped
// or disallowed value never reaches a process on the dispatched path.
func TestWorkerRefResolutionRefusesOptionShapedValues(t *testing.T) {
	for _, request := range []workerproto.RepositoryRefRequest{
		{Repository: "--upload-pack=touch /tmp/pwned", Ref: refResolveRef},
		{Repository: "https://example.invalid/x.git", Ref: "--upload-pack=touch /tmp/pwned"},
		{Repository: "file:///etc", Ref: refResolveRef},
		{Repository: "ext::sh -c touch% /tmp/pwned", Ref: refResolveRef},
	} {
		runner := &repositoryProbeRunner{}
		driver := repositoryProbeDriver(t, runner, nil)
		resolution, err := driver.ResolveRepositoryRef(context.Background(), request)
		if err == nil && resolution.Status != workerproto.RefResolutionInvalidRef {
			t.Fatalf("%+v accepted: %+v", request, resolution)
		}
		if len(runner.calls) != 0 {
			t.Fatalf("%+v started %d process(es)", request, len(runner.calls))
		}
	}
}

// refResolvingDriver is a driver that can resolve refs; fakeDriver alone cannot.
type refResolvingDriver struct {
	*fakeDriver
	resolution workerproto.RepositoryRefResolution
	requests   []workerproto.RepositoryRefRequest
}

func (d *refResolvingDriver) ResolveRepositoryRef(_ context.Context, request workerproto.RepositoryRefRequest) (workerproto.RepositoryRefResolution, error) {
	d.requests = append(d.requests, request)
	return d.resolution, nil
}

func refResolveExchange(t *testing.T, driver Driver, allowed workerproto.MessageType) Exchange {
	t.Helper()
	journal, err := OpenJournal(t.TempDir(), "normandy", "worker-1", 9)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := New(testConfig(func() time.Time { return runtimeTestNow }), journal, driver)
	if err != nil {
		t.Fatal(err)
	}
	server, err := workerproto.NewServer(workerproto.ServerConfig{
		CoordinatorID: "coordinator", WorkerID: "normandy", CoordinatorEpoch: 9, WorkerEpoch: "worker-1",
		PeerPrincipal: "ssh:coordinator", PeerKeyID: "coordinator-key", PeerSecret: []byte("coordinator-secret"),
		SignerPrincipal: "ssh:normandy", SignerKeyID: "worker-key", SignerSecret: []byte("worker-response-secret"),
		Allowed: map[workerproto.MessageType]bool{allowed: true},
		Now:     func() time.Time { return runtimeTestNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	return Exchange{Runtime: runtime, Server: server}
}

func signedRefResolveEnvelope(t *testing.T, request workerproto.RepositoryRefRequest) workerproto.Envelope {
	t.Helper()
	envelope, err := workerproto.NewEnvelope(
		workerproto.MessageRepositoryRefResolve, "session-1", "request-1", "coordinator", "normandy",
		9, "worker-1", 1, runtimeTestNow, runtimeTestNow.Add(time.Minute), request,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := workerproto.SignEnvelope(&envelope, "ssh:coordinator", "coordinator-key", []byte("coordinator-secret")); err != nil {
		t.Fatal(err)
	}
	return envelope
}

// TestExchangeAnswersARefResolution states that the message is routed to the
// driver and the signed answer is the resolution.
func TestExchangeAnswersARefResolution(t *testing.T) {
	driver := &refResolvingDriver{fakeDriver: &fakeDriver{}, resolution: workerproto.RepositoryRefResolution{
		Ref: refResolveRef, Status: workerproto.RefResolutionResolved, ObjectID: refResolveObjectID, ObservedAt: runtimeTestNow,
	}}
	exchange := refResolveExchange(t, driver, workerproto.MessageRepositoryRefResolve)
	response, err := exchange.Handle(context.Background(), signedRefResolveEnvelope(t, refResolveRequest()))
	if err != nil {
		t.Fatal(err)
	}
	if response.Type != workerproto.MessageRepositoryRefResolution {
		t.Fatalf("response type = %q", response.Type)
	}
	if err := workerproto.VerifyEnvelopeSignature(response, []byte("worker-response-secret")); err != nil {
		t.Fatal(err)
	}
	var resolution workerproto.RepositoryRefResolution
	if err := workerproto.DecodePayload(response, workerproto.MessageRepositoryRefResolution, &resolution); err != nil {
		t.Fatal(err)
	}
	if resolution.ObjectID != refResolveObjectID || len(driver.requests) != 1 || driver.requests[0].Ref != refResolveRef {
		t.Fatalf("resolution = %+v, driver saw %+v", resolution, driver.requests)
	}
}

// TestExchangeRefusesARefResolutionItCannotAnswer states that a driver without
// the capability answers with an error rather than an object ID.
func TestExchangeRefusesARefResolutionItCannotAnswer(t *testing.T) {
	exchange := refResolveExchange(t, &fakeDriver{}, workerproto.MessageRepositoryRefResolve)
	response, err := exchange.Handle(context.Background(), signedRefResolveEnvelope(t, refResolveRequest()))
	if err != nil {
		t.Fatal(err)
	}
	if response.Type != workerproto.MessageError {
		t.Fatalf("response type = %q, want an error", response.Type)
	}
}

// TestExchangeRefusesAnUnlistedRefResolution states that a worker whose
// allowlist predates the message refuses it as unauthorized, which is what an
// older worker build does.
func TestExchangeRefusesAnUnlistedRefResolution(t *testing.T) {
	exchange := refResolveExchange(t, &refResolvingDriver{fakeDriver: &fakeDriver{}}, workerproto.MessageRepositoryProbe)
	_, err := exchange.Handle(context.Background(), signedRefResolveEnvelope(t, refResolveRequest()))
	var protocolErr *workerproto.ProtocolError
	if !errors.As(err, &protocolErr) || protocolErr.Code != workerproto.ErrorAuthorization {
		t.Fatalf("error = %v, want an authorization refusal", err)
	}
}

// TestWorkerAdvertisesRefResolution states that this build tells the
// coordinator it understands the message, which is the only way the
// coordinator will ever send it.
func TestWorkerAdvertisesRefResolution(t *testing.T) {
	if !slices.Contains(AdvertisedCapabilities(nil), workerproto.CapabilityRepositoryRefResolve) {
		t.Fatalf("capabilities = %v", AdvertisedCapabilities(nil))
	}
}
