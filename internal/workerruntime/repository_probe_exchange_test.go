package workerruntime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// probingDriver is a driver that can also answer a reachability probe. The
// capability is optional, so the exchange has to work with a driver that has it
// and refuse cleanly for one that does not.
type probingDriver struct {
	*fakeDriver
	observation workerproto.RepositoryObservation
	err         error
	requests    []workerproto.RepositoryProbeRequest
}

func (d *probingDriver) ObserveRepository(_ context.Context, request workerproto.RepositoryProbeRequest) (workerproto.RepositoryObservation, error) {
	d.requests = append(d.requests, request)
	return d.observation, d.err
}

func repositoryProbeExchange(t *testing.T, driver Driver) Exchange {
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
		Allowed: map[workerproto.MessageType]bool{workerproto.MessageRepositoryProbe: true},
		Now:     func() time.Time { return runtimeTestNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	return Exchange{Runtime: runtime, Server: server}
}

func signedRepositoryProbeEnvelope(t *testing.T, request workerproto.RepositoryProbeRequest) workerproto.Envelope {
	t.Helper()
	envelope, err := workerproto.NewEnvelope(
		workerproto.MessageRepositoryProbe, "session-1", "request-1", "coordinator", "normandy",
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

// TestExchangeAnswersARepositoryProbe states that the message is routed, the
// answer is signed, and the payload is the classified observation.
func TestExchangeAnswersARepositoryProbe(t *testing.T) {
	driver := &probingDriver{
		fakeDriver: &fakeDriver{},
		observation: workerproto.RepositoryObservation{
			Class: string(backlog.RepositoryAuthenticatedOK), CredentialsResolved: true,
		},
	}
	exchange := repositoryProbeExchange(t, driver)
	request := workerproto.RepositoryProbeRequest{
		Repository: "https://github.com/owner/project", Ref: "refs/heads/main",
		CredentialRefs: []string{"secretref:f03-admin/homelab"},
	}
	response, err := exchange.Handle(context.Background(), signedRepositoryProbeEnvelope(t, request))
	if err != nil {
		t.Fatal(err)
	}
	if response.Type != workerproto.MessageRepositoryObservation {
		t.Fatalf("response type = %q", response.Type)
	}
	if err := workerproto.VerifyEnvelopeSignature(response, []byte("worker-response-secret")); err != nil {
		t.Fatal(err)
	}
	var observation workerproto.RepositoryObservation
	if err := workerproto.DecodePayload(response, workerproto.MessageRepositoryObservation, &observation); err != nil {
		t.Fatal(err)
	}
	if observation.Class != string(backlog.RepositoryAuthenticatedOK) {
		t.Fatalf("observation = %+v", observation)
	}
	if len(driver.requests) != 1 || driver.requests[0].Repository != request.Repository ||
		driver.requests[0].Ref != request.Ref || len(driver.requests[0].CredentialRefs) != 1 {
		t.Fatalf("the driver saw %+v", driver.requests)
	}
}

// TestExchangeRefusesARepositoryProbeItCannotAnswer states the degradation
// contract on the worker side: a driver that cannot observe reachability says so
// and never answers with a reachability it did not observe.
func TestExchangeRefusesARepositoryProbeItCannotAnswer(t *testing.T) {
	exchange := repositoryProbeExchange(t, &fakeDriver{})
	response, err := exchange.Handle(context.Background(), signedRepositoryProbeEnvelope(t,
		workerproto.RepositoryProbeRequest{Repository: "https://github.com/owner/project", Ref: "refs/heads/main"}))
	if err != nil {
		t.Fatal(err)
	}
	if response.Type != workerproto.MessageError {
		t.Fatalf("response type = %q, want an error", response.Type)
	}
}

// TestExchangeRefusesAnUnlistedRepositoryProbe states that the allowlist governs
// this message like every other: a valid signature is not authorization.
func TestExchangeRefusesAnUnlistedRepositoryProbe(t *testing.T) {
	journal, err := OpenJournal(t.TempDir(), "normandy", "worker-1", 9)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := New(testConfig(func() time.Time { return runtimeTestNow }), journal, &fakeDriver{})
	if err != nil {
		t.Fatal(err)
	}
	server, err := workerproto.NewServer(workerproto.ServerConfig{
		CoordinatorID: "coordinator", WorkerID: "normandy", CoordinatorEpoch: 9, WorkerEpoch: "worker-1",
		PeerPrincipal: "ssh:coordinator", PeerKeyID: "coordinator-key", PeerSecret: []byte("coordinator-secret"),
		SignerPrincipal: "ssh:normandy", SignerKeyID: "worker-key", SignerSecret: []byte("worker-response-secret"),
		Allowed: map[workerproto.MessageType]bool{workerproto.MessageSnapshot: true},
		Now:     func() time.Time { return runtimeTestNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	exchange := Exchange{Runtime: runtime, Server: server}
	_, err = exchange.Handle(context.Background(), signedRepositoryProbeEnvelope(t,
		workerproto.RepositoryProbeRequest{Repository: "https://github.com/owner/project", Ref: "refs/heads/main"}))
	if err == nil {
		t.Fatal("an unlisted message kind was handled")
	}
	var protocolErr *workerproto.ProtocolError
	if !errors.As(err, &protocolErr) || protocolErr.Code != workerproto.ErrorAuthorization {
		t.Fatalf("error = %v, want an authorization refusal", err)
	}
}
