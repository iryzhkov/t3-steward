package workerruntime

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type staticProtocolResolver struct {
	credentials ProtocolCredentials
	err         error
	reference   string
}

func (r *staticProtocolResolver) ResolveProtocol(_ context.Context, reference string) (ProtocolCredentials, error) {
	r.reference = reference
	return r.credentials, r.err
}

func TestWorkerServiceComposesRestrictedAuthenticatedExchange(t *testing.T) {
	settings := testWorkerServiceSettings(t)
	resolver := &staticProtocolResolver{credentials: testProtocolCredentials()}
	service, err := NewWorkerService(context.Background(), WorkerServiceOptions{
		Settings:            settings,
		WorkerID:            "normandy",
		WorkerEpoch:         "worker-1",
		CoordinatorEpoch:    9,
		ProtocolCredentials: resolver,
		DryRun:              true,
		Now:                 func() time.Time { return runtimeTestNow },
	})
	if err != nil {
		t.Fatalf("new worker service: %v", err)
	}
	if resolver.reference != "worker-auth" {
		t.Fatalf("resolved reference = %q", resolver.reference)
	}

	request := signedWorkerRequest(t, workerproto.MessageSnapshot, 1, workerproto.SnapshotRequest{})
	var output bytes.Buffer
	if err := service.Serve(context.Background(), bytes.NewReader(mustEncodeEnvelope(t, service.Codec, request)), &output); err != nil {
		t.Fatalf("serve snapshot: %v", err)
	}
	var response workerproto.Envelope
	if err := service.Codec.Decode(&output, &response); err != nil {
		t.Fatal(err)
	}
	if response.Type != workerproto.MessageObservations || response.InReplyTo != request.RequestID {
		t.Fatalf("response identity = %#v", response)
	}
	if err := workerproto.VerifyEnvelopeSignature(response, []byte("worker-response-secret")); err != nil {
		t.Fatalf("verify response: %v", err)
	}
}

func TestWorkerServiceRejectsCredentialPrincipalMismatch(t *testing.T) {
	settings := testWorkerServiceSettings(t)
	credentials := testProtocolCredentials()
	credentials.WorkerPrincipal = "ssh:other"
	if _, err := NewWorkerService(context.Background(), WorkerServiceOptions{
		Settings:            settings,
		WorkerID:            "normandy",
		WorkerEpoch:         "worker-1",
		CoordinatorEpoch:    9,
		ProtocolCredentials: &staticProtocolResolver{credentials: credentials},
		DryRun:              true,
		Now:                 func() time.Time { return runtimeTestNow },
	}); err == nil {
		t.Fatal("credential principal mismatch accepted")
	}
}

func serveWorkerRequest(t *testing.T, service *WorkerService, request workerproto.Envelope) workerproto.Envelope {
	t.Helper()
	var output bytes.Buffer
	if err := service.Serve(context.Background(), bytes.NewReader(mustEncodeEnvelope(t, service.Codec, request)), &output); err != nil {
		t.Fatalf("serve %s: %v", request.Type, err)
	}
	var response workerproto.Envelope
	if err := service.Codec.Decode(&output, &response); err != nil {
		t.Fatal(err)
	}
	if err := workerproto.VerifyEnvelopeSignature(response, []byte("worker-response-secret")); err != nil {
		t.Fatal(err)
	}
	return response
}

func signedWorkerRequest(t *testing.T, kind workerproto.MessageType, sequence int64, payload any) workerproto.Envelope {
	t.Helper()
	envelope, err := workerproto.NewEnvelope(
		kind,
		"session-1",
		"request-"+shortDigest([]byte(string(kind))),
		"coordinator",
		"normandy",
		9,
		"worker-1",
		sequence,
		runtimeTestNow,
		runtimeTestNow.Add(time.Minute),
		payload,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := workerproto.SignEnvelope(&envelope, "ssh:coordinator", "coordinator-key", []byte("coordinator-secret")); err != nil {
		t.Fatal(err)
	}
	return envelope
}

func mustEncodeEnvelope(t *testing.T, codec workerproto.Codec, envelope workerproto.Envelope) []byte {
	t.Helper()
	var input bytes.Buffer
	if err := codec.Encode(&input, envelope); err != nil {
		t.Fatal(err)
	}
	return input.Bytes()
}

func testProtocolCredentials() ProtocolCredentials {
	return ProtocolCredentials{
		CoordinatorPrincipal: "ssh:coordinator",
		CoordinatorKeyID:     "coordinator-key",
		CoordinatorSecret:    []byte("coordinator-secret"),
		WorkerPrincipal:      "ssh:normandy",
		WorkerKeyID:          "worker-key",
		WorkerSecret:         []byte("worker-response-secret"),
	}
}

func testWorkerServiceSettings(t *testing.T) config.BacklogV2 {
	t.Helper()
	root := t.TempDir()
	return config.BacklogV2{
		Coordinator: config.V2Coordinator{ID: "coordinator"},
		Workers: map[string]config.V2Worker{
			"normandy": {
				Address:       "normandy",
				AcceptBacklog: true,
				Capabilities:  []string{"internet"},
				Providers: map[string]config.V2Provider{
					"codex": {Models: []string{"gpt-5.6-sol"}, QuotaPool: "codex-main"},
				},
				Credential: "worker-auth",
			},
		},
		Projects: map[string]config.V2Project{
			"steward": {
				Repository:   "https://example.com/steward.git",
				DefaultRef:   "main",
				T3Project:    "development",
				SetupProfile: "go",
				Workers:      []string{"normandy"},
			},
		},
		SetupProfiles: map[string]config.V2SetupProfile{
			"go": {Commands: []string{"true"}, Timeout: config.Duration(time.Minute)},
		},
		Storage: config.V2Storage{
			Artifacts:  filepath.Join(root, "artifacts"),
			Workspaces: filepath.Join(root, "workspaces"),
		},
		Transport:     config.V2Transport{Kind: "ssh", RequestTimeout: config.Duration(time.Minute)},
		MessageLimits: config.V2MessageLimits{MaxBytes: 2 << 20, MaxArtifactBytes: 1 << 20},
		Freshness:     config.V2Freshness{WorkerMaxAge: config.Duration(2 * time.Minute)},
		Leases:        config.V2Leases{Duration: config.Duration(5 * time.Minute)},
	}
}
