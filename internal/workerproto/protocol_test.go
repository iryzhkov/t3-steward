package workerproto

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var (
	testNow    = time.Date(2026, 9, 10, 20, 0, 0, 0, time.UTC)
	testSecret = []byte("0123456789abcdef0123456789abcdef")
)

func TestProtocolJSONGoldens(t *testing.T) {
	envelope := signedTestEnvelope(t, MessageCapabilities, 1, "request-1", CapabilityNegotiation{
		SupportedVersions: []int{1}, Capabilities: []string{"artifact-transfer", "commands"},
		MaxMessageBytes: 4096, MaxArtifactBytes: 1048576,
	})
	assertJSONGolden(t, "protocol-envelope.json", envelope)
}

func TestExecutionPackageJSONGolden(t *testing.T) {
	manifest, err := BuildExecutionPackageManifest(validPackage())
	if err != nil {
		t.Fatal(err)
	}
	assertJSONGolden(t, "execution-package.json", manifest)
}

func TestExchangeValidationAndIdempotency(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Envelope)
		code   ErrorCode
	}{
		{"unsupported-version", func(e *Envelope) { e.Version = 2 }, ErrorUnsupportedVersion},
		{"stale-coordinator-epoch", func(e *Envelope) { e.CoordinatorEpoch = 8 }, ErrorStaleEpoch},
		{"stale-worker-epoch", func(e *Envelope) { e.WorkerEpoch = "worker-old" }, ErrorStaleEpoch},
		{"authentication-principal", func(e *Envelope) { e.Authentication.Principal = "intruder" }, ErrorAuthentication},
		{"authentication-signature", func(e *Envelope) { e.Authentication.Signature = strings.Repeat("0", 64) }, ErrorAuthentication},
		{"authorization", func(e *Envelope) { e.Type = MessageArtifactUpload }, ErrorAuthorization},
		{"timeout", func(e *Envelope) { e.Deadline = testNow }, ErrorTimeout},
		{"payload-checksum", func(e *Envelope) { e.PayloadSHA256 = strings.Repeat("0", 64) }, ErrorMalformed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newTestServer(t)
			envelope := signedTestEnvelope(t, MessageCapabilities, 1, "request-1", map[string]string{"hello": "world"})
			tt.change(&envelope)
			if tt.name != "authentication-signature" && tt.name != "authentication-principal" {
				if err := SignEnvelope(&envelope, "ssh:coordinator", "coordinator-key", testSecret); err != nil {
					t.Fatal(err)
				}
			}
			_, err := server.Handle(context.Background(), envelope, echoHandler)
			assertProtocolCode(t, err, tt.code)
		})
	}

	server := newTestServer(t)
	var calls int
	first := signedTestEnvelope(t, MessageCapabilities, 1, "request-1", map[string]string{"value": "one"})
	response, err := server.Handle(context.Background(), first, func(ctx context.Context, envelope Envelope) (MessageType, any, error) {
		calls++
		return MessageCapabilities, json.RawMessage(envelope.Payload), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := server.Handle(context.Background(), first, func(context.Context, Envelope) (MessageType, any, error) {
		t.Fatal("duplicate request executed handler")
		return "", nil, nil
	})
	if err != nil || calls != 1 || !bytes.Equal(mustJSON(t, response), mustJSON(t, duplicate)) {
		t.Fatalf("duplicate replay mismatch: calls=%d err=%v", calls, err)
	}
	changed := signedTestEnvelope(t, MessageCapabilities, 1, "request-1", map[string]string{"value": "changed"})
	_, err = server.Handle(context.Background(), changed, echoHandler)
	assertProtocolCode(t, err, ErrorReplay)

	reordered := signedTestEnvelope(t, MessageCapabilities, 3, "request-3", map[string]string{"value": "three"})
	_, err = server.Handle(context.Background(), reordered, echoHandler)
	assertProtocolCode(t, err, ErrorReordered)
	second := signedTestEnvelope(t, MessageCapabilities, 2, "request-2", map[string]string{"value": "two"})
	if _, err := server.Handle(context.Background(), second, echoHandler); err != nil {
		t.Fatalf("ordered request failed: %v", err)
	}
}

func TestExchangeBackpressureWhileRequestIsInFlight(t *testing.T) {
	server := newTestServer(t)
	started := make(chan struct{})
	release := make(chan struct{})
	first := signedTestEnvelope(t, MessageCapabilities, 1, "request-1", struct{}{})
	done := make(chan error, 1)
	go func() {
		_, err := server.Handle(context.Background(), first, func(context.Context, Envelope) (MessageType, any, error) {
			close(started)
			<-release
			return MessageCapabilities, struct{}{}, nil
		})
		done <- err
	}()
	<-started

	duplicateErr := func() error {
		_, err := server.Handle(context.Background(), first, echoHandler)
		return err
	}()
	assertProtocolCode(t, duplicateErr, ErrorBackpressure)

	second := signedTestEnvelope(t, MessageCapabilities, 1, "request-2", struct{}{})
	second.SessionID = "session-2"
	if err := SignEnvelope(&second, "ssh:coordinator", "coordinator-key", testSecret); err != nil {
		t.Fatal(err)
	}
	_, err := server.Handle(context.Background(), second, echoHandler)
	assertProtocolCode(t, err, ErrorBackpressure)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestExchangeStructuredErrorsAndCodecLimits(t *testing.T) {
	server := newTestServer(t)
	envelope := signedTestEnvelope(t, MessageCapabilities, 1, "request-1", struct{}{})
	response, err := server.Handle(context.Background(), envelope, func(context.Context, Envelope) (MessageType, any, error) {
		return "", nil, context.Canceled
	})
	if err != nil || response.Type != MessageError {
		t.Fatalf("structured cancellation: response=%+v err=%v", response, err)
	}
	var detail ProtocolError
	if err := json.Unmarshal(response.Payload, &detail); err != nil || detail.Code != ErrorCancelled {
		t.Fatalf("structured cancellation payload: %+v err=%v", detail, err)
	}

	codec := Codec{MaxBytes: 32}
	var output bytes.Buffer
	if err := codec.Encode(&output, strings.Repeat("x", 64)); err == nil {
		t.Fatal("oversized encode accepted")
	}
	if err := codec.Decode(strings.NewReader(strings.Repeat("x", 64)), &detail); err == nil {
		t.Fatal("oversized decode accepted")
	}
	if err := (Codec{MaxBytes: 128}).Decode(strings.NewReader(`{"code":"x","unknown":true}`), &detail); err == nil {
		t.Fatal("unknown JSON field accepted")
	}
	payloadEnvelope, err := NewEnvelope(MessageCapabilities, "session", "request", "coordinator", "worker", 1, "worker-1", 1, testNow, testNow.Add(time.Minute), map[string]any{"supportedVersions": []int{1}, "unknown": true})
	if err != nil {
		t.Fatal(err)
	}
	var capabilities CapabilityNegotiation
	if err := DecodePayload(payloadEnvelope, MessageCapabilities, &capabilities); err == nil {
		t.Fatal("unknown payload field accepted")
	}
}

func TestExecutionPackageValidationAndChecksums(t *testing.T) {
	pkg := validPackage()
	manifest, err := BuildExecutionPackageManifest(pkg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateExecutionPackageManifest(manifest, 1<<20); err != nil {
		t.Fatal(err)
	}
	tampered := manifest
	tampered.Package.Identity.ThreadID = "thread-other"
	if err := ValidateExecutionPackageManifest(tampered, 1<<20); err == nil {
		t.Fatal("tampered package accepted")
	}
	for name, mutate := range map[string]func(*ExecutionPackage){
		"stale-version":  func(p *ExecutionPackage) { p.Version++ },
		"route-worker":   func(p *ExecutionPackage) { p.Route.WorkerID = "other" },
		"unsafe-input":   func(p *ExecutionPackage) { p.StaticInputs[0].Path = "../escape" },
		"duplicate-path": func(p *ExecutionPackage) { p.StaticInputs[0].Path = p.Prompt.Path },
		"invalid-limits": func(p *ExecutionPackage) { p.Limits.MaxTotalBytes = 1 },
		"deadline-order": func(p *ExecutionPackage) {
			deadline := testNow.Add(time.Hour)
			notBefore := deadline.Add(time.Hour)
			p.NotBefore, p.Deadline = &notBefore, &deadline
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := validPackage()
			mutate(&candidate)
			if err := ValidateExecutionPackage(candidate); err == nil {
				t.Fatal("invalid package accepted")
			}
		})
	}
}

func TestArtifactManifestChecksumCustodyAndArchiveSafety(t *testing.T) {
	data := []byte("artifact bytes")
	object := artifactObject("artifact-1", "dependencies/inspect/findings.md", data)
	manifest := ArtifactTransferManifest{
		Version: 1, ID: "manifest-1", Direction: "upload", CoordinatorEpoch: 9,
		WorkerID: "normandy", WorkerEpoch: "worker-1", AssignmentID: "assignment-1",
		AssignmentEpoch: 2, Objects: []ArtifactObject{object}, TotalBytes: int64(len(data)),
		CreatedAt: testNow, ExpiresAt: testNow.Add(time.Hour),
	}
	if err := ValidateArtifactTransferManifest(manifest, 1024, 2048, testNow); err != nil {
		t.Fatal(err)
	}
	if err := VerifyArtifact(bytes.NewReader(data), object, 1024); err != nil {
		t.Fatal(err)
	}
	if err := VerifyArtifact(strings.NewReader("tampered"), object, 1024); err == nil {
		t.Fatal("checksum mismatch accepted")
	}
	custody, err := BuildCustodyRecord(ArtifactCustodyRecord{
		ManifestID: manifest.ID, ObjectID: object.ID, From: "normandy", To: "coordinator",
		Sequence: 1, Size: object.Size, SHA256: object.SHA256, VerifiedAt: testNow,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateCustodyRecord(custody); err != nil {
		t.Fatal(err)
	}
	custody.Size++
	if err := ValidateCustodyRecord(custody); err == nil {
		t.Fatal("tampered custody record accepted")
	}

	for name, archive := range map[string][]byte{
		"safe":      tarBytes(t, tar.Header{Name: "outputs/result.txt", Mode: 0600, Size: 2, Typeflag: tar.TypeReg}, []byte("ok")),
		"escape":    tarBytes(t, tar.Header{Name: "../escape", Mode: 0600, Size: 2, Typeflag: tar.TypeReg}, []byte("no")),
		"symlink":   tarBytes(t, tar.Header{Name: "link", Linkname: "/etc/passwd", Typeflag: tar.TypeSymlink}, nil),
		"truncated": []byte("not a tar"),
	} {
		t.Run(name, func(t *testing.T) {
			err := ValidateTarArchive(bytes.NewReader(archive), int64(len(archive)), ArchiveLimits{MaxEntries: 10, MaxBytes: 1024})
			if name == "safe" && err != nil {
				t.Fatal(err)
			}
			if name != "safe" && err == nil {
				t.Fatal("malformed or unsafe archive accepted")
			}
		})
	}
}

func TestSSHTransportLocalMultiProcess(t *testing.T) {
	request := signedLiveEnvelope(t)
	var invokedName string
	var invokedArgs []string
	transport, err := NewSSHTransport(SSHConfig{
		Address: "local-test", RemoteCommand: "worker-exchange", RemoteArguments: []string{"control"}, RequestTimeout: 2 * time.Second,
		ConnectTimeout: time.Second, MaxMessageBytes: 64 << 10, MaxStderrBytes: 1024,
		ResponsePrincipal: "worker:normandy", ResponseKeyID: "worker-key", ResponseSecret: testSecret,
		Factory: func(ctx context.Context, name string, args ...string) *exec.Cmd {
			invokedName = name
			invokedArgs = append([]string(nil), args...)
			return helperCommand(ctx, "echo")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := transport.RoundTrip(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Type != MessageCapabilities || response.InReplyTo != request.RequestID {
		t.Fatalf("unexpected response: %+v", response)
	}
	if err := VerifyEnvelopeSignature(response, testSecret); err != nil {
		t.Fatal(err)
	}
	wantArgs := []string{
		"-oBatchMode=yes", "-oStrictHostKeyChecking=yes", "-oConnectTimeout=1",
		"--", "local-test", "worker-exchange", "control",
	}
	if invokedName != "ssh" || !slices.Equal(invokedArgs, wantArgs) {
		t.Fatalf("unsafe SSH invocation: %q %q", invokedName, invokedArgs)
	}
}

func TestSSHTransportDropRetryTimeoutCancellationAndLimits(t *testing.T) {
	request := signedLiveEnvelope(t)
	var calls atomic.Int32
	transport, err := NewSSHTransport(SSHConfig{
		Address: "local-test", RemoteCommand: "worker-exchange", RequestTimeout: 2 * time.Second,
		ConnectTimeout: time.Second, MaxMessageBytes: 64 << 10, MaxStderrBytes: 1024,
		ResponsePrincipal: "worker:normandy", ResponseKeyID: "worker-key", ResponseSecret: testSecret,
		Factory: func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
			if calls.Add(1) == 1 {
				return exec.CommandContext(ctx, os.Args[0], "-test.run=TestProtocolDropHelperProcess", "--")
			}
			return helperCommand(ctx, "echo")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := transport.RoundTripWithRetry(context.Background(), request, RetryPolicy{
		MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond,
	})
	if err != nil || response.InReplyTo != request.RequestID || calls.Load() != 2 {
		t.Fatalf("drop retry failed: calls=%d response=%+v err=%v", calls.Load(), response, err)
	}

	timeoutTransport := localHelperTransportWithLimits(t, "sleep", 40*time.Millisecond, 64<<10)
	_, err = timeoutTransport.RoundTrip(context.Background(), request)
	assertProtocolCode(t, err, ErrorTimeout)

	cancelTransport := localHelperTransportWithLimits(t, "sleep", time.Second, 64<<10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = cancelTransport.RoundTrip(ctx, request)
	assertProtocolCode(t, err, ErrorCancelled)

	limitTransport := localHelperTransportWithLimits(t, "oversize", time.Second, 1024)
	_, err = limitTransport.RoundTrip(context.Background(), request)
	assertProtocolCode(t, err, ErrorLimit)

	badAuthTransport := localHelperTransport(t, "bad-auth")
	_, err = badAuthTransport.RoundTrip(context.Background(), request)
	assertProtocolCode(t, err, ErrorAuthentication)
}

func TestSSHArtifactTransportAuthenticatesMetadataAndBoundsRawSuffix(t *testing.T) {
	request := signedLiveEnvelope(t)
	transport := localHelperTransport(t, "artifact")
	response, raw, err := transport.RoundTripArtifactWithRetry(context.Background(), request, RetryPolicy{MaxAttempts: 1}, int64(len("raw-artifact")))
	if err != nil || response.InReplyTo != request.RequestID || string(raw) != "raw-artifact" {
		t.Fatalf("artifact response=%#v raw=%q err=%v", response, raw, err)
	}
	if _, _, err := transport.RoundTripArtifactWithRetry(context.Background(), request, RetryPolicy{MaxAttempts: 1}, 2); err == nil {
		t.Fatal("oversized raw artifact response was accepted")
	}
}

func TestSSHTransportRejectsUnsafeInvocation(t *testing.T) {
	if _, err := NewSSHTransport(SSHConfig{
		Address: "-oProxyCommand=bad", RemoteCommand: "worker-exchange",
		RequestTimeout: time.Second, ConnectTimeout: time.Second, MaxMessageBytes: 1, MaxStderrBytes: 1,
	}); err == nil {
		t.Fatal("option-like address accepted")
	}
	if _, err := NewSSHTransport(SSHConfig{
		Address: "worker", RemoteCommand: "worker-exchange;bad",
		RequestTimeout: time.Second, ConnectTimeout: time.Second, MaxMessageBytes: 1, MaxStderrBytes: 1,
	}); err == nil {
		t.Fatal("shell-bearing remote command accepted")
	}
	if _, err := NewSSHTransport(SSHConfig{
		Address: "worker", RemoteCommand: "worker-exchange", RemoteArguments: []string{"--bad"},
		RequestTimeout: time.Second, ConnectTimeout: time.Second, MaxMessageBytes: 1, MaxStderrBytes: 1,
	}); err == nil {
		t.Fatal("option-like remote argument accepted")
	}
}

func TestProtocolHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_WORKERPROTO_HELPER") != "1" {
		return
	}
	mode := os.Getenv("WORKERPROTO_HELPER_MODE")
	switch mode {
	case "sleep":
		time.Sleep(5 * time.Second)
		os.Exit(0)
	case "oversize":
		_, _ = os.Stdout.Write(bytes.Repeat([]byte("x"), 4096))
		os.Exit(0)
	case "echo", "bad-auth", "artifact":
	default:
		os.Exit(3)
	}
	server := newLiveTestServer()
	if mode == "bad-auth" {
		server.config.SignerSecret = []byte("abcdef0123456789abcdef0123456789")
	}
	err := ServeOne(os.Stdin, os.Stdout, Codec{MaxBytes: 64 << 10}, server, echoHandler)
	if err != nil {
		_, _ = os.Stderr.WriteString(err.Error())
		os.Exit(2)
	}
	if mode == "artifact" {
		_, _ = os.Stdout.WriteString("raw-artifact")
	}
	os.Exit(0)
}

func TestProtocolDropHelperProcess(t *testing.T) {
	if len(os.Args) > 0 && strings.Contains(strings.Join(os.Args, " "), "TestProtocolDropHelperProcess") &&
		os.Getenv("GO_WANT_WORKERPROTO_HELPER") == "" {
		os.Exit(17)
	}
}

func TestThrottleProtocolPayloadRoundTrip(t *testing.T) {
	deadline := testNow.Add(time.Minute)
	command := domain.ThrottleCommand{
		ID: "throttle-1", DirectiveID: "directive-1", AttemptID: "attempt-1",
		AssignmentID: "assignment-1", AssignmentEpoch: 2, WorkerID: "normandy",
		ThreadID: "thread-1", WorkspacePath: "/tmp/workspace",
		Route: domain.ProviderRoute{WorkerID: "normandy", ProviderInstanceID: "codex", Model: "gpt-5.6-sol", QuotaPoolID: "codex-main"},
		Kind:  domain.ThrottleCommandDrain, QuotaPoolID: "codex-main", Reason: "quota draining",
		Deadline: &deadline, CreatedAt: testNow,
	}
	envelope, err := NewEnvelope(
		MessageThrottleCommands, "session-1", "request-throttle", "coordinator", "normandy",
		9, "worker-1", 1, testNow, deadline, ThrottleDelivery{Commands: []domain.ThrottleCommand{command}},
	)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ThrottleDelivery
	if err := DecodePayload(envelope, MessageThrottleCommands, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Commands) != 1 || !reflect.DeepEqual(decoded.Commands[0], command) {
		t.Fatalf("decoded throttle delivery = %+v", decoded)
	}
	ack := domain.ThrottleAcknowledgement{
		CommandID: command.ID, AttemptID: command.AttemptID, Accepted: true,
		Result: domain.ThrottleResultCheckpointed, AcknowledgedAt: testNow,
	}
	response, err := NewEnvelope(
		MessageThrottleAcknowledgements, "session-1", "response-throttle", "normandy", "coordinator",
		9, "worker-1", 1, testNow, deadline,
		ThrottleAcknowledgements{Acknowledgements: []domain.ThrottleAcknowledgement{ack}},
	)
	if err != nil {
		t.Fatal(err)
	}
	var decodedAcks ThrottleAcknowledgements
	if err := DecodePayload(response, MessageThrottleAcknowledgements, &decodedAcks); err != nil {
		t.Fatal(err)
	}
	if len(decodedAcks.Acknowledgements) != 1 || !reflect.DeepEqual(decodedAcks.Acknowledgements[0], ack) {
		t.Fatalf("decoded throttle acknowledgements = %+v", decodedAcks)
	}
}

func validPackage() ExecutionPackage {
	prompt := artifactObject("artifact-prompt", "prompt/task.md", []byte("do work"))
	input := artifactObject("artifact-input", "inputs/context.md", []byte("context"))
	dependency := artifactObject("artifact-dependency", "dependencies/inspect/findings.md", []byte("findings"))
	deadline := testNow.Add(2 * time.Hour)
	expiry := testNow.Add(3 * time.Hour)
	return ExecutionPackage{
		Version: 1, ID: "package-1", CoordinatorID: "coordinator", CoordinatorEpoch: 9,
		WorkerID: "normandy", WorkerEpoch: "worker-1",
		Identity: ExecutionIdentity{
			WorkflowID: "workflow-1", WorkflowRunID: "run-1", TaskID: "task-1",
			AttemptID: "attempt-1", AssignmentID: "assignment-1", AssignmentEpoch: 2,
			DispatchToken: "dispatch-1", ThreadID: "thread-1",
		},
		Class: domain.TaskClassRequired, Prompt: prompt, StaticInputs: []ArtifactObject{input},
		Dependencies: []DependencyInput{{TaskID: "inspect", Artifacts: []ArtifactObject{dependency}}},
		Route: domain.ProviderRoute{
			WorkerID: "normandy", ProviderInstanceID: "codex", Model: "gpt-5.6-sol",
			Options: map[string]string{"effort": "high"}, QuotaPoolID: "codex-main",
		},
		Environment: EnvironmentReference{
			CatalogRevision: "catalog-1", Project: "t3-steward", Repository: "github.com/iryzhkov/t3-steward",
			Ref: "ce1fd4d", Scope: "task", SetupProfile: "go", T3Project: "development",
			ResourceLocks: []string{"repo:t3-steward"}, RequiredCredentials: []string{"git:github"},
		},
		Verification: []string{"go test ./..."}, Outputs: []domain.ArtifactDeclaration{{Name: "findings.md", MediaType: "text/markdown"}},
		Deadline: &deadline, ExpiresAt: &expiry,
		Limits: ExecutionLimits{
			MaxTurns: 12, PrepareTimeout: time.Minute, VerificationTimeout: 10 * time.Minute,
			MaxArtifactBytes: 1 << 20, MaxTotalBytes: 4 << 20,
		},
		CreatedAt: testNow,
	}
}

func artifactObject(id, path string, data []byte) ArtifactObject {
	sum := sha256.Sum256(data)
	return ArtifactObject{
		ID: id, Path: path, Kind: "input", MediaType: "text/plain",
		Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:]),
	}
}

func signedTestEnvelope(t *testing.T, kind MessageType, sequence int64, requestID string, payload any) Envelope {
	t.Helper()
	envelope, err := NewEnvelope(kind, "session-1", requestID, "coordinator", "normandy", 9, "worker-1", sequence, testNow, testNow.Add(time.Minute), payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := SignEnvelope(&envelope, "ssh:coordinator", "coordinator-key", testSecret); err != nil {
		t.Fatal(err)
	}
	return envelope
}

func signedLiveEnvelope(t *testing.T) Envelope {
	t.Helper()
	now := time.Now().UTC()
	envelope, err := NewEnvelope(MessageCapabilities, "session-live", "request-live", "coordinator", "normandy", 9, "worker-1", 1, now, now.Add(10*time.Second), CapabilityNegotiation{SupportedVersions: []int{1}, MaxMessageBytes: 64 << 10, MaxArtifactBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := SignEnvelope(&envelope, "ssh:coordinator", "coordinator-key", testSecret); err != nil {
		t.Fatal(err)
	}
	return envelope
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	server, err := NewServer(ServerConfig{
		CoordinatorID: "coordinator", WorkerID: "normandy", CoordinatorEpoch: 9, WorkerEpoch: "worker-1",
		PeerPrincipal: "ssh:coordinator", PeerKeyID: "coordinator-key", PeerSecret: testSecret,
		SignerPrincipal: "worker:normandy", SignerKeyID: "worker-key", SignerSecret: testSecret,
		Allowed: map[MessageType]bool{MessageCapabilities: true}, Now: func() time.Time { return testNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func newLiveTestServer() *Server {
	server, err := NewServer(ServerConfig{
		CoordinatorID: "coordinator", WorkerID: "normandy", CoordinatorEpoch: 9, WorkerEpoch: "worker-1",
		PeerPrincipal: "ssh:coordinator", PeerKeyID: "coordinator-key", PeerSecret: testSecret,
		SignerPrincipal: "worker:normandy", SignerKeyID: "worker-key", SignerSecret: testSecret,
		Allowed: map[MessageType]bool{MessageCapabilities: true}, MaxClockSkew: time.Minute,
	})
	if err != nil {
		panic(err)
	}
	return server
}

func echoHandler(_ context.Context, envelope Envelope) (MessageType, any, error) {
	return envelope.Type, json.RawMessage(envelope.Payload), nil
}

func localHelperTransport(t *testing.T, mode string) *SSHTransport {
	return localHelperTransportWithLimits(t, mode, 2*time.Second, 64<<10)
}

func localHelperTransportWithLimits(t *testing.T, mode string, timeout time.Duration, maxBytes int64) *SSHTransport {
	t.Helper()
	transport, err := NewSSHTransport(SSHConfig{
		Address: "local-test", RemoteCommand: "worker-exchange", RequestTimeout: timeout,
		ConnectTimeout: time.Second, MaxMessageBytes: maxBytes, MaxStderrBytes: 1024,
		ResponsePrincipal: "worker:normandy", ResponseKeyID: "worker-key", ResponseSecret: testSecret,
		Factory: func(ctx context.Context, _ string, _ ...string) *exec.Cmd { return helperCommand(ctx, mode) },
	})
	if err != nil {
		t.Fatal(err)
	}
	return transport
}

func helperCommand(ctx context.Context, mode string) *exec.Cmd {
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=TestProtocolHelperProcess", "--")
	command.Env = append(os.Environ(), "GO_WANT_WORKERPROTO_HELPER=1", "WORKERPROTO_HELPER_MODE="+mode)
	return command
}

func assertProtocolCode(t *testing.T, err error, code ErrorCode) {
	t.Helper()
	var protocolErr *ProtocolError
	if !errors.As(err, &protocolErr) || protocolErr.Code != code {
		t.Fatalf("error=%v, want protocol code %s", err, code)
	}
}

func assertJSONGolden(t *testing.T, name string, value any) {
	t.Helper()
	actual, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	actual = append(actual, '\n')
	expected, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("%v\nactual:\n%s", err, actual)
	}
	if !bytes.Equal(actual, expected) {
		t.Fatalf("%s mismatch\nactual:\n%s", name, actual)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func tarBytes(t *testing.T, header tar.Header, data []byte) []byte {
	t.Helper()
	var output bytes.Buffer
	writer := tar.NewWriter(&output)
	if err := writer.WriteHeader(&header); err != nil {
		t.Fatal(err)
	}
	if len(data) > 0 {
		if _, err := writer.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}
