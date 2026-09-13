package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

type coordinatorResultControlTransport struct {
	upload workerproto.ArtifactUploadResponse
	acked  bool
}

func (t *coordinatorResultControlTransport) RoundTripWithRetry(_ context.Context, request workerproto.Envelope, _ workerproto.RetryPolicy) (workerproto.Envelope, error) {
	switch request.Type {
	case workerproto.MessageArtifactPoll:
		return coordinatorResultResponse(request, workerproto.MessageArtifactAnnouncement, workerproto.ArtifactAnnouncement{Upload: &t.upload})
	case workerproto.MessageArtifactAcknowledge:
		t.acked = true
		return coordinatorResultResponse(request, workerproto.MessageArtifactAcknowledged, workerproto.ArtifactAcknowledgement{ManifestID: t.upload.Manifest.ID})
	default:
		return workerproto.Envelope{}, errors.New("unexpected control request")
	}
}

type coordinatorResultArtifactTransport struct {
	upload workerproto.ArtifactUploadResponse
	raw    []byte
}

func (t coordinatorResultArtifactTransport) RoundTripWithRetry(context.Context, workerproto.Envelope, workerproto.RetryPolicy) (workerproto.Envelope, error) {
	return workerproto.Envelope{}, errors.New("unexpected ordinary artifact request")
}

func (t coordinatorResultArtifactTransport) RoundTripArtifactWithRetry(_ context.Context, request workerproto.Envelope, _ workerproto.RetryPolicy, _ int64) (workerproto.Envelope, []byte, error) {
	response, err := coordinatorResultResponse(request, workerproto.MessageArtifactUpload, t.upload)
	return response, append([]byte(nil), t.raw...), err
}

type testProtocolCredentialResolver struct {
	reference string
}

func (r *testProtocolCredentialResolver) ResolveProtocol(_ context.Context, reference string) (workerruntime.ProtocolCredentials, error) {
	r.reference = reference
	return workerruntime.ProtocolCredentials{
		CoordinatorPrincipal: "coordinator", CoordinatorKeyID: "coordinator-key",
		CoordinatorSecret: []byte("coordinator-secret-material"),
		WorkerPrincipal:   "worker", WorkerKeyID: "worker-key",
		WorkerSecret: []byte("worker-secret-material"),
	}, nil
}

func TestNewCoordinatorWorkerSessionBindsConfiguredAuthenticationAndPackage(t *testing.T) {
	cfg := config.Default()
	setCoordinatorTestRoots(t, &cfg)
	cfg.BacklogV2.Coordinator.ID = "coordinator"
	cfg.BacklogV2.SetupProfiles = map[string]config.V2SetupProfile{
		"test": {Commands: []string{"true"}, Timeout: config.Duration(time.Minute)},
	}
	project := cfg.BacklogV2.Projects["steward"]
	project.SetupProfile = "test"
	project.Repository = "https://example.invalid/steward.git"
	cfg.BacklogV2.Projects["steward"] = project
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	resolver := &testProtocolCredentialResolver{}
	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)

	session, err := newCoordinatorWorkerSession(
		context.Background(), cfg.BacklogV2, store, "normandy", 7,
		"session-1", resolver, now, nil, backlog.CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store},
	)
	if err != nil {
		t.Fatal(err)
	}
	builder, builderOK := session.Builder.(coordinatorDeliveringOfferBuilder)
	base, baseOK := builder.Base.(backlog.CoordinatorOfferBuilder)
	if session.Client == nil || session.ArtifactClient == nil || session.Importer.Store != store || session.CheckpointImporter.Store != store ||
		!builderOK || !baseOK || builder.Transport == nil || base.Store != store ||
		base.CoordinatorEpoch != 7 || base.CoordinatorID != "coordinator" ||
		base.Catalog != session.Binding.Catalog ||
		resolver.reference != "test" {
		t.Fatalf("session = %+v reference=%q", session, resolver.reference)
	}
}

func TestNewCoordinatorWorkerSessionFailsWithoutConfiguredWorkerEpoch(t *testing.T) {
	cfg := config.Default()
	setCoordinatorTestRoots(t, &cfg)
	cfg.BacklogV2.Workers["normandy"] = config.V2Worker{
		Address: "normandy", AcceptBacklog: true, Credential: "test",
		Providers: cfg.BacklogV2.Workers["normandy"].Providers,
	}
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_, err = newCoordinatorWorkerSession(
		context.Background(), cfg.BacklogV2, store, "normandy", 7,
		"session-1", &testProtocolCredentialResolver{}, time.Now().UTC(), nil, backlog.CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store},
	)
	if err == nil || !strings.Contains(err.Error(), "configured epoch") {
		t.Fatalf("error = %v", err)
	}
}

func TestNewCoordinatorWorkerSessionIDIsFresh(t *testing.T) {
	first, err := newCoordinatorWorkerSessionID("coordinator", "normandy")
	if err != nil {
		t.Fatal(err)
	}
	second, err := newCoordinatorWorkerSessionID("coordinator", "normandy")
	if err != nil {
		t.Fatal(err)
	}
	if first == second || !strings.HasPrefix(first, "coordinator-normandy-") {
		t.Fatalf("session ids = %q, %q", first, second)
	}
}

func TestCoordinatorWorkerSessionsApplyFinalAdmissionAndIsolateFailures(t *testing.T) {
	var workers []string
	sessions := &coordinatorWorkerSessions{
		workerIDs: []string{"alpha", "bravo"},
		exchange: func(_ context.Context, workerID string, quota backlog.QuotaBridgeReport) (backlog.WorkerExchangeReport, error) {
			workers = append(workers, workerID)
			admission := backlog.WorkerAdmissionPolicyFromQuotaReport(quota)
			if !admission.AllowsNewWork("open") || admission.AllowsNewWork("closed") {
				t.Fatalf("admission = %#v", admission)
			}
			if workerID == "alpha" {
				return backlog.WorkerExchangeReport{}, errors.New("unavailable")
			}
			return backlog.WorkerExchangeReport{Snapshot: domain.WorkerSnapshot{WorkerID: workerID}}, nil
		},
	}
	report := sessions.Tick(context.Background(), backlog.QuotaBridgeReport{Derived: []backlog.QuotaPoolAdmissionSnapshot{
		{QuotaPoolID: "open", Admission: domain.AdmissionOpen},
		{QuotaPoolID: "closed", Admission: domain.AdmissionClosed},
	}})
	if len(report.Results) != 2 || report.Results[0].Err == nil || report.Results[1].Err != nil ||
		strings.Join(workers, ",") != "alpha,bravo" {
		t.Fatalf("report = %#v workers=%v", report, workers)
	}
}

func TestImportCoordinatorWorkerResultFetchesImportsThenAcknowledges(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 10, 15, 0, 0, 0, time.UTC)
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	task := domain.Task{ID: "task-1", WorkflowID: "workflow-1", Name: "task", Outputs: []domain.ArtifactDeclaration{{Name: "answer.txt", MediaType: "text/plain"}}}
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressVerifying, Control: domain.ControlStopped, Revision: 2, AssignmentID: "assignment-1", UpdatedAt: now}
	assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "normandy", WorkerEpoch: "worker-1", State: domain.AssignmentCompleted, ThreadID: "thread-1", Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: task.WorkflowID}}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	contents := [][]byte{[]byte("answer\n"), []byte("BACKLOG STATUS: done\n"), []byte(`{"thread":{"id":"thread-1","latestTurn":{"turnId":"turn-1","state":"completed","startedAt":"2026-09-13T05:00:00Z","completedAt":"2026-09-13T05:01:00Z"},"session":{"threadId":"thread-1","status":"ready","activeTurnId":null,"lastError":null}}}`)}
	objects := []workerproto.ArtifactObject{
		coordinatorResultObject("output-1", "results/answer.txt", "output", "text/plain", contents[0]),
		coordinatorResultObject("final-message-attempt-1", "results/final-message.md", "summary", "text/markdown", contents[1]),
		coordinatorResultObject("thread-archive-attempt-1", "results/thread.json", "log", "application/json", contents[2]),
	}
	manifest := workerproto.ArtifactTransferManifest{Version: 1, ID: "upload-assignment-1-result", Direction: "upload", CoordinatorEpoch: 1, WorkerID: "normandy", WorkerEpoch: "worker-1", AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, Objects: objects, TotalBytes: int64(len(contents[0]) + len(contents[1]) + len(contents[2])), CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	upload := workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: coordinatorResultCustody(t, manifest)}
	controlTransport := &coordinatorResultControlTransport{upload: upload}
	control := coordinatorResultClient(t, now, "control", controlTransport)
	artifact := coordinatorResultClient(t, now, "artifact", coordinatorResultArtifactTransport{upload: upload, raw: bytes.Join(contents, nil)})
	session := coordinatorWorkerSession{
		Client: control, ArtifactClient: artifact,
		Importer: backlog.CoordinatorResultImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: backlog.CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 1024, MaxTotalBytes: 1024, Now: func() time.Time { return now.Add(time.Minute) }},
	}
	report, err := importCoordinatorWorkerResult(ctx, session, backlog.WorkerExchangeReport{}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if !controlTransport.acked || len(report.Imports) != 1 || len(report.Imports[0].Artifacts) != 3 || len(report.Imports[0].Transition) != 1 || report.Imports[0].Transition[0].Attempt.Progress != domain.ProgressSucceeded {
		t.Fatalf("result report=%#v acknowledged=%t", report, controlTransport.acked)
	}
}

func TestImportCoordinatorWorkerResultDoesNotAcknowledgeCorruptFetch(t *testing.T) {
	now := time.Date(2026, 9, 10, 15, 0, 0, 0, time.UTC)
	object := coordinatorResultObject("output-1", "results/answer.txt", "output", "text/plain", []byte("good"))
	manifest := workerproto.ArtifactTransferManifest{Version: 1, ID: "upload-assignment-1-result", Direction: "upload", CoordinatorEpoch: 1, WorkerID: "normandy", WorkerEpoch: "worker-1", AssignmentID: "assignment-1", AssignmentEpoch: 1, Objects: []workerproto.ArtifactObject{object}, TotalBytes: object.Size, CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	upload := workerproto.ArtifactUploadResponse{Manifest: manifest}
	controlTransport := &coordinatorResultControlTransport{upload: upload}
	session := coordinatorWorkerSession{
		Client:         coordinatorResultClient(t, now, "control-corrupt", controlTransport),
		ArtifactClient: coordinatorResultClient(t, now, "artifact-corrupt", coordinatorResultArtifactTransport{upload: upload, raw: []byte("evil")}),
	}
	if _, err := importCoordinatorWorkerResult(context.Background(), session, backlog.WorkerExchangeReport{}, 1024); err == nil {
		t.Fatal("corrupt artifact fetch was accepted")
	}
	if controlTransport.acked {
		t.Fatal("corrupt artifact fetch was acknowledged")
	}
}

func TestImportCoordinatorWorkerCheckpointPublishesThenAcknowledges(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 10, 16, 0, 0, 0, time.UTC)
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	task := domain.Task{ID: "task-1", WorkflowID: "workflow-1", Name: "task"}
	attempt := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: task.ID, Number: 1, Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 1, AssignmentID: "assignment-1", UpdatedAt: now}
	assignment := domain.Assignment{ID: "assignment-1", AttemptID: attempt.ID, WorkerID: "normandy", WorkerEpoch: "worker-1", State: domain.AssignmentClaimed, Epoch: 1, LeaseToken: "lease", DispatchToken: "dispatch", CreatedAt: now, UpdatedAt: now}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{ID: attempt.WorkflowRunID, WorkflowID: task.WorkflowID}}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment}}); err != nil {
		t.Fatal(err)
	}
	content := []byte("resume here\n")
	object := coordinatorResultObject("checkpoint-attempt-1-deadbeef", "checkpoints/checkpoint-attempt-1-deadbeef.md", "checkpoint", "text/markdown", content)
	checkpoint := &domain.CheckpointMetadata{ArtifactID: object.ID, Path: ".t3/checkpoint.md", SHA256: object.SHA256, Size: object.Size, CapturedAt: now.Add(time.Minute)}
	record := domain.ThrottleAttemptRecord{DirectiveID: "directive-1", AttemptID: attempt.ID, Revision: 1, Delivery: domain.ThrottleDeliveryPending, Control: domain.ControlDraining, Command: domain.ThrottleCommand{ID: "throttle-1", DirectiveID: "directive-1", AttemptID: attempt.ID, Kind: domain.ThrottleCommandDrain}, UpdatedAt: now.Add(time.Minute)}
	if err := store.CommitThrottleAttemptTransitions(ctx, []domain.ThrottleAttemptTransition{{ExpectedRevision: 0, Record: record}}); err != nil {
		t.Fatal(err)
	}
	record.Revision = 2
	record.Delivery = domain.ThrottleDeliveryAcknowledged
	record.Result = domain.ThrottleResultCheckpointed
	record.Control = domain.ControlPaused
	record.Checkpoint = checkpoint
	record.UpdatedAt = now.Add(2 * time.Minute)
	if err := store.CommitThrottleAttemptTransitions(ctx, []domain.ThrottleAttemptTransition{{ExpectedRevision: 1, Record: record}}); err != nil {
		t.Fatal(err)
	}
	manifest := workerproto.ArtifactTransferManifest{Version: 1, ID: "upload-assignment-1-checkpoint-deadbeef", Direction: "upload", CoordinatorEpoch: 1, WorkerID: "normandy", WorkerEpoch: "worker-1", AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch, Objects: []workerproto.ArtifactObject{object}, TotalBytes: object.Size, CreatedAt: now.Add(time.Minute), ExpiresAt: now.Add(time.Hour)}
	upload := workerproto.ArtifactUploadResponse{Manifest: manifest, Custody: coordinatorResultCustody(t, manifest)}
	controlTransport := &coordinatorResultControlTransport{upload: upload}
	session := coordinatorWorkerSession{
		Client:             coordinatorResultClient(t, now, "checkpoint-control", controlTransport),
		ArtifactClient:     coordinatorResultClient(t, now, "checkpoint-artifact", coordinatorResultArtifactTransport{upload: upload, raw: content}),
		CheckpointImporter: backlog.CoordinatorCheckpointImporter{CoordinatorID: "coordinator", CoordinatorEpoch: 1, Store: store, Artifacts: backlog.CoordinatorArtifactStore{Root: filepath.Join(t.TempDir(), "artifacts"), Catalog: store}, MaxArtifactBytes: 1024, MaxTotalBytes: 1024, Now: func() time.Time { return now.Add(3 * time.Minute) }},
	}
	report, err := importCoordinatorWorkerCheckpoint(ctx, session, backlog.WorkerExchangeReport{}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if !controlTransport.acked || len(report.Checkpoints) != 1 || report.Checkpoints[0].ID != object.ID {
		t.Fatalf("checkpoint report=%#v acknowledged=%t", report, controlTransport.acked)
	}
}

func coordinatorResultClient(t *testing.T, now time.Time, suffix string, transport workerproto.RoundTripper) *workerproto.Client {
	t.Helper()
	client, err := workerproto.NewClient(workerproto.ClientConfig{CoordinatorID: "coordinator", WorkerID: "normandy", CoordinatorEpoch: 1, WorkerEpoch: "worker-1", SessionID: "session-" + suffix, RequestTimeout: time.Minute, SignerPrincipal: "coordinator", SignerKeyID: "key", SignerSecret: []byte("coordinator-secret-material"), RetryPolicy: workerproto.RetryPolicy{MaxAttempts: 1}, Transport: transport, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func coordinatorResultResponse(request workerproto.Envelope, kind workerproto.MessageType, payload any) (workerproto.Envelope, error) {
	response, err := workerproto.NewEnvelope(kind, request.SessionID, "response-"+request.RequestID, request.Recipient, request.Sender, request.CoordinatorEpoch, request.WorkerEpoch, request.Sequence, request.SentAt, request.Deadline, payload)
	response.InReplyTo = request.RequestID
	return response, err
}

func coordinatorResultObject(id, path, kind, media string, data []byte) workerproto.ArtifactObject {
	sum := sha256.Sum256(data)
	return workerproto.ArtifactObject{ID: id, Path: path, Kind: kind, MediaType: media, Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
}

func coordinatorResultCustody(t *testing.T, manifest workerproto.ArtifactTransferManifest) []workerproto.ArtifactCustodyRecord {
	t.Helper()
	records := make([]workerproto.ArtifactCustodyRecord, 0, len(manifest.Objects))
	previous := ""
	for index, object := range manifest.Objects {
		record, err := workerproto.BuildCustodyRecord(workerproto.ArtifactCustodyRecord{ManifestID: manifest.ID, ObjectID: object.ID, From: "worker:" + manifest.WorkerID, To: "outbox:coordinator", Sequence: int64(index + 1), Size: object.Size, SHA256: object.SHA256, VerifiedAt: manifest.CreatedAt, PreviousSHA256: previous})
		if err != nil {
			t.Fatal(err)
		}
		previous = record.RecordSHA256
		records = append(records, record)
	}
	return records
}
