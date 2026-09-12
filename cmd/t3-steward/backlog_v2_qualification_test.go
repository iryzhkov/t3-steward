package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

// TestBacklogV2ProductionQualification is the repeatable, no-external-effects
// portion of the S19 gate. Each group runs in a fresh process and delegates to
// tests that create only temporary coordinator, worker, credential, artifact,
// bundle, workspace, and backup roots. The worker topology group also starts a
// separate restricted worker process behind the authenticated protocol.
//
// The authorized multi-host observation and canary are deliberately not part
// of this test: they require operator-selected hosts and credentials.
func TestBacklogV2ProductionQualification(t *testing.T) {
	repositoryRoot := coordinatorTestRepositoryRoot(t)
	groups := []struct {
		name     string
		packages []string
		tests    []string
	}{
		{
			name:     "separate_process_topology",
			packages: []string{"./cmd/t3-steward", "./internal/workerruntime"},
			tests: []string{
				"TestCoordinatorLocalMultiProcessWorkflow",
				"TestRunBacklogV2CoordinatorServesAuthenticatedLocalAdmin",
				"TestRunBacklogV2CoordinatorAcceptsNativeArchiveSubmissionAndReplay",
				"TestLocalCoordinatorStubWorkerMultiProcess",
			},
		},
		{
			name:     "transport_loss_reorder_duplicate_and_clock_skew",
			packages: []string{"./internal/workerproto"},
			tests: []string{
				"TestClientPoisonsAmbiguousSessionAndRejectsWrongSnapshot",
				"TestExchangeValidationAndIdempotency",
				"TestSSHTransportDropRetryTimeoutCancellationAndLimits",
				"TestSSHArtifactTransportAuthenticatesMetadataAndBoundsRawSuffix",
			},
		},
		{
			name:     "restart_replay_and_rollback",
			packages: []string{"./internal/backlog", "./internal/backlogadmin", "./internal/store/sqlite", "./internal/workerruntime"},
			tests: []string{
				"TestFleetCoordinatorCommitsPlanAndReplaysLostCommandResponse",
				"TestFleetCoordinatorRecoversLostDispatchAcknowledgementWithoutDuplicateExecution",
				"TestReconcileAssignmentDispatchRetriesSameIdentityAfterRestart",
				"TestExecutePendingCommandsSurvivesRestartAndReplaysRetry",
				"TestCoordinatorRestartFencesOldClaimsAndRetainsPlan",
				"TestAssignmentPlanRollsBackWhenAnyAttemptIsStale",
				"TestRuntimeRestartReconcilesEveryInFlightBoundary",
				"TestProtocolServerUsesReplayStoreAcrossRestart",
			},
		},
		{
			name:     "stale_quota_schedule_and_legacy_exclusion",
			packages: []string{"./cmd/t3-steward", "./internal/backlog"},
			tests: []string{
				"TestRunBacklogV2RefusesLegacyCoordinatorOverlapBeforeStateOpen",
				"TestCoordinatorQuotaReconcilerPersistsClosedAdmissionWithoutEvidence",
				"TestQuotaBridgeDeduplicatesSharedPoolAndFailsClosedWhenStale",
				"TestScheduleTimerCatchUpOverlapAndRestart",
			},
		},
		{
			name:     "artifact_corruption_backup_restore_and_recovery",
			packages: []string{"./cmd/t3-steward", "./internal/backlog", "./internal/backupsnapshot", "./internal/store/sqlite", "./internal/workerruntime"},
			tests: []string{
				"TestImportCoordinatorWorkerResultDoesNotAcknowledgeCorruptFetch",
				"TestCoordinatorArtifactsTransferAcrossWorkerRestartAndVerifyChecksum",
				"TestSnapshotBackupRestoreRoundTrip",
				"TestSnapshotVerifyRefusesCorruptIncompleteAndMismatched",
				"TestUnknownRecoveryIsEvidenceRevisionAndReplayFenced",
				"TestStaleEpochAndCorruptArtifactFailClosed",
			},
		},
	}
	for _, group := range groups {
		group := group
		t.Run(group.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			expression := "^(?:" + strings.Join(group.tests, "|") + ")$"
			arguments := append([]string{"test"}, group.packages...)
			arguments = append(arguments, "-run", expression, "-count=1")
			command := exec.CommandContext(ctx, "go", arguments...)
			command.Dir = repositoryRoot
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("qualification subprocess failed: %v\n%s", err, output)
			}
		})
	}
}

// TestBacklogV2ProductionProcessTopology runs the real coordinator boundary,
// local admin client, and restricted worker service in three separate OS
// processes. All configuration and persistent roots are beneath one temporary
// directory, the worker is in no-external-effects mode, and no SSH or T3
// endpoint is contacted.
func TestBacklogV2ProductionProcessTopology(t *testing.T) {
	root := os.Getenv("T3_QUALIFICATION_ROOT")
	switch os.Getenv("T3_QUALIFICATION_ROLE") {
	case "coordinator":
		runQualificationCoordinator(t, root)
		return
	case "client":
		runQualificationClient(t, root)
		return
	case "worker":
		runQualificationWorker(t, root)
		return
	case "remote-worker":
		runQualificationWorker(t, root)
		os.Exit(0)
	}

	root = shortTempDir(t)
	if err := os.MkdirAll(filepath.Join(root, "drop"), 0o700); err != nil {
		t.Fatal(err)
	}
	coordinator := qualificationProcess(t, root, "coordinator")
	var coordinatorOutput strings.Builder
	coordinator.Stdout = &coordinatorOutput
	coordinator.Stderr = &coordinatorOutput
	if err := coordinator.Start(); err != nil {
		t.Fatal(err)
	}

	socket := filepath.Join(root, "state.db.admin.sock")
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(socket); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = coordinator.Process.Kill()
			t.Fatalf("coordinator socket was not created\n%s", coordinatorOutput.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	clientOutput, err := qualificationProcess(t, root, "client").CombinedOutput()
	if err != nil {
		_ = coordinator.Process.Kill()
		t.Fatalf("client process failed: %v\n%s", err, clientOutput)
	}

	worker := qualificationProcess(t, root, "worker")
	stdin, err := worker.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	responseRead, responseWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer responseRead.Close()
	worker.ExtraFiles = []*os.File{responseWrite}
	var workerErrors strings.Builder
	worker.Stdout = &workerErrors
	worker.Stderr = &workerErrors
	if err := worker.Start(); err != nil {
		t.Fatal(err)
	}
	responseWrite.Close()
	now := time.Now().UTC()
	request, err := workerproto.NewEnvelope(
		workerproto.MessageSnapshot, "qualification-session", "qualification-request",
		"coordinator", "normandy", 1, "worker-1", 1, now, now.Add(time.Minute),
		workerproto.SnapshotRequest{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := workerproto.SignEnvelope(&request, "ssh:coordinator", "coordinator-key", []byte("qualification-coordinator-secret")); err != nil {
		t.Fatal(err)
	}
	codec := workerproto.Codec{MaxBytes: 1 << 20}
	if err := codec.Encode(stdin, request); err != nil {
		t.Fatal(err)
	}
	if err := stdin.Close(); err != nil {
		t.Fatal(err)
	}
	var response workerproto.Envelope
	if err := codec.Decode(responseRead, &response); err != nil {
		waitErr := worker.Wait()
		t.Fatalf("decode worker response: %v (worker: %v)\n%s", err, waitErr, workerErrors.String())
	}
	if err := worker.Wait(); err != nil {
		t.Fatalf("worker process failed: %v\n%s", err, workerErrors.String())
	}
	if response.Type != workerproto.MessageObservations || response.InReplyTo != request.RequestID {
		t.Fatalf("worker response = %+v", response)
	}
	if err := workerproto.VerifyEnvelopeSignature(response, []byte("qualification-worker-secret")); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Wait(); err != nil {
		t.Fatalf("coordinator process failed: %v\n%s", err, coordinatorOutput.String())
	}
}

// TestBacklogV2AuthorizedMultiHostCanary is opt-in because it contacts the
// explicitly authorized host named by T3_S19_REMOTE_HOST. The remote command
// must be an ephemeral restricted wrapper that starts this test binary in the
// remote-worker role with disposable roots. It performs an authenticated
// snapshot followed by an empty-offer canary: no assignment, workspace
// preparation, or T3 dispatch is sent.
func TestBacklogV2AuthorizedMultiHostCanary(t *testing.T) {
	host := strings.TrimSpace(os.Getenv("T3_S19_REMOTE_HOST"))
	remoteCommand := strings.TrimSpace(os.Getenv("T3_S19_REMOTE_COMMAND"))
	if host == "" || remoteCommand == "" {
		t.Skip("authorized multi-host canary is not configured")
	}
	transport, err := workerproto.NewSSHTransport(workerproto.SSHConfig{
		Address: host, RemoteCommand: remoteCommand, RemoteArguments: []string{"control"},
		RequestTimeout: 20 * time.Second, ConnectTimeout: 10 * time.Second,
		MaxMessageBytes: 1 << 20, MaxStderrBytes: 1 << 20,
		ResponsePrincipal: "ssh:" + host, ResponseKeyID: "worker-key",
		ResponseSecret: []byte("qualification-worker-secret"),
	})
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		client, err := workerproto.NewClient(workerproto.ClientConfig{
			CoordinatorID: "coordinator", WorkerID: host, CoordinatorEpoch: 1,
			WorkerEpoch: "worker-1", SessionID: fmt.Sprintf("s19-%d-%d", os.Getpid(), attempt),
			RequestTimeout:  20 * time.Second,
			SignerPrincipal: "ssh:coordinator", SignerKeyID: "coordinator-key",
			SignerSecret: []byte("qualification-coordinator-secret"),
			RetryPolicy:  workerproto.RetryPolicy{MaxAttempts: 2, BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second},
			Transport:    transport,
		})
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := client.Snapshot(context.Background())
		if err != nil {
			t.Fatalf("remote observation %d: %v", attempt, err)
		}
		if snapshot.WorkerID != host || snapshot.WorkerEpoch != "worker-1" ||
			snapshot.CoordinatorEpoch != 1 || !snapshot.Connected {
			t.Fatalf("remote snapshot %d = %+v", attempt, snapshot)
		}
		claims, err := client.DeliverOffers(context.Background(), nil)
		if err != nil {
			t.Fatalf("remote empty-offer canary %d: %v", attempt, err)
		}
		if len(claims) != 0 {
			t.Fatalf("remote empty-offer canary %d returned %d claims", attempt, len(claims))
		}
	}
}

func qualificationProcess(t *testing.T, root, role string) *exec.Cmd {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestBacklogV2ProductionProcessTopology$")
	command.Env = append(os.Environ(), "T3_QUALIFICATION_ROOT="+root, "T3_QUALIFICATION_ROLE="+role)
	return command
}

func qualificationConfig(root string) config.Config {
	workerID := qualificationWorkerID()
	cfg := config.Default()
	cfg.StatePath = filepath.Join(root, "state.db")
	cfg.Backlog.Dir = filepath.Join(root, "drop")
	cfg.BacklogV2.Mode = "coordinator"
	cfg.BacklogV2.Coordinator.ID = "coordinator"
	cfg.BacklogV2.Storage.Bundles = filepath.Join(root, "bundles")
	cfg.BacklogV2.Storage.Artifacts = filepath.Join(root, "artifacts")
	cfg.BacklogV2.Storage.Workspaces = filepath.Join(root, "workspaces")
	cfg.BacklogV2.Scheduling.Interval = config.Duration(time.Hour)
	cfg.BacklogV2.QuotaPools = map[string]config.V2QuotaPool{
		"pool": {Provider: "test", MaxConcurrent: 1},
	}
	cfg.BacklogV2.Workers = map[string]config.V2Worker{
		workerID: {
			Address: "local.invalid", Epoch: "worker-1", AcceptBacklog: true, Credential: "qualification",
			Providers: map[string]config.V2Provider{"test": {Models: []string{"test"}, QuotaPool: "pool"}},
		},
	}
	cfg.BacklogV2.Projects = map[string]config.V2Project{
		"steward": {Repository: "https://example.invalid/steward.git", DefaultRef: "main", T3Project: "test", SetupProfile: "test", Workers: []string{workerID}},
	}
	cfg.BacklogV2.SetupProfiles = map[string]config.V2SetupProfile{
		"test": {Commands: []string{"true"}, Timeout: config.Duration(time.Minute)},
	}
	return cfg
}

func runQualificationCoordinator(t *testing.T, root string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := runBacklogV2Coordinator(ctx, qualificationConfig(root), slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
}

func runQualificationClient(t *testing.T, root string) {
	t.Helper()
	cfg := qualificationConfig(root)
	client := backlogadmin.LocalClient{
		Path:               filepath.Join(root, "state.db.admin.sock"),
		MaxResponseBytes:   cfg.BacklogV2.MessageLimits.MaxBytes,
		MaxArtifactBytes:   cfg.BacklogV2.MessageLimits.MaxArtifactBytes,
		MaxSubmissionBytes: cfg.BacklogV2.MessageLimits.MaxBytes,
		RequestTimeout:     cfg.BacklogV2.Transport.RequestTimeout.D(),
	}
	response, err := client.Query(context.Background(), backlogadmin.Query{
		Version: backlogadmin.Version, Kind: backlogadmin.QueryStatus,
		Principal: backlogadmin.Principal{ID: "qualification-client"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Status == nil || response.Status.Runtime.Mode != "coordinator" {
		t.Fatalf("status response = %+v", response)
	}
}

type qualificationProtocolCredentials struct{}

func (qualificationProtocolCredentials) ResolveProtocol(context.Context, string) (workerruntime.ProtocolCredentials, error) {
	return workerruntime.ProtocolCredentials{
		CoordinatorPrincipal: "ssh:coordinator", CoordinatorKeyID: "coordinator-key",
		CoordinatorSecret: []byte("qualification-coordinator-secret"),
		WorkerPrincipal:   "ssh:" + qualificationWorkerID(), WorkerKeyID: "worker-key",
		WorkerSecret: []byte("qualification-worker-secret"),
	}, nil
}

func runQualificationWorker(t *testing.T, root string) {
	t.Helper()
	cfg := qualificationConfig(root)
	service, err := workerruntime.NewWorkerService(context.Background(), workerruntime.WorkerServiceOptions{
		Settings: cfg.BacklogV2, WorkerID: qualificationWorkerID(), WorkerEpoch: "worker-1", CoordinatorEpoch: 1,
		ProtocolCredentials: qualificationProtocolCredentials{}, DryRun: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var output io.Writer = os.Stdout
	var responseFile *os.File
	if os.Getenv("T3_QUALIFICATION_ROLE") != "remote-worker" {
		responseFile = os.NewFile(3, "qualification-worker-response")
		if responseFile == nil {
			t.Fatal("qualification worker response descriptor is missing")
		}
		defer responseFile.Close()
		output = responseFile
	}
	if err := service.Serve(context.Background(), os.Stdin, output); err != nil {
		t.Fatal(err)
	}
}

func qualificationWorkerID() string {
	if workerID := strings.TrimSpace(os.Getenv("T3_QUALIFICATION_WORKER")); workerID != "" {
		return workerID
	}
	return "normandy"
}
