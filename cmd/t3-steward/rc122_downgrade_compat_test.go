//go:build rc122compat && linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	localwait "github.com/iryzhkov/t3-steward/internal/wait"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// Run via scripts/rc122-downgrade-compat.sh: both subprocesses are compiled
// from the pinned rc121 worktree, never from candidate copies of its types.
func TestRC122DowngradeCompatibility(t *testing.T) {
	oldCLI, oldReader := os.Getenv("RC121_CLI"), os.Getenv("RC121_READER")
	if oldCLI == "" || oldReader == "" {
		t.Fatal("run scripts/rc122-downgrade-compat.sh")
	}
	home := t.TempDir()
	t.Setenv("HOME", home)
	// No live host projections, environment overrides or sockets are consulted.
	for _, key := range []string{"T3_STEWARD_CONFIG", "T3_STEWARD_BACKLOG_V2_MODE", "T3_STEWARD_STATE_PATH", "T3_STEWARD_COORDINATOR_CLIENT", "T3_STEWARD_ORIGINAL_HOME"} {
		t.Setenv(key, "")
	}
	dir, err := os.MkdirTemp("/tmp", "rc122-fixture-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	ctx := context.Background()
	now := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	dbPath := filepath.Join(dir, "state.db")
	s, err := sqlite.OpenMigrated(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	task := domain.Task{ID: "task-1", WorkflowID: "workflow-1", Name: "implement", Class: domain.TaskClassRequired, NeedsVerdict: map[string]string{"review": "accept"}, FixLoop: &domain.FixLoopTask{Name: "repair", Round: 1, MaxRounds: 2, Kind: "implement"}, Retry: &domain.TaskRetryPolicy{Infrastructure: 2, Backoff: time.Minute}, Outputs: []domain.ArtifactDeclaration{{Name: "result.md"}}}
	failed := domain.Attempt{ID: "attempt-1", TaskID: task.ID, WorkflowRunID: "run-1", Number: 1, Revision: 1, Progress: domain.ProgressFailed, Control: domain.ControlStopped, Failure: "workspace is missing", UpdatedAt: now, CompletedAt: &now}
	retry := domain.Attempt{ID: "attempt-2", TaskID: task.ID, WorkflowRunID: "run-1", Number: 2, Revision: 1, Progress: domain.ProgressActive, Control: domain.ControlRunning, ThreadID: "thread-2", AssignmentID: "assignment-2", UpdatedAt: now, AutomaticRetry: &domain.AutomaticRetry{SourceAttemptID: failed.ID, Class: domain.FailureInfrastructure, Code: domain.ReasonWorkspaceMissing, Ordinal: 1, Budget: 2, NotBefore: now, CommandID: "auto-retry-attempt-1"}}
	run := domain.WorkflowRun{ID: "run-1", WorkflowID: "workflow-1", Revision: 1, Progress: domain.ProgressActive, CreatedAt: now, UpdatedAt: now, Sink: &domain.SinkTask{ID: domain.SinkTaskID("run-1"), Name: domain.SinkTaskName, Progress: domain.ProgressFailed, Result: &domain.SinkResult{FixLoops: []domain.FixLoopSummary{{Name: "repair", Rounds: 1, MaxRounds: 2, FinalVerdict: "changes-requested"}}}}}
	records := sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "workflow-1", Version: 2, Name: "compat", Project: "compat", Class: domain.TaskClassRequired, TaskIDs: []string{task.ID}, CreatedAt: now}},
		WorkflowRuns: []domain.WorkflowRun{run}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{failed, retry},
		Assignments: []domain.Assignment{{ID: retry.AssignmentID, AttemptID: retry.ID, WorkerID: "compat-worker", Epoch: 1, State: domain.AssignmentClaimed, LeaseToken: "lease", DispatchToken: "dispatch", ThreadID: retry.ThreadID, CreatedAt: now, UpdatedAt: now}},
		Artifacts:   []domain.Artifact{{ID: "result-1", WorkflowRunID: run.ID, TaskID: task.ID, AttemptID: failed.ID, Kind: domain.ArtifactOutput, Name: "result.md", MediaType: "text/markdown", StoragePath: "artifacts/result.md", SHA256: strings.Repeat("a", 64), Size: 6, CreatedAt: now}},
	}
	payload, err := json.Marshal(retry.AutomaticRetry)
	if err != nil {
		t.Fatal(err)
	}
	records.AdminCommands = []domain.AdminCommand{{ID: "auto-retry-attempt-1", Kind: domain.AdminCommandRetry, TargetType: domain.AdminTargetAttempt, TargetID: failed.ID, ExpectedRevision: failed.Revision, RequestedBy: domain.AutomaticRetryRequestedBy, Reason: "compat receipt", Payload: payload, State: domain.AdminCommandApplied, CreatedAt: now, AppliedAt: &now}}
	if err := s.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	class, _ := domain.ClassifyAttemptFailure(failed)
	if n, err := s.RecordAttemptFailureClassifications(ctx, []sqlite.AttemptFailureStamp{{AttemptID: failed.ID, Revision: failed.Revision, Classification: class}}); err != nil || n != 1 {
		t.Fatalf("normal classification write: n=%d err=%v", n, err)
	}
	wait, err := s.RegisterTaskWait(ctx, domain.TaskWaitRegistration{RequestID: "compat-park", WorkflowRunID: run.ID, TaskID: task.ID, AttemptID: retry.ID, IssuedRevision: retry.Revision, ThreadID: retry.ThreadID, Wake: domain.WakeAll, MaxDuration: time.Hour, Name: "compat", Condition: "true", Shell: &domain.ShellWaitCondition{Dir: dir, Command: []string{"true"}}}, now)
	if err != nil || wait.ConditionDigest == "" {
		t.Fatalf("normal wait write: %+v %v", wait, err)
	}
	if err := s.SaveWait(ctx, localwait.Wait{ID: "local-time", ThreadID: retry.ThreadID, Kind: domain.WaitKindTime, At: &now, For: time.Minute, Status: localwait.StatusWaiting}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveWait(ctx, localwait.Wait{ID: "local-github", ThreadID: retry.ThreadID, Kind: domain.WaitKindGitHub, GitHub: &localwait.GitHubTarget{Kind: "commit", ID: strings.Repeat("a", 40), State: "checks-completed", Repo: "example/repo"}, Status: localwait.StatusWaiting}); err != nil {
		t.Fatal(err)
	}
	got, err := s.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(got)
	for _, key := range []string{"failureClass", "failureReason", "automaticRetry", "needsVerdict", "fixLoop", "retry", "fixLoops"} {
		if !bytes.Contains(raw, []byte(`"`+key+`"`)) {
			t.Fatalf("candidate did not write %s", key)
		}
	}
	invoke := func(input []byte, binary string, args ...string) ([]byte, error) {
		t.Helper()
		cmd := exec.Command(binary, args...)
		for _, value := range os.Environ() {
			if !strings.HasPrefix(value, "T3_STEWARD_") {
				cmd.Env = append(cmd.Env, value)
			}
		}
		cmd.Stdin = bytes.NewReader(input)
		return cmd.CombinedOutput()
	}
	// The candidate's preserved digest uses the real capture code; the record
	// shape is the worker's optional attempt-directory sidecar. Run the rc121
	// LocalDriver collection path with that sidecar already present.
	collectionRoot := filepath.Join(dir, "collection")
	pkg := workerproto.ExecutionPackage{WorkerID: "compat-worker", Class: domain.TaskClassRequired,
		Identity: workerproto.ExecutionIdentity{WorkflowID: "sidecar-workflow", WorkflowRunID: "sidecar-run", TaskID: "sidecar-task", AttemptID: "sidecar-attempt", AssignmentID: "sidecar-assignment", AssignmentEpoch: 1, ThreadID: "sidecar-thread", DispatchToken: "sidecar-dispatch"},
		Outputs:  []domain.ArtifactDeclaration{{Name: "answer.txt", MediaType: "text/plain"}}}
	attemptDir := filepath.Join(collectionRoot, "runs", pkg.Identity.WorkflowRunID, pkg.Identity.TaskID, pkg.Identity.AttemptID)
	workspace := filepath.Join(attemptDir, "workspace")
	if err := os.MkdirAll(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "answer.txt"), []byte("sidecar-compatible output\n"), 0600); err != nil {
		t.Fatal(err)
	}
	preserved, err := backlog.CapturePreservedResult(ctx, "", workspace, pkg.Outputs)
	if err != nil {
		t.Fatal(err)
	}
	sidecar, err := json.Marshal(struct {
		Identity workerproto.ExecutionIdentity `json:"identity"`
		Turn     string                        `json:"turn"`
		Result   backlog.PreservedResult       `json:"result"`
	}{pkg.Identity, "sidecar-turn", preserved})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(attemptDir, "preserved-result.json"), sidecar, 0600); err != nil {
		t.Fatal(err)
	}
	packageJSON, err := json.Marshal(pkg)
	if err != nil {
		t.Fatal(err)
	}
	if out, err := invoke(packageJSON, oldReader, "collect-sidecar", collectionRoot); err != nil {
		t.Fatalf("rc121 sidecar collection: %v\n%s", err, out)
	}
	t.Log("rc121 LocalDriver collection: candidate preserved-result sidecar tolerated; declared output captured and published; sidecar unchanged")

	out, err := invoke(nil, oldReader, "store", dbPath)
	if err != nil {
		t.Fatalf("rc121 store read: %v\n%s", err, out)
	}
	t.Log("rc121 store reader: candidate attempts, task, sink and result artifact decoded")
	cfgPath := filepath.Join(dir, "config.yaml")
	write := func(content string) {
		t.Helper()
		if err := os.WriteFile(cfgPath, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("{}\n")
	if _, err := config.ValidateStagedFile(cfgPath); err != nil {
		t.Fatalf("candidate default config: %v", err)
	}
	if out, err := invoke(nil, oldReader, "config", cfgPath); err != nil {
		t.Fatalf("rc121 default config: %v %s", err, out)
	}
	t.Log("new config keys absent: both versions accept")
	write("backlog_v2:\n  coordinator:\n    automatic_retries:\n      max_infrastructure: 3\n")
	if _, err := config.ValidateStagedFile(cfgPath); err != nil {
		t.Fatalf("candidate explicit default: %v", err)
	}
	if out, err := invoke(nil, oldReader, "config", cfgPath); err == nil || !bytes.Contains(out, []byte("automatic_retries")) {
		t.Fatalf("expected rc121 explicit-key refusal: %v %s", err, out)
	}
	t.Log("automatic_retries.max_infrastructure at explicit default: rc121 rejects unknown automatic_retries; omit the block for rollback")

	// Candidate serves the candidate-written store; use the actual rc121 CLI's
	// strict local transport and its real read-version negotiation.
	write(fmt.Sprintf("state_path: %q\n", dbPath))
	service, err := backlogadmin.New(s, localAdminAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := backlogadmin.ListenLocal(dbPath + ".admin.sock")
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	server := &backlogadmin.LocalServer{Listener: listener, Service: coordinatorLocalService{admin: service}, AllowedUID: uint32(os.Getuid()), MaxRequestBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20, RequestTimeout: 10 * time.Second, MaxConcurrent: 1}
	go func() { done <- server.Serve(serveCtx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("server shutdown: %v", err)
		}
	}()
	for _, args := range [][]string{{"task", "show", "run-1/implement", "--json"}, {"show", "run-1", "--include-sink", "--json"}, {"list", "--include-sink", "--json"}, {"diagnose", "run-1", "--json"}, {"commands", "run-1", "--json"}} {
		argv := append([]string{"backlog", "--config", cfgPath}, args...)
		out, err := invoke(nil, oldCLI, argv...)
		if err != nil {
			t.Fatalf("rc121 CLI %v: %v\n%s", args, err, out)
		}
		if !json.Valid(out) {
			t.Fatalf("rc121 CLI %v did not decode JSON: %s", args, out)
		}
		t.Logf("rc121 CLI backlog %s: decoded", strings.Join(args, " "))
	}

	// Both directions use the real strict worker protocol payload codec.
	wire, err := invoke(nil, oldReader, "encode-snapshot")
	if err != nil {
		t.Fatal(err)
	}
	var observations workerproto.Observations
	codec := workerproto.Codec{MaxBytes: 1 << 20}
	if err := codec.Decode(bytes.NewReader(wire), &observations); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveWorkerSnapshot(ctx, observations.Snapshot); err != nil {
		t.Fatalf("candidate coordinator rc121 snapshot: %v", err)
	}
	observations.Snapshot.Inventory.Capabilities = append(observations.Snapshot.Inventory.Capabilities, "coordinator-client-v1", "ask-relay-v1", "git-push-compat", "huyang-trusted-v1")
	var encoded bytes.Buffer
	if err := codec.Encode(&encoded, observations); err != nil {
		t.Fatal(err)
	}
	if out, err := invoke(encoded.Bytes(), oldReader, "decode-snapshot"); err != nil {
		t.Fatalf("rc121 coordinator candidate snapshot: %v %s", err, out)
	}
	request, err := invoke(nil, oldReader, "encode-request")
	if err != nil {
		t.Fatal(err)
	}
	var snapshotRequest workerproto.SnapshotRequest
	if err := codec.Decode(bytes.NewReader(request), &snapshotRequest); err != nil {
		t.Fatal(err)
	}
	encoded.Reset()
	if err := codec.Encode(&encoded, snapshotRequest); err != nil {
		t.Fatal(err)
	}
	if out, err := invoke(encoded.Bytes(), oldReader, "decode-request"); err != nil {
		t.Fatalf("rc121 worker candidate request: %v %s", err, out)
	}
	t.Log("mixed-version worker snapshots and snapshot requests: strict decode passed both directions")
}
