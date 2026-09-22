package workerruntime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/providercontainment"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

func proveAttentionStopQuiescesProviderAndVerification(t *testing.T, pkg workerproto.ExecutionPackage) {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	commands := t.TempDir()
	t.Setenv("T3_TEST_SUPERVISOR_ROOT", root)
	t.Setenv("PATH", commands+":"+os.Getenv("PATH"))
	if err := os.WriteFile(filepath.Join(commands, "systemd-run"), []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	control := `#!/bin/bash
if [[ " $* " == *" show "* ]]; then
  for arg in "$@"; do [[ "$arg" == t3-contained-* ]] && unit="$arg"; done
  digest="$(sha256sum "$T3_TEST_SUPERVISOR_ROOT/$unit/intent.json" | cut -d' ' -f1)"
  printf 'LoadState=loaded\nDescription=t3-containment:%s\nActiveState=active\nSubState=running\nInvocationID=test\n' "$digest"
fi
exit 0
`
	if err := os.WriteFile(filepath.Join(commands, "systemctl"), []byte(control), 0o700); err != nil {
		t.Fatal(err)
	}
	manager := ContainedT3{Supervisor: providercontainment.Supervisor{Root: root, Executable: "/bin/true"}, Timeout: time.Second}
	provider := providercontainment.Launch{ExecutionID: pkg.Identity.ThreadID, Spec: providercontainment.Spec{WorkerID: pkg.WorkerID}}
	verification := providercontainment.Launch{ExecutionID: pkg.Identity.ThreadID + ":verify-1", Spec: providercontainment.Spec{WorkerID: pkg.WorkerID}}
	if _, err := manager.Supervisor.Start(ctx, provider); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Supervisor.Start(ctx, verification); err != nil {
		t.Fatal(err)
	}
	path, err := manager.recordPath(pkg)
	if err != nil {
		t.Fatal(err)
	}
	if err = privateJSON(path+".preparation", containedPreparation{Identity: pkg.Identity, WorkerID: pkg.WorkerID, Launch: provider}); err != nil {
		t.Fatal(err)
	}
	if err = privateJSON(path+".capture", containedCapture{Identity: pkg.Identity, WorkerID: pkg.WorkerID, Archive: []byte("{}")}); err != nil {
		t.Fatal(err)
	}
	if err = privateJSON(path+".verify.1", containedPreparation{Identity: pkg.Identity, WorkerID: pkg.WorkerID, Launch: verification}); err != nil {
		t.Fatal(err)
	}
	if err = manager.Quiesce(ctx, pkg, true); err != nil {
		t.Fatal(err)
	}
	for _, launch := range []providercontainment.Launch{provider, verification} {
		if stopped, err := manager.Supervisor.Observe(ctx, launch); err != nil || !stopped.Stopped {
			t.Fatalf("quiesce left custody for %s: %+v err=%v", launch.ExecutionID, stopped, err)
		}
	}
}

func TestAttentionStopObservationIsProducedByRuntimeAndConsumedBySQLite(t *testing.T) {
	ctx := context.Background()
	now := runtimeTestNow
	root := t.TempDir()
	driver := &fakeDriver{workspace: filepath.Join(root, "workspace"), observations: []backlog.DispatchThreadState{backlog.DispatchThreadActive, backlog.DispatchThreadActive}}
	runtime := newClaimedRuntimeWithClock(t, root, driver, func() time.Time { return now })
	if err := runtime.markPhase("assignment-1", PhaseRunning, "", driver.workspace, "thread-1"); err != nil {
		t.Fatal(err)
	}

	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for epoch := int64(1); epoch < 9; epoch++ {
		if _, err = store.AdvanceCoordinatorEpoch(ctx, epoch); err != nil {
			t.Fatal(err)
		}
	}

	pkg := testPackage()
	proveAttentionStopQuiescesProviderAndVerification(t, pkg)
	run := domain.WorkflowRun{ID: pkg.Identity.WorkflowRunID, WorkflowID: pkg.Identity.WorkflowID, Revision: 1, Progress: domain.ProgressActive, CreatedAt: now, UpdatedAt: now}
	task := domain.Task{ID: pkg.Identity.TaskID, Name: "task", WorkflowID: pkg.Identity.WorkflowID}
	run, err = domain.BindRunSink(run, []domain.Task{task})
	if err != nil {
		t.Fatal(err)
	}
	attempt := domain.Attempt{
		ID: pkg.Identity.AttemptID, WorkflowRunID: run.ID, TaskID: task.ID, Number: 1,
		Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 1,
		AssignmentID: pkg.Identity.AssignmentID, ThreadID: pkg.Identity.ThreadID, UpdatedAt: now,
	}
	assignment := domain.Assignment{
		ID: pkg.Identity.AssignmentID, AttemptID: attempt.ID, WorkerID: pkg.WorkerID, WorkerEpoch: pkg.WorkerEpoch,
		Route: pkg.Route, State: domain.AssignmentClaimed, Epoch: pkg.Identity.AssignmentEpoch,
		LeaseToken: "lease-1", DispatchToken: pkg.Identity.DispatchToken, ThreadID: pkg.Identity.ThreadID,
		ExecutorDemand: &domain.ResourceDemand{}, LeaseExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	}
	if err = store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{run}, Tasks: []domain.Task{task},
		Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := runtime.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	wait, err := store.RegisterTaskWait(ctx, domain.TaskWaitRegistration{
		RequestID: "request-1", WorkflowRunID: run.ID, TaskID: task.ID, AttemptID: attempt.ID,
		IssuedRevision: attempt.Revision, ThreadID: attempt.ThreadID, Wake: domain.WakeEach,
		MaxDuration: time.Hour, Name: "operator", Kind: domain.WaitKindAttention,
		Attention: &domain.AttentionRequest{Kind: domain.AttentionDirection, Prompt: "stop?", AssignmentID: assignment.ID},
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	decision := domain.AttentionDecision{
		ID: "decision-1", WaitID: wait.ID, RequestID: wait.RequestID, WorkflowRunID: wait.WorkflowRunID,
		TaskID: wait.TaskID, AttemptID: wait.AttemptID, AssignmentID: assignment.ID,
		AssignmentEpoch: wait.Attention.AssignmentEpoch, WorkerID: wait.Attention.WorkerID, ThreadID: wait.ThreadID,
		RegisteredRevision: wait.RegisteredRevision, ContentDigest: wait.Attention.ContentDigest,
		DecisionDeadline: wait.Attention.DecisionDeadline, Kind: domain.AttentionStop, Reason: "operator stop",
	}
	now = now.Add(time.Minute)
	_, receipt, err := store.DecideAttentionForCoordinator(ctx, decision, "remote:human", pkg.CoordinatorID, now)
	if err != nil || receipt.State != domain.AttentionApplied {
		t.Fatalf("apply stop receipt=%+v err=%v", receipt, err)
	}
	records, err := store.LoadThrottleAttemptRecords(ctx)
	if err != nil || len(records) != 1 {
		t.Fatalf("throttle records=%+v err=%v", records, err)
	}
	command := records[0].Command
	acks, err := runtime.DeliverThrottle(ctx, []domain.ThrottleCommand{command})
	if err != nil || len(acks) != 1 || !acks[0].Accepted || acks[0].Result != domain.ThrottleResultStopped {
		t.Fatalf("deliver stop acks=%+v err=%v", acks, err)
	}
	record := records[0]
	record.Revision++
	record.Delivery = domain.ThrottleDeliveryAcknowledged
	record.Result = acks[0].Result
	record.Control = domain.ControlPausedUncheckpointed
	record.AcknowledgedAt = &acks[0].AcknowledgedAt
	record.UpdatedAt = acks[0].AcknowledgedAt
	if err = store.CommitThrottleAttemptTransitions(ctx, []domain.ThrottleAttemptTransition{{
		ExpectedRevision: record.Revision - 1, Record: record,
	}}); err != nil {
		t.Fatal(err)
	}

	now = now.Add(time.Minute)
	runtime = newTestRuntimeWithClock(t, root, driver, func() time.Time { return now })
	snapshot, err = runtime.Snapshot(ctx)
	if err != nil || len(snapshot.Assignments) != 1 ||
		snapshot.Assignments[0].State != domain.AssignmentReleased || snapshot.Assignments[0].Control != domain.ControlStopped {
		t.Fatalf("runtime stop snapshot=%+v err=%v", snapshot.Assignments, err)
	}
	storedSnapshots, err := store.LoadWorkerSnapshots(ctx)
	if err != nil || len(storedSnapshots) != 1 {
		t.Fatalf("stored snapshots=%+v err=%v", storedSnapshots, err)
	}
	snapshot.Sequence = storedSnapshots[0].Sequence + 1
	if err = store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	waits, err := store.ListTaskWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range waits {
		if candidate.ID == wait.ID {
			got := candidate.AttentionReceipts[len(candidate.AttentionReceipts)-1]
			if got.State != domain.AttentionObserved || got.ObservedAt == nil {
				t.Fatalf("attention receipt=%+v", got)
			}
			return
		}
	}
	t.Fatal("attention wait disappeared")
}
