package backlog

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestBacklogV2EndToEndLocalWorkflowHardening(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	statePath := filepath.Join(root, "state.db")
	artifactRoot := filepath.Join(root, "artifacts")
	bundle := validBundle(t)
	writeBundleFile(t, bundle, "prompts/implement.md", "consume the inspection and implement")
	rewriteBundleManifest(t, bundle, `
version: 2
name: hardening
class: required
environment: {project: t3-steward, ref: feature/backlog-orchestrator}
routes:
  - {host: normandy, instance: codex, model: gpt-5.6-sol, quota_pool: openai}
tasks:
  inspect:
    prompt_file: prompts/inspect.md
    outputs: [findings.md]
    verify: [test -s findings.md]
  implement:
    prompt_file: prompts/implement.md
    needs: [inspect]
    inputs_from: {inspect: [findings.md]}
    outputs: [result.txt]
    verify:
      - test -f result.txt && grep -q inspected .t3/dependencies/inspect/findings.md && grep -q complete result.txt
`)

	store, err := sqlite.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 10, 20, 0, 0, 0, time.UTC)
	nextID := 0
	ingested, err := (BundleIngester{
		StorageRoot: artifactRoot,
		Store:       store,
		Now:         func() time.Time { return now },
		NewID: func() string {
			nextID++
			return fmt.Sprintf("%02d", nextID)
		},
	}).Ingest(ctx, bundle)
	if err != nil {
		t.Fatalf("ingest local workflow: %v", err)
	}
	t.Cleanup(func() { _ = removeIngestedTree(artifactRoot) })

	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	execution, err := NewDAGExecution(DAGState{
		Run: records.WorkflowRuns[0], Tasks: records.Tasks, Attempts: records.Attempts,
	})
	if err != nil {
		t.Fatal(err)
	}
	tasks := tasksByName(records.Tasks)
	attempts := attemptsByTask(records.Attempts)
	finalizerID := 0
	finalizer := AttemptFinalizer{
		StorageRoot: artifactRoot,
		Processes:   testProcessRunner{},
		Now:         func() time.Time { return now.Add(time.Duration(finalizerID+1) * time.Minute) },
		NewID: func(kind string) string {
			finalizerID++
			return fmt.Sprintf("%s-e2e-%d", kind, finalizerID)
		},
	}

	if err := execution.StartAttempt(attempts[tasks["inspect"].ID].ID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	inspectWorkspace := filepath.Join(root, "inspect-workspace")
	writeTestFile(t, inspectWorkspace, "findings.md", "inspected dependency\n")
	state := execution.Snapshot()
	inspectAttempt := attemptsByTask(state.Attempts)[tasks["inspect"].ID]
	inspectFinal, err := finalizer.Finalize(ctx, AttemptFinalization{
		Task: tasks["inspect"], Attempt: inspectAttempt, WorkspaceDir: inspectWorkspace, ExplicitSuccess: true,
	})
	if err != nil {
		t.Fatalf("finalize inspection: %v", err)
	}
	cleanupImmutable(t, inspectFinal.StorageDir)
	if err := execution.CompleteAttempt(inspectAttempt.ID, inspectFinal.Completion, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	state = execution.Snapshot()
	if attemptsByTask(state.Attempts)[tasks["implement"].ID].Progress != domain.ProgressReady {
		t.Fatalf("dependency did not release implementation: %#v", state.Attempts)
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{state.Run}, Attempts: state.Attempts, Artifacts: inspectFinal.Artifacts,
	}); err != nil {
		t.Fatalf("persist inspection completion: %v", err)
	}

	implementWorkspace := filepath.Join(root, "implement-attempt-1")
	writeTestFile(t, implementWorkspace, ".workspace", "")
	materialized, err := MaterializeDependencies(
		implementWorkspace, artifactRoot, ingested.RunID, tasks["implement"], records.Tasks, inspectFinal.Artifacts,
	)
	if err != nil {
		t.Fatalf("materialize dependency artifacts: %v", err)
	}
	if !reflect.DeepEqual(materialized, []string{".t3/dependencies/inspect/findings.md"}) {
		t.Fatalf("materialized dependencies = %#v", materialized)
	}
	cleanupImmutable(t, filepath.Join(implementWorkspace, ".t3", "dependencies"))
	implementAttempt := attemptsByTask(state.Attempts)[tasks["implement"].ID]
	if err := execution.StartAttempt(implementAttempt.ID, now.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	state = execution.Snapshot()
	implementAttempt = attemptsByTask(state.Attempts)[tasks["implement"].ID]
	failed, err := finalizer.Finalize(ctx, AttemptFinalization{
		Task: tasks["implement"], Attempt: implementAttempt, WorkspaceDir: implementWorkspace, ExplicitSuccess: true,
	})
	if err != nil {
		t.Fatalf("finalize failed implementation: %v", err)
	}
	cleanupImmutable(t, failed.StorageDir)
	if failed.Completion.VerificationPassed {
		t.Fatal("missing declared output unexpectedly passed verification")
	}
	if err := execution.CompleteAttempt(implementAttempt.ID, failed.Completion, now.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := execution.RetryTask(tasks["implement"].ID, "attempt-retry", now.Add(5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := execution.StartAttempt("attempt-retry", now.Add(6*time.Minute)); err != nil {
		t.Fatal(err)
	}

	retryWorkspace := filepath.Join(root, "implement-attempt-2")
	writeTestFile(t, retryWorkspace, ".workspace", "")
	if _, err := MaterializeDependencies(
		retryWorkspace, artifactRoot, ingested.RunID, tasks["implement"], records.Tasks, inspectFinal.Artifacts,
	); err != nil {
		t.Fatalf("materialize retry dependencies: %v", err)
	}
	cleanupImmutable(t, filepath.Join(retryWorkspace, ".t3", "dependencies"))
	state = execution.Snapshot()
	retry := attemptByID(t, state.Attempts, "attempt-retry")
	retry.Revision = 1
	retry.AssignmentID = "assignment-retry"
	retry.ThreadID = "thread-retry"
	replaceAttempt(state.Attempts, retry)
	assignment := domain.Assignment{
		ID: "assignment-retry", AttemptID: retry.ID, WorkerID: "normandy", WorkerEpoch: "worker-epoch-1",
		Route: domain.ProviderRoute{
			WorkerID: "normandy", ProviderInstanceID: "codex", Model: "gpt-5.6-sol",
			Options: map[string]string{"effort": "medium"}, QuotaPoolID: "openai",
		},
		State: domain.AssignmentClaimed, Epoch: 1, LeaseToken: "lease-retry",
		LeaseExpiresAt: now.Add(time.Hour), DispatchToken: "dispatch-retry",
		ThreadID: retry.ThreadID, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{state.Run}, Attempts: state.Attempts,
		Assignments: []domain.Assignment{assignment}, Artifacts: failed.Artifacts,
	}); err != nil {
		t.Fatalf("persist retry before pause: %v", err)
	}
	binding := ThrottleAttemptBinding{Attempt: retry, Assignment: assignment, WorkspacePath: retryWorkspace}
	pause, err := PlanAdminPauseDelivery("pause-retry", "end-to-end checkpoint", false, binding, now.Add(7*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CommitThrottleAttemptTransitions(ctx, []domain.ThrottleAttemptTransition{pause}); err != nil {
		t.Fatalf("persist pause intent: %v", err)
	}
	transport := throttleTransportFunc(func(_ context.Context, workerID string, commands []domain.ThrottleCommand) ([]domain.ThrottleAcknowledgement, error) {
		if workerID != "normandy" || len(commands) != 1 {
			t.Fatalf("unexpected throttle delivery to %q: %#v", workerID, commands)
		}
		result := domain.ThrottleResultCheckpointed
		var checkpoint *domain.CheckpointMetadata
		if commands[0].Kind == domain.ThrottleCommandResume {
			result = domain.ThrottleResultResumed
		} else {
			checkpoint = &domain.CheckpointMetadata{
				ArtifactID: "checkpoint-retry", Path: ".t3/checkpoint.md",
				SHA256: "checkpoint-sha", Size: 42, CapturedAt: now.Add(8 * time.Minute),
			}
		}
		return []domain.ThrottleAcknowledgement{{
			CommandID: commands[0].ID, AttemptID: retry.ID, Accepted: true,
			Result: result, Checkpoint: checkpoint, AcknowledgedAt: now.Add(8 * time.Minute),
		}}, nil
	})
	paused, err := ReconcilePendingThrottleCommands(ctx, store, transport, now.Add(8*time.Minute))
	if err != nil || len(paused.Acknowledgements) != 1 {
		t.Fatalf("pause reconciliation = %#v, err = %v", paused, err)
	}
	pausedRecords, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pausedRetry := attemptByID(t, pausedRecords.Attempts, retry.ID)
	if pausedRetry.Control != domain.ControlPaused || pausedRetry.CheckpointArtifactID != "checkpoint-retry" {
		t.Fatalf("durable paused retry = %#v", pausedRetry)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlite.Open(statePath)
	if err != nil {
		t.Fatalf("restart coordinator store: %v", err)
	}
	defer store.Close()

	resumed, err := ReconcileThrottleResumes(
		ctx, store, transport,
		[]domain.QuotaAdmissionRecord{{
			QuotaPoolID: "openai", Revision: 2, Admission: domain.AdmissionRecovering,
			Reason: "recovery probe", ObservedAt: now.Add(9 * time.Minute), AppliedAt: now.Add(9 * time.Minute),
		}},
		[]domain.QuotaPool{{ID: "openai", MaxConcurrent: 1}},
		now.Add(9*time.Minute),
	)
	if err != nil || len(resumed.Commands) != 1 || resumed.Commands[0].Kind != domain.ThrottleCommandResume {
		t.Fatalf("resume reconciliation = %#v, err = %v", resumed, err)
	}

	records, err = store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	retry = attemptByID(t, records.Attempts, retry.ID)
	if retry.Control != domain.ControlRunning || retry.CheckpointArtifactID != "checkpoint-retry" {
		t.Fatalf("resumed retry = %#v", retry)
	}
	writeTestFile(t, retryWorkspace, "result.txt", "complete implementation\n")
	succeeded, err := finalizer.Finalize(ctx, AttemptFinalization{
		Task: tasks["implement"], Attempt: retry, WorkspaceDir: retryWorkspace, ExplicitSuccess: true,
	})
	if err != nil {
		t.Fatalf("finalize retry: %v", err)
	}
	cleanupImmutable(t, succeeded.StorageDir)
	if !succeeded.Completion.VerificationPassed {
		t.Fatalf("retry verification = %#v", succeeded.Completion)
	}
	execution, err = NewDAGExecution(DAGState{
		Run: records.WorkflowRuns[0], Tasks: records.Tasks, Attempts: records.Attempts,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := execution.CompleteAttempt(retry.ID, succeeded.Completion, now.Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	state = execution.Snapshot()
	if state.Run.Progress != domain.ProgressSucceeded {
		t.Fatalf("workflow completion = %#v", state.Run)
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{state.Run}, Attempts: state.Attempts, Artifacts: succeeded.Artifacts,
	}); err != nil {
		t.Fatalf("persist workflow completion: %v", err)
	}

	schedule := domain.Schedule{
		ID: "schedule-hardening", Name: "hardening", Version: 1, WorkflowID: ingested.WorkflowID,
		Expression: "0 2 * * *", Timezone: "UTC", Overlap: domain.ScheduleOverlapForbid,
		Misfire: domain.ScheduleMisfireSkip, AfterFailure: domain.ScheduleFailureNextCycle,
		Enabled: true, Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	template := domain.ScheduleTemplate{
		ScheduleID: schedule.ID, Version: 1, WorkflowID: ingested.WorkflowID,
		Expression: schedule.Expression, Timezone: schedule.Timezone, Overlap: schedule.Overlap,
		Misfire: schedule.Misfire, AfterFailure: schedule.AfterFailure, CreatedAt: now,
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Schedules: []domain.Schedule{schedule}, ScheduleTemplates: []domain.ScheduleTemplate{template},
	}); err != nil {
		t.Fatalf("persist schedule: %v", err)
	}
	first, err := store.CommitScheduleTrigger(ctx, domain.ScheduleTriggerRequest{
		ScheduleID: schedule.ID, TriggerID: "trigger-first", WorkflowRunID: "scheduled-run-first",
		NominalAt: now.Add(time.Hour), ObservedAt: now.Add(time.Hour), Source: domain.ScheduleTriggerScheduled,
	})
	if err != nil || first.Trigger.State != domain.TriggerAccepted || first.WorkflowRun == nil {
		t.Fatalf("first trigger = %#v, err = %v", first, err)
	}
	second, err := store.CommitScheduleTrigger(ctx, domain.ScheduleTriggerRequest{
		ScheduleID: schedule.ID, TriggerID: "trigger-second", WorkflowRunID: "scheduled-run-second",
		NominalAt: now.Add(2 * time.Hour), ObservedAt: now.Add(2 * time.Hour), Source: domain.ScheduleTriggerScheduled,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Trigger.State != domain.TriggerSuppressed || second.Trigger.Reason != "overlap-forbidden" || second.WorkflowRun != nil {
		t.Fatalf("overlapping trigger = %#v", second)
	}
}

func tasksByName(tasks []domain.Task) map[string]domain.Task {
	result := make(map[string]domain.Task, len(tasks))
	for _, task := range tasks {
		result[task.Name] = task
	}
	return result
}

func attemptsByTask(attempts []domain.Attempt) map[string]domain.Attempt {
	result := make(map[string]domain.Attempt, len(attempts))
	for _, attempt := range attempts {
		if current, ok := result[attempt.TaskID]; !ok || attempt.Number > current.Number {
			result[attempt.TaskID] = attempt
		}
	}
	return result
}

func attemptByID(t *testing.T, attempts []domain.Attempt, id string) domain.Attempt {
	t.Helper()
	for _, attempt := range attempts {
		if attempt.ID == id {
			return attempt
		}
	}
	t.Fatalf("attempt %q not found in %#v", id, attempts)
	return domain.Attempt{}
}

func replaceAttempt(attempts []domain.Attempt, replacement domain.Attempt) {
	for index := range attempts {
		if attempts[index].ID == replacement.ID {
			attempts[index] = replacement
			return
		}
	}
}
