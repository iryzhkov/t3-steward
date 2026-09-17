package backlogadmin

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func parkedRecords(now time.Time) sqlite.CoordinatorRecords {
	return sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "workflow-1", Version: 2, Name: "workflow", Class: domain.TaskClassRequired, TaskIDs: []string{"task-1"}}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressActive, Revision: 2, CreatedAt: now, UpdatedAt: now}},
		Tasks:        []domain.Task{{ID: "task-1", WorkflowID: "workflow-1", Name: "nest-model", Class: domain.TaskClassRequired}},
		Attempts: []domain.Attempt{{
			ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1,
			Progress: domain.ProgressWaitingExternal, Control: domain.ControlWaitingExternal, Revision: 6,
			AssignmentID: "assignment-1", ThreadID: "thread-1", UpdatedAt: now,
		}},
		Assignments: []domain.Assignment{{
			ID: "assignment-1", AttemptID: "attempt-1", WorkerID: "homelab", WorkerEpoch: "worker-1",
			Route: domain.ProviderRoute{WorkerID: "homelab", ProviderInstanceID: "claudeAgent", Model: "opus", QuotaPoolID: "claude-main"},
			State: domain.AssignmentClaimed, Epoch: 1, ThreadID: "thread-1", LeaseExpiresAt: now.Add(time.Minute),
		}},
	}
}

// S-2: an attempt parked in waiting-external whose wait was cancelled from
// the thread has no live wait and nothing to wake it. rewake resumes it the
// way a settled wait's wake would; with a live wait it is refused, naming the
// wait, because the wait's settlement is the ordinary way out.
func TestPlanAdminRewakeResumesParkedAttemptWithoutLiveWait(t *testing.T) {
	now := adminTestNow
	records := parkedRecords(now)
	command := domain.AdminCommand{
		ID: "rewake-1", Kind: domain.AdminCommandRewake, TargetType: domain.AdminTargetAttempt,
		TargetID: "attempt-1", ExpectedRevision: 6, Reason: "the user answered in the thread",
		State: domain.AdminCommandPending, CreatedAt: now,
	}
	application, _, err := planAdminCommandWithWaits(records, nil, nil, nil, command, now)
	if err != nil {
		t.Fatal(err)
	}
	if application.State != domain.AdminCommandApplied || application.Attempt == nil ||
		application.Attempt.Progress != domain.ProgressActive || application.Attempt.Control != domain.ControlResuming ||
		application.Attempt.Revision != 7 || application.Attempt.AssignmentID != "assignment-1" {
		t.Fatalf("application = %#v attempt=%#v", application, application.Attempt)
	}
	// Refused while a wait is live, naming it.
	refused, _, err := planAdminCommandWithWaits(records, nil, nil, map[string]string{"attempt-1": "tw-nest-model-1"}, command, now)
	if err != nil {
		t.Fatal(err)
	}
	if refused.State != domain.AdminCommandRejected || !strings.Contains(refused.Failure, "task wait tw-nest-model-1 is live") ||
		!strings.Contains(refused.Failure, "allowed: cancel") || strings.Contains(refused.Failure, "rewake, cancel") {
		t.Fatalf("refusal = %#v", refused)
	}
	// Refused for an attempt that is not parked.
	records.Attempts[0].Progress, records.Attempts[0].Control = domain.ProgressActive, domain.ControlRunning
	notParked, _, err := planAdminCommandWithWaits(records, nil, nil, nil, command, now)
	if err != nil {
		t.Fatal(err)
	}
	if notParked.State != domain.AdminCommandRejected || notParked.Failure != "rewake is invalid from active/running; allowed: pause, cancel, skip" {
		t.Fatalf("not parked = %#v", notParked)
	}
	// Refused when the execution that parked the attempt is gone.
	records = parkedRecords(now)
	records.Assignments[0].State = domain.AssignmentReleased
	released, _, err := planAdminCommandWithWaits(records, nil, nil, nil, command, now)
	if err != nil {
		t.Fatal(err)
	}
	if released.State != domain.AdminCommandRejected || !strings.Contains(released.Failure, "requires a claimed assignment") {
		t.Fatalf("released = %#v", released)
	}
}

// The whole path through the store: a rewake is accepted as a command kind,
// applied under the revision fence and leaves the attempt resuming.
func TestExecutePendingRewakeAppliesThroughTheStore(t *testing.T) {
	store := openAdminTestStore(t)
	now := adminTestNow
	if err := store.SaveCoordinatorRecords(context.Background(), parkedRecords(now)); err != nil {
		t.Fatal(err)
	}
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now.Add(time.Minute) })
	submitted, err := service.Mutate(context.Background(), Mutation{
		Version: Version, Principal: Principal{ID: "operator"}, ID: "rewake-store",
		Kind: domain.AdminCommandRewake, WorkflowRunID: "run-1", TaskID: "nest-model",
		ExpectedRevision: 6, Reason: "the user answered in the thread",
	})
	if err != nil || submitted.Command.State != domain.AdminCommandPending {
		t.Fatalf("submission = %#v, %v", submitted, err)
	}
	report, err := service.ExecutePendingCommands(context.Background())
	if err != nil || len(report.Decisions) != 1 || report.Decisions[0].Command.State != domain.AdminCommandApplied {
		t.Fatalf("execution = %#v, %v", report, err)
	}
	loaded, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Attempts[0].Progress != domain.ProgressActive || loaded.Attempts[0].Control != domain.ControlResuming || loaded.Attempts[0].Revision != 7 {
		t.Fatalf("attempt = %#v", loaded.Attempts[0])
	}
}

// S-5: a rejection names the commands the current state admits.
func TestPlanAdminRejectionsNameAllowedCommands(t *testing.T) {
	now := adminTestNow
	tests := []struct {
		name     string
		kind     domain.AdminCommandKind
		progress domain.ProgressState
		control  domain.ControlState
		want     string
	}{
		{name: "resume parked", kind: domain.AdminCommandResume, progress: domain.ProgressWaitingExternal, control: domain.ControlWaitingExternal,
			want: "resume is invalid from waiting-external/waiting-external; allowed: rewake, cancel, skip"},
		{name: "retry parked", kind: domain.AdminCommandRetry, progress: domain.ProgressWaitingExternal, control: domain.ControlWaitingExternal,
			want: "retry is invalid from waiting-external/waiting-external; allowed: rewake, cancel, skip"},
		{name: "start running", kind: domain.AdminCommandStart, progress: domain.ProgressActive, control: domain.ControlRunning,
			want: "start is invalid from active/running; allowed: pause, cancel, skip"},
		{name: "cancel succeeded", kind: domain.AdminCommandCancel, progress: domain.ProgressSucceeded, control: domain.ControlStopped,
			want: "cancel is invalid from succeeded/stopped; allowed: none (the attempt is final; submit a new workflow)"},
		{name: "pause failed", kind: domain.AdminCommandPause, progress: domain.ProgressFailed, control: domain.ControlStopped,
			want: "pause is invalid from failed/stopped; allowed: retry, skip"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			records := parkedRecords(now)
			records.Attempts[0].Progress, records.Attempts[0].Control = test.progress, test.control
			command := domain.AdminCommand{
				ID: "invalid-" + test.name, Kind: test.kind, TargetType: domain.AdminTargetAttempt,
				TargetID: "attempt-1", ExpectedRevision: 6, State: domain.AdminCommandPending, CreatedAt: now,
			}
			application, _, err := planAdminCommand(records, nil, nil, command, now)
			if err != nil {
				t.Fatal(err)
			}
			if application.State != domain.AdminCommandRejected || application.Failure != test.want {
				t.Fatalf("failure = %q, want %q", application.Failure, test.want)
			}
		})
	}
}

// S-17: the task detail carries the worker's last report on the attempt:
// thread, worker, observed control and session state, and the pause reason.
func TestTaskDetailCarriesAttemptEvidence(t *testing.T) {
	now := adminTestNow
	records := parkedRecords(now)
	records.Attempts[0].Progress, records.Attempts[0].Control = domain.ProgressActive, domain.ControlPaused
	workers := []domain.WorkerSnapshot{{
		WorkerID: "homelab", WorkerEpoch: "worker-1", Sequence: 4, Connected: true, ObservedAt: now, ValidUntil: now.Add(time.Minute),
		Inventory: domain.WorkerInventory{ID: "homelab", WebBaseURL: "https://homelab.example"},
		Assignments: []domain.WorkerAssignmentObservation{{
			AssignmentID: "assignment-1", AssignmentEpoch: 1, State: domain.AssignmentClaimed, Control: domain.ControlPaused,
			ThreadID: "thread-1", WorkspacePath: "/runs/attempt-1/workspace", ObservedAt: now,
			Journal: &domain.WorkerJournalExcerpt{Phase: "stopped", ThreadState: "stopped", PauseReason: "claudeAgent/claude/seven_day at 97%"},
		}},
	}}
	v := newView(records, workers, nil, RuntimeInfo{}, now)
	detail, ok := v.taskDetail("run-1", "nest-model")
	if !ok || detail.Evidence == nil {
		t.Fatalf("detail = %#v ok=%v", detail, ok)
	}
	evidence := *detail.Evidence
	if evidence.ThreadID != "thread-1" || evidence.WorkerID != "homelab" || evidence.Control != domain.ControlPaused ||
		evidence.Phase != "stopped" || evidence.ThreadState != "stopped" || evidence.PauseReason != "claudeAgent/claude/seven_day at 97%" ||
		!evidence.ObservedAt.Equal(now) {
		t.Fatalf("evidence = %#v", evidence)
	}
	// An attempt the worker has not reported yet still names its thread and worker.
	v = newView(records, nil, nil, RuntimeInfo{}, now)
	detail, _ = v.taskDetail("run-1", "nest-model")
	if detail.Evidence == nil || detail.Evidence.ThreadID != "thread-1" || detail.Evidence.WorkerID != "homelab" || detail.Evidence.Phase != "" {
		t.Fatalf("unreported evidence = %#v", detail.Evidence)
	}
}
