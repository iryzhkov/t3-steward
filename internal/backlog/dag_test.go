package backlog

import (
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var dagTestTime = time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)

func TestDAGExecutionChainReadinessStrictCompletionAndRetry(t *testing.T) {
	execution := newTestDAG(t,
		testTask("inspect"),
		testTask("implement", "inspect"),
		testTask("review", "implement"),
	)
	assertAttemptProgress(t, execution, "inspect-1", domain.ProgressReady)
	assertAttemptProgress(t, execution, "implement-1", domain.ProgressBlocked)
	assertRunProgress(t, execution, domain.ProgressReady)

	mustStart(t, execution, "inspect-1")
	mustComplete(t, execution, "inspect-1", CompletionResult{
		ExplicitSuccess: true, VerificationPassed: false,
	})
	assertAttemptProgress(t, execution, "inspect-1", domain.ProgressFailed)
	assertAttemptFailure(t, execution, "inspect-1", "verification failed")
	assertAttemptProgress(t, execution, "implement-1", domain.ProgressBlocked)
	assertRunProgress(t, execution, domain.ProgressFailed)

	if err := execution.RetryTask("task-inspect", "inspect-2", dagTestTime.Add(2*time.Minute)); err != nil {
		t.Fatalf("retry inspect: %v", err)
	}
	assertAttemptProgress(t, execution, "inspect-2", domain.ProgressReady)
	mustStartAt(t, execution, "inspect-2", dagTestTime.Add(3*time.Minute))
	mustCompleteAt(t, execution, "inspect-2", CompletionResult{
		ExplicitSuccess: true, VerificationPassed: true,
	}, dagTestTime.Add(4*time.Minute))
	assertAttemptProgress(t, execution, "inspect-1", domain.ProgressFailed)
	assertAttemptProgress(t, execution, "inspect-2", domain.ProgressSucceeded)
	assertAttemptProgress(t, execution, "implement-1", domain.ProgressReady)

	mustStart(t, execution, "implement-1")
	mustComplete(t, execution, "implement-1", CompletionResult{
		ExplicitSuccess: true, VerificationPassed: true,
	})
	assertAttemptProgress(t, execution, "review-1", domain.ProgressReady)
	mustStart(t, execution, "review-1")
	mustComplete(t, execution, "review-1", CompletionResult{
		ExplicitSuccess: true, VerificationPassed: true,
	})
	assertRunProgress(t, execution, domain.ProgressSucceeded)

	snapshot := execution.Snapshot()
	if snapshot.Run.CompletedAt == nil {
		t.Fatal("succeeded run has no completion time")
	}
	if snapshot.Run.Revision != 10 {
		t.Fatalf("run revision = %d, want 10", snapshot.Run.Revision)
	}
}

func TestDAGExecutionDiamondWaitsForEveryDependency(t *testing.T) {
	execution := newTestDAG(t,
		testTask("root"),
		testTask("left", "root"),
		testTask("right", "root"),
		testTask("join", "right", "left"),
	)
	succeedAttempt(t, execution, "root-1")
	assertAttemptProgress(t, execution, "left-1", domain.ProgressReady)
	assertAttemptProgress(t, execution, "right-1", domain.ProgressReady)

	succeedAttempt(t, execution, "left-1")
	assertAttemptProgress(t, execution, "join-1", domain.ProgressBlocked)
	assertAttemptFailure(t, execution, "join-1", "waiting for dependencies: right")

	succeedAttempt(t, execution, "right-1")
	assertAttemptProgress(t, execution, "join-1", domain.ProgressReady)
	succeedAttempt(t, execution, "join-1")
	assertRunProgress(t, execution, domain.ProgressSucceeded)
}

func TestDAGExecutionMissingSuccessNeverReleasesDependency(t *testing.T) {
	execution := newTestDAG(t, testTask("first"), testTask("second", "first"))
	mustStart(t, execution, "first-1")
	mustComplete(t, execution, "first-1", CompletionResult{VerificationPassed: true})

	assertAttemptProgress(t, execution, "first-1", domain.ProgressFailed)
	assertAttemptFailure(t, execution, "first-1", "missing explicit success")
	assertAttemptProgress(t, execution, "second-1", domain.ProgressBlocked)
	assertRunProgress(t, execution, domain.ProgressFailed)
}

func TestDAGExecutionFailureDoesNotStopIndependentBranch(t *testing.T) {
	execution := newTestDAG(t,
		testTask("failed-root"),
		testTask("blocked-child", "failed-root"),
		testTask("independent"),
	)
	mustStart(t, execution, "failed-root-1")
	mustComplete(t, execution, "failed-root-1", CompletionResult{Failure: "agent failed"})
	assertAttemptProgress(t, execution, "blocked-child-1", domain.ProgressBlocked)
	assertAttemptProgress(t, execution, "independent-1", domain.ProgressReady)
	assertRunProgress(t, execution, domain.ProgressReady)

	succeedAttempt(t, execution, "independent-1")
	assertRunProgress(t, execution, domain.ProgressFailed)
}

func TestDAGExecutionCancellationPropagatesAndPreservesSuccess(t *testing.T) {
	execution := newTestDAG(t,
		testTask("complete"),
		testTask("target", "complete"),
		testTask("child", "target"),
		testTask("grandchild", "child"),
	)
	succeedAttempt(t, execution, "complete-1")
	mustStart(t, execution, "target-1")

	if err := execution.CancelTask("task-target", dagTestTime.Add(time.Minute)); err != nil {
		t.Fatalf("cancel target: %v", err)
	}
	assertAttemptProgress(t, execution, "complete-1", domain.ProgressSucceeded)
	assertAttemptProgress(t, execution, "target-1", domain.ProgressCancelled)
	assertAttemptProgress(t, execution, "child-1", domain.ProgressCancelled)
	assertAttemptProgress(t, execution, "grandchild-1", domain.ProgressCancelled)
	assertRunProgress(t, execution, domain.ProgressCancelled)
}

func TestDAGExecutionSkipProjectsTerminalRunWithoutReleasingDependency(t *testing.T) {
	execution := newTestDAG(t,
		testTask("target"),
		testTask("child", "target"),
	)
	if err := execution.SkipTask("task-target", dagTestTime.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	assertAttemptProgress(t, execution, "target-1", domain.ProgressSkipped)
	assertAttemptProgress(t, execution, "child-1", domain.ProgressBlocked)
	assertRunProgress(t, execution, domain.ProgressSkipped)
	if err := execution.SkipTask("task-target", dagTestTime.Add(2*time.Minute)); err == nil {
		t.Fatal("skipped an already skipped task")
	}
}

func TestDAGExecutionValidatesGraphAndAttempts(t *testing.T) {
	tests := []struct {
		name  string
		state DAGState
		want  string
	}{
		{
			name:  "missing dependency",
			state: testDAGState(testTask("task", "absent")),
			want:  "needs missing task",
		},
		{
			name:  "cycle",
			state: testDAGState(testTask("left", "right"), testTask("right", "left")),
			want:  "contains a cycle",
		},
		{
			name:  "duplicate dependency",
			state: testDAGState(testTask("root"), testTask("child", "root", "root")),
			want:  "repeats dependency",
		},
		{
			name: "attempt for another run",
			state: func() DAGState {
				state := testDAGState(testTask("task"))
				state.Attempts[0].WorkflowRunID = "another-run"
				return state
			}(),
			want: "belongs to run",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewDAGExecution(test.state)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("NewDAGExecution error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestDAGExecutionRejectsInvalidTransitions(t *testing.T) {
	execution := newTestDAG(t, testTask("task"))
	if err := execution.CompleteAttempt("task-1", CompletionResult{
		ExplicitSuccess: true, VerificationPassed: true,
	}, dagTestTime); err == nil {
		t.Fatal("completed a ready attempt without starting it")
	}
	if err := execution.RetryTask("task-task", "task-2", dagTestTime); err == nil {
		t.Fatal("retried a nonterminal task")
	}
	succeedAttempt(t, execution, "task-1")
	if err := execution.RetryTask("task-task", "task-2", dagTestTime); err == nil {
		t.Fatal("retried a successful task")
	}
}

func newTestDAG(t *testing.T, tasks ...domain.Task) *DAGExecution {
	t.Helper()
	execution, err := NewDAGExecution(testDAGState(tasks...))
	if err != nil {
		t.Fatalf("new DAG execution: %v", err)
	}
	return execution
}

func testDAGState(tasks ...domain.Task) DAGState {
	state := DAGState{
		Run: domain.WorkflowRun{
			ID: "run", WorkflowID: "workflow", Progress: domain.ProgressQueued,
			Revision: 1, CreatedAt: dagTestTime, UpdatedAt: dagTestTime,
		},
		Tasks: tasks,
	}
	for _, task := range tasks {
		state.Attempts = append(state.Attempts, domain.Attempt{
			ID: task.Name + "-1", WorkflowRunID: "run", TaskID: task.ID, Number: 1,
			Progress: domain.ProgressBlocked, Control: domain.ControlUnassigned,
			UpdatedAt: dagTestTime,
		})
	}
	return state
}

func testTask(name string, needs ...string) domain.Task {
	return domain.Task{
		ID: "task-" + name, WorkflowID: "workflow", Name: name,
		Needs: append([]string(nil), needs...),
	}
}

func succeedAttempt(t *testing.T, execution *DAGExecution, attemptID string) {
	t.Helper()
	mustStart(t, execution, attemptID)
	mustComplete(t, execution, attemptID, CompletionResult{
		ExplicitSuccess: true, VerificationPassed: true,
	})
}

func mustStart(t *testing.T, execution *DAGExecution, attemptID string) {
	t.Helper()
	mustStartAt(t, execution, attemptID, dagTestTime)
}

func mustStartAt(t *testing.T, execution *DAGExecution, attemptID string, now time.Time) {
	t.Helper()
	if err := execution.StartAttempt(attemptID, now); err != nil {
		t.Fatalf("start %s: %v", attemptID, err)
	}
}

func mustComplete(t *testing.T, execution *DAGExecution, attemptID string, result CompletionResult) {
	t.Helper()
	mustCompleteAt(t, execution, attemptID, result, dagTestTime.Add(time.Minute))
}

func mustCompleteAt(t *testing.T, execution *DAGExecution, attemptID string, result CompletionResult, now time.Time) {
	t.Helper()
	if err := execution.CompleteAttempt(attemptID, result, now); err != nil {
		t.Fatalf("complete %s: %v", attemptID, err)
	}
}

func assertAttemptProgress(t *testing.T, execution *DAGExecution, attemptID string, want domain.ProgressState) {
	t.Helper()
	for _, attempt := range execution.Snapshot().Attempts {
		if attempt.ID == attemptID {
			if attempt.Progress != want {
				t.Fatalf("attempt %s progress = %q, want %q", attemptID, attempt.Progress, want)
			}
			return
		}
	}
	t.Fatalf("attempt %s not found", attemptID)
}

func assertAttemptFailure(t *testing.T, execution *DAGExecution, attemptID, want string) {
	t.Helper()
	for _, attempt := range execution.Snapshot().Attempts {
		if attempt.ID == attemptID {
			if attempt.Failure != want {
				t.Fatalf("attempt %s failure = %q, want %q", attemptID, attempt.Failure, want)
			}
			return
		}
	}
	t.Fatalf("attempt %s not found", attemptID)
}

func assertRunProgress(t *testing.T, execution *DAGExecution, want domain.ProgressState) {
	t.Helper()
	if got := execution.Snapshot().Run.Progress; got != want {
		t.Fatalf("run progress = %q, want %q", got, want)
	}
}
