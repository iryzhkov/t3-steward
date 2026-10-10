package backlog

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestParseManifestRetryBlocks(t *testing.T) {
	t.Parallel()
	manifest := mustParseManifest(t, `
version: 2
name: retries
environment:
  project: t3-steward
retry:
  infrastructure: 1
  backoff: 5m
tasks:
  inherits:
    prompt_file: prompts/a.md
  overrides:
    prompt_file: prompts/b.md
    retry:
      infrastructure: 4
  opted-out:
    prompt_file: prompts/c.md
    retry:
      infrastructure: 0
`)
	policy := func(name string) *domain.TaskRetryPolicy {
		return taskRetryPolicy(effectiveManifestRetry(manifest.Retry, manifest.Tasks[name].Retry))
	}
	if got := policy("inherits"); got == nil || *got != (domain.TaskRetryPolicy{Infrastructure: 1, Backoff: 5 * time.Minute}) {
		t.Fatalf("inherited = %+v", got)
	}
	if got := policy("overrides"); got == nil || *got != (domain.TaskRetryPolicy{Infrastructure: 4, Backoff: 5 * time.Minute}) {
		t.Fatalf("overridden = %+v", got)
	}
	if got := policy("opted-out"); got == nil || got.Infrastructure != 0 {
		t.Fatalf("opted out = %+v", got)
	}
	// No retry block anywhere keeps the definition free of a policy, so the
	// defaults apply at runtime and older definitions are unchanged.
	if got := taskRetryPolicy(effectiveManifestRetry(nil, nil)); got != nil {
		t.Fatalf("undeclared = %+v", got)
	}
}

func TestParseManifestRefusesInvalidRetryBlocks(t *testing.T) {
	t.Parallel()
	for name, block := range map[string]string{
		"budget too large":   "retry:\n  infrastructure: 6\n",
		"negative budget":    "retry:\n  infrastructure: -1\n",
		"zero backoff":       "retry:\n  backoff: 0s\n",
		"backoff too long":   "retry:\n  backoff: 2h\n",
		"empty block":        "retry: {}\n",
		"unknown retry knob": "retry:\n  code: 1\n",
	} {
		for _, level := range []string{"workflow", "task"} {
			raw := "version: 2\nname: retries\nenvironment:\n  project: t3-steward\n"
			task := "tasks:\n  work:\n    prompt_file: prompts/a.md\n"
			if level == "workflow" {
				raw += block + task
			} else {
				raw += task + "    " + strings.ReplaceAll(strings.TrimSuffix(block, "\n"), "\n", "\n    ") + "\n"
			}
			if _, err := ParseManifest([]byte(raw)); err == nil {
				t.Errorf("%s at %s level was accepted:\n%s", name, level, raw)
			}
		}
	}
}

var retryTestNow = time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)

// retryRecords is one unsupervised run with a single task whose latest
// attempt failed with reason.
func retryRecords(reason string, retry *domain.TaskRetryPolicy, earlier ...domain.Attempt) sqlite.CoordinatorRecords {
	completed := retryTestNow.Add(-time.Minute)
	attempts := append([]domain.Attempt(nil), earlier...)
	attempts = append(attempts, domain.Attempt{
		ID: "attempt-latest", WorkflowRunID: "run-1", TaskID: "task-1", Number: len(earlier) + 1,
		Progress: domain.ProgressFailed, Control: domain.ControlStopped, Revision: 7, Failure: reason,
		UpdatedAt: completed, CompletedAt: &completed,
	})
	return sqlite.CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressActive}},
		Tasks:        []domain.Task{{ID: "task-1", WorkflowID: "workflow-1", Name: "build", Retry: retry}},
		Attempts:     attempts,
	}
}

func TestPlanAutomaticRetriesRetriesInfrastructureFailures(t *testing.T) {
	t.Parallel()
	records := retryRecords("T3 thread creation failed: connection refused", nil)
	plan := PlanAutomaticRetries(records, 3, retryTestNow)
	if len(plan.Stamps) != 1 || plan.Stamps[0].Classification != (domain.FailureClassification{Class: domain.FailureInfrastructure, Code: domain.ReasonThreadStartFailed}) ||
		plan.Stamps[0].Revision != 7 {
		t.Fatalf("stamps = %+v", plan.Stamps)
	}
	if len(plan.Commands) != 1 {
		t.Fatalf("commands = %+v", plan.Commands)
	}
	command := plan.Commands[0]
	if command.ID != "auto-retry-attempt-latest" || command.Kind != domain.AdminCommandRetry ||
		command.TargetType != domain.AdminTargetAttempt || command.TargetID != "attempt-latest" ||
		command.ExpectedRevision != 7 || command.RequestedBy != domain.AutomaticRetryRequestedBy ||
		command.State != domain.AdminCommandPending || !strings.Contains(command.Reason, "automatic retry 1 of 2") {
		t.Fatalf("command = %+v", command)
	}
	receipt, ok := AutomaticRetryFromCommand(command)
	if !ok {
		t.Fatalf("payload %s is not an automatic retry", command.Payload)
	}
	want := domain.AutomaticRetry{
		SourceAttemptID: "attempt-latest", Class: domain.FailureInfrastructure, Code: domain.ReasonThreadStartFailed,
		Ordinal: 1, Budget: 2, NotBefore: retryTestNow.Add(-time.Minute + domain.DefaultRetryBackoff), CommandID: command.ID,
	}
	if receipt != want {
		t.Fatalf("receipt = %+v, want %+v", receipt, want)
	}
	if pending := AutomaticRetryPendingRuns(records, 3); !pending["run-1"] {
		t.Fatal("a due retry does not hold its run")
	}
}

func TestPlanAutomaticRetriesNeverRetriesOtherClasses(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{
		"verification command failed (2): make test",                        // code
		"gate command failed (1): go vet ./...: exit status 1",              // code
		"permanent collection secret failure: result secret scan refused x", // policy
		"task timeout expired; execution stopped",                           // policy
		"missing declared output: handoff.md",                               // protocol
		"a reason nobody has classified",                                    // unknown
	} {
		records := retryRecords(reason, nil)
		plan := PlanAutomaticRetries(records, 3, retryTestNow)
		if len(plan.Commands) != 0 {
			t.Errorf("%q was retried: %+v", reason, plan.Commands)
		}
		if len(plan.Stamps) != 1 {
			t.Errorf("%q was not classified", reason)
		}
		if pending := AutomaticRetryPendingRuns(records, 3); len(pending) != 0 {
			t.Errorf("%q holds its run", reason)
		}
	}
	cancelled := retryRecords("", nil)
	cancelled.Attempts[0].Progress = domain.ProgressCancelled
	if plan := PlanAutomaticRetries(cancelled, 3, retryTestNow); len(plan.Commands) != 0 ||
		len(plan.Stamps) != 1 || plan.Stamps[0].Classification.Class != domain.FailureCancelled {
		t.Fatalf("cancelled plan = %+v", plan)
	}
}

func TestPlanAutomaticRetriesRespectsBudgetsAndBackoff(t *testing.T) {
	t.Parallel()
	reason := "provider turn did not complete successfully: overloaded"
	automatic := func(id string, number int) domain.Attempt {
		return domain.Attempt{
			ID: id, WorkflowRunID: "run-1", TaskID: "task-1", Number: number, Progress: domain.ProgressFailed, Failure: reason,
			AutomaticRetry: &domain.AutomaticRetry{SourceAttemptID: "x", Ordinal: number - 1, Budget: 2},
		}
	}
	first := domain.Attempt{ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1", Number: 1, Progress: domain.ProgressFailed, Failure: reason}

	// The second automatic retry waits twice the backoff.
	second := PlanAutomaticRetries(retryRecords(reason, nil, first, automatic("attempt-2", 2)), 3, retryTestNow)
	if len(second.Commands) != 1 {
		t.Fatalf("second retry = %+v", second.Commands)
	}
	if receipt, _ := AutomaticRetryFromCommand(second.Commands[0]); receipt.Ordinal != 2 ||
		!receipt.NotBefore.Equal(retryTestNow.Add(-time.Minute+2*domain.DefaultRetryBackoff)) {
		t.Fatalf("second receipt = %+v", receipt)
	}
	// The default budget of two is spent.
	exhausted := retryRecords(reason, nil, first, automatic("attempt-2", 2), automatic("attempt-3", 3))
	if plan := PlanAutomaticRetries(exhausted, 3, retryTestNow); len(plan.Commands) != 0 {
		t.Fatalf("exhausted budget retried: %+v", plan.Commands)
	}
	if pending := AutomaticRetryPendingRuns(exhausted, 3); len(pending) != 0 {
		t.Fatal("an exhausted budget holds its run")
	}
	// An operator's retry does not spend the automatic budget.
	operator := first
	operator.ID, operator.Number = "attempt-2", 2
	if plan := PlanAutomaticRetries(retryRecords(reason, nil, first, operator), 3, retryTestNow); len(plan.Commands) != 1 {
		t.Fatalf("operator retry spent the budget: %+v", plan.Commands)
	}
	// A task that opts out, and a coordinator ceiling of zero, retry nothing.
	if plan := PlanAutomaticRetries(retryRecords(reason, &domain.TaskRetryPolicy{Infrastructure: 0}), 3, retryTestNow); len(plan.Commands) != 0 {
		t.Fatalf("opted-out task retried: %+v", plan.Commands)
	}
	if plan := PlanAutomaticRetries(retryRecords(reason, nil), 0, retryTestNow); len(plan.Commands) != 0 || len(plan.Stamps) != 1 {
		t.Fatalf("disabled coordinator = %+v", plan)
	}
	// The ceiling caps a larger declared budget.
	capped := retryRecords(reason, &domain.TaskRetryPolicy{Infrastructure: 5, Backoff: time.Minute}, first)
	plan := PlanAutomaticRetries(capped, 1, retryTestNow)
	if len(plan.Commands) != 1 {
		t.Fatalf("capped first retry = %+v", plan.Commands)
	}
	if receipt, _ := AutomaticRetryFromCommand(plan.Commands[0]); receipt.Budget != 1 {
		t.Fatalf("capped receipt = %+v", receipt)
	}
	// A backoff that already passed is not scheduled in the past.
	late := retryRecords(reason, &domain.TaskRetryPolicy{Infrastructure: 1, Backoff: time.Second})
	if receipt, _ := AutomaticRetryFromCommand(PlanAutomaticRetries(late, 3, retryTestNow).Commands[0]); !receipt.NotBefore.Equal(retryTestNow) {
		t.Fatalf("late receipt = %+v", receipt)
	}
}

func TestPlanAutomaticRetriesSkipsSupervisedFinalAndDecidedRuns(t *testing.T) {
	t.Parallel()
	reason := "T3 thread never started: no turn"
	supervised := retryRecords(reason, nil)
	supervised.WorkflowRuns[0].Supervision = &domain.SupervisionRecord{}
	if plan := PlanAutomaticRetries(supervised, 3, retryTestNow); len(plan.Commands) != 0 {
		t.Fatalf("supervised run retried: %+v", plan.Commands)
	}
	final := retryRecords(reason, nil)
	final.WorkflowRuns[0].Sink = &domain.SinkTask{Progress: domain.ProgressFailed}
	if plan := PlanAutomaticRetries(final, 3, retryTestNow); len(plan.Commands) != 0 {
		t.Fatalf("final run retried: %+v", plan.Commands)
	}
	// A submitted command is never submitted again, and holds the run only
	// while it is pending.
	submitted := retryRecords(reason, nil)
	command := PlanAutomaticRetries(submitted, 3, retryTestNow).Commands[0]
	submitted.AdminCommands = []domain.AdminCommand{command}
	if plan := PlanAutomaticRetries(submitted, 3, retryTestNow); len(plan.Commands) != 0 {
		t.Fatalf("resubmitted: %+v", plan.Commands)
	}
	if pending := AutomaticRetryPendingRuns(submitted, 3); !pending["run-1"] {
		t.Fatal("a pending retry command does not hold its run")
	}
	submitted.AdminCommands[0].State = domain.AdminCommandRejected
	if pending := AutomaticRetryPendingRuns(submitted, 3); len(pending) != 0 {
		t.Fatal("a rejected retry command still holds its run")
	}
	// An operator retry already pending is left to apply on its own.
	operator := retryRecords(reason, nil)
	operator.AdminCommands = []domain.AdminCommand{{ID: "op", Kind: domain.AdminCommandRetry, TargetType: domain.AdminTargetAttempt,
		TargetID: "attempt-latest", State: domain.AdminCommandPending}}
	if plan := PlanAutomaticRetries(operator, 3, retryTestNow); len(plan.Commands) != 0 {
		t.Fatalf("duplicated an operator retry: %+v", plan.Commands)
	}
	// A recorded classification is not recorded again.
	stamped := retryRecords(reason, nil)
	stamped.Attempts[0].FailureClass, stamped.Attempts[0].FailureReason = domain.FailureInfrastructure, domain.ReasonThreadStartFailed
	if plan := PlanAutomaticRetries(stamped, 3, retryTestNow); len(plan.Stamps) != 0 || len(plan.Commands) != 1 {
		t.Fatalf("stamped plan = %+v", plan)
	}
}

func TestAutomaticRetryFromCommandRefusesLookalikes(t *testing.T) {
	t.Parallel()
	payload, _ := json.Marshal(domain.AutomaticRetry{SourceAttemptID: "a", Ordinal: 1, Budget: 2})
	genuine := domain.AdminCommand{ID: "auto-retry-a", Kind: domain.AdminCommandRetry, TargetID: "a",
		RequestedBy: domain.AutomaticRetryRequestedBy, Payload: payload}
	if _, ok := AutomaticRetryFromCommand(genuine); !ok {
		t.Fatal("genuine command refused")
	}
	for name, change := range map[string]func(*domain.AdminCommand){
		"operator principal": func(c *domain.AdminCommand) { c.RequestedBy = "operator" },
		"other ID":           func(c *domain.AdminCommand) { c.ID = "retry-1" },
		"other kind":         func(c *domain.AdminCommand) { c.Kind = domain.AdminCommandStart },
		"other target":       func(c *domain.AdminCommand) { c.TargetID, c.ID = "b", "auto-retry-b" },
		"no payload":         func(c *domain.AdminCommand) { c.Payload = nil },
	} {
		command := genuine
		change(&command)
		if _, ok := AutomaticRetryFromCommand(command); ok {
			t.Errorf("%s accepted", name)
		}
	}
}
