package backlog

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// FailureStamp is the classification the coordinator records on one terminal
// attempt. Revision fences the write: a stamp computed from an older record
// is dropped and recomputed on the next boundary.
type FailureStamp struct {
	AttemptID      string
	Revision       int64
	Classification domain.FailureClassification
}

// AutomaticRetryPlan is what one coordinator boundary does about failed
// attempts: the classifications to record and the retry commands to submit.
type AutomaticRetryPlan struct {
	Stamps   []FailureStamp
	Commands []domain.AdminCommand
}

// automaticRetryDecision is the retry verdict for the latest attempt of one
// task in one run.
type automaticRetryDecision struct {
	attempt domain.Attempt
	policy  domain.TaskRetryPolicy
	retry   domain.AutomaticRetry
}

// PlanAutomaticRetries decides, from one coordinator snapshot, which failed
// attempts are classified and which are retried automatically.
//
// A task is retried when its latest attempt in the run failed with an
// infrastructure-class failure, its effective budget (the task's declared
// budget, or the default, capped by coordinatorMax) has retries left, the
// run is unsupervised and its sink is not final, and no automatic retry
// command was ever submitted for that attempt. The retry itself is an
// ordinary admin retry command, so it is audited, fenced on the attempt's
// revision and applied by the same code an operator's retry is: review
// retry admission, the closed-run refusal and later quota admission and
// verification all still apply. Its payload carries the AutomaticRetry
// receipt, whose NotBefore delays the new attempt by the backoff.
//
// A supervised run is left to its overseer, whose recovery owns retries
// there; two retry authorities over one run would race.
func PlanAutomaticRetries(records sqlite.CoordinatorRecords, coordinatorMax int, now time.Time) AutomaticRetryPlan {
	var plan AutomaticRetryPlan
	for _, attempt := range domain.DeclaredTaskAttempts(records.Attempts) {
		classification, ok := domain.ClassifyAttemptFailure(attempt)
		if !ok || attempt.FailureClass != "" {
			continue
		}
		plan.Stamps = append(plan.Stamps, FailureStamp{AttemptID: attempt.ID, Revision: attempt.Revision, Classification: classification})
	}
	commands := adminCommandsByID(records.AdminCommands)
	for _, decision := range automaticRetryDecisions(records, coordinatorMax) {
		id := decision.retry.CommandID
		if _, submitted := commands[id]; submitted {
			continue
		}
		if operatorRetryPending(records.AdminCommands, decision.attempt.ID) {
			continue
		}
		retry := decision.retry
		completed := decision.attempt.UpdatedAt
		if decision.attempt.CompletedAt != nil {
			completed = *decision.attempt.CompletedAt
		}
		retry.NotBefore = completed.UTC().Add(decision.policy.RetryDelay(retry.Ordinal))
		if retry.NotBefore.Before(now) {
			retry.NotBefore = now.UTC()
		}
		payload, err := json.Marshal(retry)
		if err != nil {
			continue
		}
		plan.Commands = append(plan.Commands, domain.AdminCommand{
			ID: id, Kind: domain.AdminCommandRetry, TargetType: domain.AdminTargetAttempt, TargetID: decision.attempt.ID,
			ExpectedRevision: decision.attempt.Revision,
			Reason: fmt.Sprintf("automatic retry %d of %d after %s failure (%s)",
				retry.Ordinal, retry.Budget, retry.Class, retry.Code),
			RequestedBy: domain.AutomaticRetryRequestedBy, Payload: payload,
			State: domain.AdminCommandPending, CreatedAt: now.UTC(),
		})
	}
	return plan
}

// AutomaticRetryPendingRuns names the runs that have a failed task the
// coordinator is about to retry: the retry is due but its command was not
// submitted yet, or is submitted and not applied. The projection leaves such
// a run alone, because settling its sink, or skipping the failed task's
// dependents, would decide the run before the retry could run. A retry
// command that was rejected releases the run, so it can never hold forever.
func AutomaticRetryPendingRuns(records sqlite.CoordinatorRecords, coordinatorMax int) map[string]bool {
	pending := map[string]bool{}
	commands := adminCommandsByID(records.AdminCommands)
	for _, decision := range automaticRetryDecisions(records, coordinatorMax) {
		if command, submitted := commands[decision.retry.CommandID]; submitted && command.State != domain.AdminCommandPending {
			continue
		}
		pending[decision.attempt.WorkflowRunID] = true
	}
	return pending
}

// automaticRetryDecisions lists, in a stable order, the latest attempts that
// qualify for an automatic retry, each with its receipt minus NotBefore.
func automaticRetryDecisions(records sqlite.CoordinatorRecords, coordinatorMax int) []automaticRetryDecision {
	if coordinatorMax <= 0 {
		return nil
	}
	runs := make(map[string]domain.WorkflowRun, len(records.WorkflowRuns))
	for _, run := range records.WorkflowRuns {
		runs[run.ID] = run
	}
	// A run's own graph, when it has one, defines its tasks, so a budget an
	// amendment changed is the one that applies.
	runTasks := map[string]map[string]domain.Task{}
	taskOf := func(run domain.WorkflowRun, id string) (domain.Task, bool) {
		if _, ok := runTasks[run.ID]; !ok {
			runTasks[run.ID] = map[string]domain.Task{}
			for _, task := range domain.TasksForRun(run, records.Tasks) {
				runTasks[run.ID][task.ID] = task
			}
		}
		task, ok := runTasks[run.ID][id]
		return task, ok
	}
	type key struct{ run, task string }
	latest := map[key]domain.Attempt{}
	automatic := map[key]int{}
	for _, attempt := range domain.DeclaredTaskAttempts(records.Attempts) {
		k := key{attempt.WorkflowRunID, attempt.TaskID}
		if current, ok := latest[k]; !ok || attempt.Number > current.Number {
			latest[k] = attempt
		}
		if attempt.AutomaticRetry != nil {
			automatic[k]++
		}
	}
	var decisions []automaticRetryDecision
	for k, attempt := range latest {
		if attempt.Progress != domain.ProgressFailed {
			continue
		}
		run, ok := runs[k.run]
		if !ok || run.Supervision != nil || run.Sink != nil && run.Sink.Progress.Terminal() {
			continue
		}
		task, ok := taskOf(run, k.task)
		if !ok {
			continue
		}
		classification, _ := domain.ClassifyAttemptFailure(attempt)
		if !classification.Class.Retryable() {
			continue
		}
		policy := domain.EffectiveRetryPolicy(task, coordinatorMax)
		ordinal := automatic[k] + 1
		if ordinal > policy.Infrastructure {
			continue
		}
		decisions = append(decisions, automaticRetryDecision{attempt: attempt, policy: policy, retry: domain.AutomaticRetry{
			SourceAttemptID: attempt.ID, Class: classification.Class, Code: classification.Code,
			Ordinal: ordinal, Budget: policy.Infrastructure, CommandID: domain.AutomaticRetryCommandID(attempt.ID),
		}})
	}
	sort.Slice(decisions, func(i, j int) bool { return decisions[i].attempt.ID < decisions[j].attempt.ID })
	return decisions
}

func adminCommandsByID(commands []domain.AdminCommand) map[string]domain.AdminCommand {
	byID := make(map[string]domain.AdminCommand, len(commands))
	for _, command := range commands {
		byID[command.ID] = command
	}
	return byID
}

// operatorRetryPending reports whether an operator already asked to retry
// the attempt; the coordinator does not add a second retry of its own.
func operatorRetryPending(commands []domain.AdminCommand, attemptID string) bool {
	for _, command := range commands {
		if command.Kind == domain.AdminCommandRetry && command.TargetType == domain.AdminTargetAttempt &&
			command.TargetID == attemptID && command.State == domain.AdminCommandPending {
			return true
		}
	}
	return false
}

// AutomaticRetryFromCommand returns the receipt an automatic retry command
// carries, and false for any other command. Only a command with the stable
// automatic ID of its target and the coordinator's principal counts, so an
// operator's retry can never be mistaken for one or spend the budget.
func AutomaticRetryFromCommand(command domain.AdminCommand) (domain.AutomaticRetry, bool) {
	if command.Kind != domain.AdminCommandRetry || command.RequestedBy != domain.AutomaticRetryRequestedBy ||
		command.ID != domain.AutomaticRetryCommandID(command.TargetID) || len(command.Payload) == 0 {
		return domain.AutomaticRetry{}, false
	}
	var retry domain.AutomaticRetry
	if err := json.Unmarshal(command.Payload, &retry); err != nil || retry.SourceAttemptID != command.TargetID || retry.Ordinal < 1 {
		return domain.AutomaticRetry{}, false
	}
	return retry, true
}
