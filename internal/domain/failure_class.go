package domain

import (
	"fmt"
	"strings"
	"time"
)

// FailureClass is the typed cause of a failed or cancelled attempt. It decides
// whether the coordinator may retry the task on its own: only infrastructure
// failures are retried automatically, and only within a declared budget.
type FailureClass string

const (
	// FailureInfrastructure is a failure of the machinery around the agent:
	// a worker, a provider session, a workspace or a collection. Running the
	// same task again can succeed without anyone changing anything.
	FailureInfrastructure FailureClass = "infrastructure"
	// FailureProtocol is a result that broke the task contract: a declared
	// output is missing, the agent ended its turn unfinished, or the result
	// could not be imported.
	FailureProtocol FailureClass = "protocol"
	// FailureCode is work that ran and was checked, and the check failed: a
	// declared verification command or gate command exited non-zero.
	FailureCode FailureClass = "code"
	// FailurePolicy is a result a rule or a declared limit refused: the secret
	// scanner, a size limit, a task timeout, a memory reservation or the
	// review completion gate.
	FailurePolicy FailureClass = "policy"
	// FailureCancelled is an attempt an operator, a run cancel or the
	// coordinator stopped.
	FailureCancelled FailureClass = "cancelled"
	// FailureUnknown is a failure whose reason matches no entry of the table.
	FailureUnknown FailureClass = "unknown"
)

// FailureReasonCode is the machine-readable reason inside a class. Codes are
// stable identifiers: a new failure reason gets a new code, an existing code is
// never reused for a different meaning.
type FailureReasonCode string

const (
	ReasonPreparationFailed       FailureReasonCode = "preparation-failed"
	ReasonThreadStartFailed       FailureReasonCode = "thread-start-failed"
	ReasonTurnStartRefused        FailureReasonCode = "turn-start-refused"
	ReasonProviderTurnFailed      FailureReasonCode = "provider-turn-failed"
	ReasonSessionNotReady         FailureReasonCode = "session-not-ready"
	ReasonQuotaPause              FailureReasonCode = "quota-pause"
	ReasonThreadIdentity          FailureReasonCode = "thread-identity"
	ReasonWorkspaceMissing        FailureReasonCode = "workspace-missing"
	ReasonPreservedResultMismatch FailureReasonCode = "preserved-result-mismatch"
	ReasonHostMemory              FailureReasonCode = "host-memory"
	ReasonExecutionUnknown        FailureReasonCode = "execution-unknown"
	ReasonExecutionAbandoned      FailureReasonCode = "execution-abandoned"

	ReasonMissingOutput       FailureReasonCode = "missing-output"
	ReasonMissingEvidence     FailureReasonCode = "missing-evidence"
	ReasonUnfinishedWork      FailureReasonCode = "unfinished-work"
	ReasonMissingSuccess      FailureReasonCode = "missing-success"
	ReasonReviewOutputInvalid FailureReasonCode = "review-output-invalid"
	ReasonResultRejected      FailureReasonCode = "result-rejected"
	ReasonPendingThreadWork   FailureReasonCode = "pending-thread-work"

	ReasonVerificationFailed FailureReasonCode = "verification-failed"
	ReasonGateFailed         FailureReasonCode = "gate-failed"

	ReasonSecretScan      FailureReasonCode = "secret-scan"
	ReasonResultSize      FailureReasonCode = "result-size"
	ReasonTaskTimeout     FailureReasonCode = "task-timeout"
	ReasonMemoryLimit     FailureReasonCode = "memory-limit"
	ReasonReviewGate      FailureReasonCode = "review-gate"
	ReasonCoordinatorStop FailureReasonCode = "coordinator-stop"

	ReasonCancelled    FailureReasonCode = "cancelled"
	ReasonUnrecognized FailureReasonCode = "unrecognized"
)

// FailureClassification is the class and reason code of one attempt's failure.
type FailureClassification struct {
	Class FailureClass      `json:"class"`
	Code  FailureReasonCode `json:"code"`
}

// String renders the classification the way task result and campaign show
// print it, "class/code".
func (c FailureClassification) String() string {
	if c.Class == "" {
		return ""
	}
	return string(c.Class) + "/" + string(c.Code)
}

// FailureRule is one row of the classification table: a failure whose reason
// contains Match is of Class with reason Code. Description says what the
// failure is, for the documentation the table is rendered into.
type FailureRule struct {
	Match       string
	Class       FailureClass
	Code        FailureReasonCode
	Description string
}

// FailureClassificationTable maps the failure reasons Steward records today to
// their classes. It is searched in order and the first row whose Match occurs
// in the reason decides. The order is the safety order: a reason that joins
// several failures ("verification command failed ...; missing declared output
// ...") takes the class least willing to retry, so policy rows come first,
// then code, then protocol, and infrastructure last. A review output row sits
// before the bare "verification failed" row because its reason contains that
// phrase. A reason no row matches
// is FailureUnknown, which is never retried.
//
// docs/failure-classification.md renders this table; a test keeps the two in
// step.
var FailureClassificationTable = []FailureRule{
	{"permanent collection secret failure: ", FailurePolicy, ReasonSecretScan, "the result secret scanner refused the collected result"},
	{"withheld by secret scan", FailurePolicy, ReasonSecretScan, "the result secret scanner withheld a failed result"},
	{"permanent collection size failure: ", FailurePolicy, ReasonResultSize, "a collected artifact exceeded the transfer size limit"},
	{"task timeout expired", FailurePolicy, ReasonTaskTimeout, "the task's declared timeout expired"},
	{"MB memory reservation", FailurePolicy, ReasonMemoryLimit, "a contained run exceeded its declared memory reservation"},
	{"review gate ", FailurePolicy, ReasonReviewGate, "the review completion gate refused unreviewed or changed work"},
	{"stopped by the coordinator before dispatch", FailureCancelled, ReasonCoordinatorStop, "the coordinator stopped the attempt before it was dispatched"},

	{"verification command failed (", FailureCode, ReasonVerificationFailed, "a declared verification command exited non-zero"},
	{"gate command failed (", FailureCode, ReasonGateFailed, "a declared gate command exited non-zero"},
	{"review_output verification failed: ", FailureProtocol, ReasonReviewOutputInvalid, "the declared review output is missing or malformed"},
	{"verification failed", FailureCode, ReasonVerificationFailed, "verification failed without a recorded command"},

	{"missing declared output: ", FailureProtocol, ReasonMissingOutput, "the turn ended without writing a declared output"},
	{"missing verification evidence: ", FailureProtocol, ReasonMissingEvidence, "the result lacks verification evidence"},
	{"missing gate evidence", FailureProtocol, ReasonMissingEvidence, "the result lacks gate evidence"},
	{"agent reported unfinished work: ", FailureProtocol, ReasonUnfinishedWork, "the agent ended its turn with continue or needs-input"},
	{"result import rejected", FailureProtocol, ReasonResultRejected, "the result violates the import contract"},
	{"thread still has pending input, approval, or background work", FailureProtocol, ReasonPendingThreadWork, "the turn ended with input, an approval or background work pending"},
	{"missing explicit success", FailureProtocol, ReasonMissingSuccess, "the result carries no explicit success"},

	{"preparation failed", FailureInfrastructure, ReasonPreparationFailed, "the worker could not prepare the workspace"},
	{"preparation returned an empty workspace", FailureInfrastructure, ReasonPreparationFailed, "the worker could not prepare the workspace"},
	{"T3 thread creation failed: ", FailureInfrastructure, ReasonThreadStartFailed, "the worker could not create the T3 thread"},
	{"T3 thread never started: ", FailureInfrastructure, ReasonThreadStartFailed, "the T3 thread never started"},
	{"T3 refused to start the provider turn", FailureInfrastructure, ReasonTurnStartRefused, "T3 or the provider refused to start the turn"},
	{"provider turn did not complete successfully", FailureInfrastructure, ReasonProviderTurnFailed, "the provider turn ended in an error"},
	{"paused by quota watchdog: ", FailureInfrastructure, ReasonQuotaPause, "a quota pause ended the turn"},
	{"provider session is not ready without an active turn or error", FailureInfrastructure, ReasonSessionNotReady, "the provider session was left in an unusable state"},
	{"thread completion identity is missing or mismatched", FailureInfrastructure, ReasonThreadIdentity, "T3 reported a different thread than the attempt's"},
	{"provider completion timestamps are missing or invalid", FailureInfrastructure, ReasonThreadIdentity, "T3 reported an incomplete turn record"},
	{"workspace is missing; outputs cannot be collected", FailureInfrastructure, ReasonWorkspaceMissing, "the workspace vanished before collection"},
	{"preserved result digest mismatch", FailureInfrastructure, ReasonPreservedResultMismatch, "the workspace changed between the end of the turn and a collection retry"},
	{"contained run was killed for exceeding available memory", FailureInfrastructure, ReasonHostMemory, "the host ran out of memory"},
	{"unknown execution resolved as failed", FailureInfrastructure, ReasonExecutionUnknown, "an operator resolved an execution of unknown state as failed"},
	{"was parked on a task-bound wait; the execution cannot be resumed", FailureInfrastructure, ReasonExecutionAbandoned, "the assignment of a parked attempt settled"},
	{"no longer owns parked attempt", FailureInfrastructure, ReasonExecutionAbandoned, "the assignment of a parked attempt was replaced"},
}

// ClassifyFailure classifies a failure reason as recorded on an attempt. It
// reads only the reason text, so it is the same function on the coordinator,
// in the CLI and in tests.
func ClassifyFailure(reason string) FailureClassification {
	for _, rule := range FailureClassificationTable {
		if strings.Contains(reason, rule.Match) {
			return FailureClassification{Class: rule.Class, Code: rule.Code}
		}
	}
	return FailureClassification{Class: FailureUnknown, Code: ReasonUnrecognized}
}

// ClassifyAttemptFailure returns the classification of a failed or cancelled
// attempt, and false for any other attempt. A classification the coordinator
// recorded on the attempt wins; an attempt recorded before classification
// existed is classified from its reason.
func ClassifyAttemptFailure(attempt Attempt) (FailureClassification, bool) {
	switch attempt.Progress {
	case ProgressFailed:
	case ProgressCancelled:
		return FailureClassification{Class: FailureCancelled, Code: ReasonCancelled}, true
	default:
		return FailureClassification{}, false
	}
	if attempt.FailureClass != "" {
		return FailureClassification{Class: attempt.FailureClass, Code: attempt.FailureReason}, true
	}
	return ClassifyFailure(attempt.Failure), true
}

// Retryable reports whether the class may be retried automatically. Only
// infrastructure failures are: a code, protocol or policy failure would fail
// the same way again, and an unknown one is not known to be safe to repeat.
func (c FailureClass) Retryable() bool { return c == FailureInfrastructure }

const (
	// DefaultInfrastructureRetries is the automatic retry budget of a task
	// whose workflow declares none.
	DefaultInfrastructureRetries = 2
	// MaxInfrastructureRetries is the largest budget a manifest may declare.
	MaxInfrastructureRetries = 5
	// DefaultCoordinatorMaxInfrastructureRetries is the coordinator's ceiling
	// when its configuration names none.
	DefaultCoordinatorMaxInfrastructureRetries = 3
	// DefaultRetryBackoff is the delay before the first automatic retry.
	DefaultRetryBackoff = 2 * time.Minute
	// MaxRetryBackoff caps both a declared backoff and the doubled delay of
	// later retries.
	MaxRetryBackoff = time.Hour
)

// TaskRetryPolicy is a task's declared automatic retry budget. A nil policy
// on a task means the defaults.
type TaskRetryPolicy struct {
	// Infrastructure is how many times an infrastructure-class failure of
	// the task is retried automatically within one run. Zero turns automatic
	// retries off for the task.
	Infrastructure int `json:"infrastructure"`
	// Backoff is the delay before the first automatic retry; each later one
	// waits twice as long as the one before, up to MaxRetryBackoff.
	Backoff time.Duration `json:"backoff,omitempty"`
}

// Validate refuses a budget outside 0..MaxInfrastructureRetries and a
// backoff outside 0..MaxRetryBackoff.
func (p TaskRetryPolicy) Validate() error {
	if p.Infrastructure < 0 || p.Infrastructure > MaxInfrastructureRetries {
		return fmt.Errorf("retry.infrastructure must be between 0 and %d, got %d", MaxInfrastructureRetries, p.Infrastructure)
	}
	if p.Backoff < 0 || p.Backoff > MaxRetryBackoff {
		return fmt.Errorf("retry.backoff must be between 0 and %s, got %s", MaxRetryBackoff, p.Backoff)
	}
	return nil
}

// EffectiveRetryPolicy is the budget and first delay that apply to a task:
// the task's declared policy or the defaults, with the budget capped by the
// coordinator's ceiling.
func EffectiveRetryPolicy(task Task, coordinatorMax int) TaskRetryPolicy {
	policy := TaskRetryPolicy{Infrastructure: DefaultInfrastructureRetries, Backoff: DefaultRetryBackoff}
	if task.Retry != nil {
		policy = *task.Retry
		if policy.Backoff == 0 {
			policy.Backoff = DefaultRetryBackoff
		}
	}
	if coordinatorMax < 0 {
		coordinatorMax = 0
	}
	if policy.Infrastructure > coordinatorMax {
		policy.Infrastructure = coordinatorMax
	}
	return policy
}

// RetryDelay is the wait before automatic retry number ordinal (1-based):
// the policy's backoff doubled for every earlier automatic retry, capped at
// MaxRetryBackoff.
func (p TaskRetryPolicy) RetryDelay(ordinal int) time.Duration {
	delay := p.Backoff
	for i := 1; i < ordinal && delay < MaxRetryBackoff; i++ {
		delay *= 2
	}
	if delay > MaxRetryBackoff {
		delay = MaxRetryBackoff
	}
	return delay
}

// AutomaticRetry is the receipt an automatically created retry attempt
// carries: which attempt failed, how it was classified, which retry of the
// budget this is, and when it may start.
type AutomaticRetry struct {
	SourceAttemptID string            `json:"sourceAttemptId"`
	Class           FailureClass      `json:"class"`
	Code            FailureReasonCode `json:"code"`
	Ordinal         int               `json:"ordinal"`
	Budget          int               `json:"budget"`
	NotBefore       time.Time         `json:"notBefore"`
	CommandID       string            `json:"commandId"`
}

// AutomaticRetryRequestedBy is the principal recorded on the admin commands
// the coordinator submits for automatic retries.
const AutomaticRetryRequestedBy = "steward-coordinator/automatic-retry"

// AutomaticRetryCommandID is the stable admin command ID of the automatic
// retry of one failed attempt. It makes the retry idempotent: the coordinator
// submits it at most once per failed attempt, however many boundaries see the
// failure.
func AutomaticRetryCommandID(attemptID string) string { return "auto-retry-" + attemptID }
