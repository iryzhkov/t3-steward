// Package domain holds the provider-neutral types shared by the quota sources,
// the policy engine, the T3 control adapter and the state store.
//
// A quota belongs to a provider account or to a model-specific bucket, never to
// one thread. Every bucket is identified by a BucketKey and observed through a
// sequence of QuotaSnapshots.
package domain

import (
	"fmt"
	"strings"
	"time"
)

// Window names used by the normalizers. Providers report their own names; the
// watchdog keeps them verbatim so that reset handling does not depend on
// guessing a duration from the name.
const (
	WindowPrimary   = "primary"
	WindowSecondary = "secondary"
	WindowSpending  = "spending"
)

// BucketKey identifies one quota window of one limit of one provider account.
type BucketKey struct {
	// ProviderInstanceID is T3's provider instance id (for example "codex" or
	// "claudeAgent").
	ProviderInstanceID string `json:"providerInstanceId"`
	// AccountID is the provider account when the provider reports one. Empty
	// means "the only account this provider instance is signed in to".
	AccountID string `json:"accountId"`
	// LimitID is the provider's stable limit identity. When a provider has no
	// id the normalizer falls back to the limit name.
	LimitID string `json:"limitId"`
	// Window is the window name within the limit: "primary", "secondary",
	// "spending" for Codex; "five_hour", "seven_day", ... for Claude.
	Window string `json:"window"`
}

// String renders the key in a stable, log-friendly form.
func (k BucketKey) String() string {
	parts := []string{k.ProviderInstanceID}
	if k.AccountID != "" {
		parts = append(parts, k.AccountID)
	}
	parts = append(parts, k.LimitID, k.Window)
	return strings.Join(parts, "/")
}

// ParseBucketKey inverts String. Unknown shapes yield a key with only the
// provider instance set.
func ParseBucketKey(s string) BucketKey {
	parts := strings.Split(s, "/")
	switch len(parts) {
	case 3:
		return BucketKey{ProviderInstanceID: parts[0], LimitID: parts[1], Window: parts[2]}
	case 4:
		return BucketKey{ProviderInstanceID: parts[0], AccountID: parts[1], LimitID: parts[2], Window: parts[3]}
	default:
		return BucketKey{ProviderInstanceID: s}
	}
}

// QuotaSnapshot is one observation of one bucket.
type QuotaSnapshot struct {
	Key BucketKey `json:"key"`
	// LimitName is a human-readable limit name for messages.
	LimitName string `json:"limitName"`
	// UsedPercent is 0..100. Providers that report remaining percent are
	// converted with 100 - remaining.
	UsedPercent float64 `json:"usedPercent"`
	// WindowDuration is the reported window length, zero when unknown.
	WindowDuration time.Duration `json:"windowDuration"`
	// ResetsAt is the reported reset time, nil when the provider gave none.
	ResetsAt *time.Time `json:"resetsAt"`
	// ModelSelector is empty for account-wide limits. Otherwise it is a
	// lower-case substring matched against the thread's selected model id
	// (for example "opus" or "sonnet").
	ModelSelector string `json:"modelSelector"`
	// ObservedAt is when the provider produced the observation.
	ObservedAt time.Time `json:"observedAt"`
	// SourceEventID deduplicates the same event arriving through two sources.
	SourceEventID string `json:"sourceEventId"`
	// ThreadID is the T3 thread whose session reported the event, when known.
	// It is informational only: the quota belongs to the account.
	ThreadID string `json:"threadId,omitempty"`
}

// IsExpired reports whether the reported reset time has already passed at
// now, in which case the snapshot cannot describe the current window.
func (s QuotaSnapshot) IsExpired(now time.Time) bool {
	return s.ResetsAt != nil && !s.ResetsAt.After(now)
}

// Phase is the position of a bucket in the policy state machine.
type Phase string

const (
	PhaseNormal   Phase = "normal"
	PhaseWarned   Phase = "warned"
	PhaseDraining Phase = "draining"
	PhaseStopped  Phase = "stopped"
)

// Rank orders phases by severity so that escalation is monotonic within one
// reset epoch.
func (p Phase) Rank() int {
	switch p {
	case PhaseWarned:
		return 1
	case PhaseDraining:
		return 2
	case PhaseStopped:
		return 3
	default:
		return 0
	}
}

// BucketState is the persisted state machine of one bucket.
type BucketState struct {
	Key   BucketKey `json:"key"`
	Phase Phase     `json:"phase"`
	// Epoch identifies the reset window the phase belongs to. It is derived
	// from ResetsAt; an empty epoch means the provider reported no reset time.
	Epoch string `json:"epoch"`
	// WindowDuration preserves the reported length for timer-driven policy checks.
	WindowDuration time.Duration `json:"windowDuration,omitempty"`
	// LimitName is the last human-readable name seen for the bucket.
	LimitName     string     `json:"limitName"`
	ModelSelector string     `json:"modelSelector"`
	UsedPercent   float64    `json:"usedPercent"`
	ResetsAt      *time.Time `json:"resetsAt"`
	ObservedAt    time.Time  `json:"observedAt"`
	LastEventID   string     `json:"lastEventId"`
	// DrainDeadline is set when the bucket enters the draining phase. When it
	// passes without a stop, the engine stops affected threads anyway.
	DrainDeadline *time.Time `json:"drainDeadline"`
	// RearmObservations counts consecutive observations below the rearm
	// threshold, used when the provider reports no reset time.
	RearmObservations int `json:"rearmObservations"`
	// Healthy is true when the last accepted observation was below the warn
	// threshold in a phase that allows resumption. It is what the resume
	// scheduler consults.
	Healthy bool `json:"healthy"`
	// RecoveredAt is when the bucket last rearmed.
	RecoveredAt *time.Time `json:"recoveredAt"`
	// StoppedAt is when the bucket entered the stopped phase of the current
	// window; nil in every other phase. A turn whose latest user message is
	// newer than it was started by the user knowingly and is not held.
	StoppedAt *time.Time `json:"stoppedAt,omitempty"`
	UpdatedAt time.Time  `json:"updatedAt"`
	// Recent holds the last readings of the current window, for the burn
	// rate.
	Recent []Reading `json:"recent,omitempty"`
	// RatePerMinute is the burn rate over the rate window, percent per
	// minute; zero when unknown or not rising.
	RatePerMinute float64 `json:"ratePerMinute"`
	// ExhaustsIn is the projected time until 100% at the current rate;
	// nil when no exhaustion is projected.
	ExhaustsIn *time.Duration `json:"exhaustsIn,omitempty"`
	// ETAStrikes counts consecutive readings whose projection asked for a
	// higher level than the percentage ladder; escalation on the
	// projection needs two, so a single burst does not fire it.
	ETAStrikes int `json:"etaStrikes,omitempty"`
	// AppliedThresholds is the percentage ladder the phase was last derived
	// under. The daemon compares it with the loaded ladder at start and
	// lowers a phase the new ladder would not produce; nil for a state
	// written before the field existed.
	AppliedThresholds *ThresholdSet `json:"appliedThresholds,omitempty"`
	// ProbedAt is when a paused owned attempt was last resumed as a probe
	// of this bucket's epoch while the phase was stopped and no thread on
	// the host could produce a reading; nil when none was. One probe per
	// epoch: a reading that follows rearms or re-stops the bucket honestly.
	ProbedAt *time.Time `json:"probedAt,omitempty"`
}

// ThresholdSet is the percentage ladder a phase is derived from: warn,
// drain and stop, in percent used.
type ThresholdSet struct {
	WarnPercent  float64 `json:"warnPercent"`
	DrainPercent float64 `json:"drainPercent"`
	StopPercent  float64 `json:"stopPercent"`
}

// String renders the ladder as "85/90/95".
func (t ThresholdSet) String() string {
	return fmt.Sprintf("%.0f/%.0f/%.0f", t.WarnPercent, t.DrainPercent, t.StopPercent)
}

// Reading is one usage reading kept for rate estimation.
type Reading struct {
	At   time.Time `json:"at"`
	Used float64   `json:"used"`
}

// ActionKind is what the policy engine asked the controller to do.
type ActionKind string

const (
	ActionWarn  ActionKind = "warn"
	ActionDrain ActionKind = "drain"
	ActionStop  ActionKind = "stop"
	ActionRearm ActionKind = "rearm"
	// ActionResume is produced by the resume scheduler, not the policy engine.
	ActionResume ActionKind = "resume"
	// NoticeUserResumed is not an action but a thread notice kind: the
	// thread's user resumed or started it by hand after the bucket's stop,
	// and the notice time is that user message. For the rest of the epoch
	// the watchdog neither stops nor drains the thread and warns it at most
	// once (S-18).
	NoticeUserResumed ActionKind = "user-resumed"
)

// Action is one instruction from the policy engine.
type Action struct {
	Kind   ActionKind
	Bucket BucketKey
	// Snapshot is the observation that triggered the action, or the last
	// accepted one for timer-driven actions.
	Snapshot QuotaSnapshot
	Reason   string
	// EpochUnchanged marks a rearm that lowers a phase inside the bucket's
	// current epoch, the load-time re-derivation, rather than opening a new
	// one. The epoch's thread notices (the user-resumed record, the warn and
	// drain notices) belong to that epoch and are kept; a rearm from a reset
	// clears them with the epoch.
	EpochUnchanged bool
}

// Decision is the output of one policy evaluation.
type Decision struct {
	State   BucketState
	Actions []Action
	// Ignored explains why a snapshot produced no state change (duplicate,
	// stale, expired). Empty when the snapshot was accepted.
	Ignored string
}

// Thread is the subset of a T3 thread the watchdog needs.
type Thread struct {
	ID                 string
	Title              string
	ProjectID          string
	ProviderInstanceID string
	Model              string
	// ModelSelection is the thread's model selection exactly as T3 reported
	// it, so a resume can reuse it without understanding its contents.
	ModelSelection  map[string]any
	RuntimeMode     string
	InteractionMode string
	Running         bool
	// TurnID is the id of the latest turn.
	TurnID string
	// TurnState is T3's latest-turn state: running, interrupted, completed,
	// error, or empty.
	TurnState string
	// SessionStatus is T3's provider session status when a session exists.
	SessionStatus             string
	LatestUserMessageAt       *time.Time
	HasPendingApprovals       bool
	HasPendingUserInput       bool
	HasActionableProposedPlan bool
	// BackgroundWork is "working" while native subagents or workflows run
	// after the turn settled, "monitoring" for watch loops, empty otherwise.
	BackgroundWork string
	ArchivedAt     *time.Time
	// SettledAt is when T3 moved the thread to its settled shelf, by hand
	// or by auto-settlement; nil while it is active.
	SettledAt *time.Time
	// SettledOverride is "settled" or "active" when the user pinned the
	// state, empty otherwise.
	SettledOverride string
	UpdatedAt       time.Time
}

// Settled reports whether the thread rests on T3's settled shelf or is
// archived: finished work, from T3's point of view.
func (t Thread) Settled() bool {
	if t.SettledOverride == "active" {
		return false
	}
	return t.ArchivedAt != nil || t.SettledAt != nil || t.SettledOverride == "settled"
}

// MatchesBucket reports whether a limit applies to the thread: the provider
// instance must match and, for model-specific limits, the selected model must
// contain the selector.
func (t Thread) MatchesBucket(key BucketKey, modelSelector string) bool {
	if t.ProviderInstanceID != key.ProviderInstanceID {
		return false
	}
	if modelSelector == "" {
		return true
	}
	return strings.Contains(strings.ToLower(t.Model), strings.ToLower(modelSelector))
}

// Warning is the message delivered to a coordinator thread.
type Warning struct {
	Kind ActionKind
	Text string
}

// ResumeStatus is the lifecycle of a resume intent.
type ResumeStatus string

const (
	ResumePending   ResumeStatus = "pending"
	ResumeEligible  ResumeStatus = "eligible"
	ResumeResuming  ResumeStatus = "resuming"
	ResumeResumed   ResumeStatus = "resumed"
	ResumeCancelled ResumeStatus = "cancelled"
	ResumeFailed    ResumeStatus = "failed"
)

// ResumeIntent records that the watchdog stopped a thread and may resume it.
type ResumeIntent struct {
	ThreadID           string       `json:"threadId"`
	ProviderInstanceID string       `json:"providerInstanceId"`
	Model              string       `json:"model"`
	StoppedByWatchdog  bool         `json:"stoppedByWatchdog"`
	StoppedAt          time.Time    `json:"stoppedAt"`
	StoppedTurnID      string       `json:"stoppedTurnId"`
	StopGeneration     string       `json:"stopGeneration"`
	CheckpointExpected bool         `json:"checkpointExpected"`
	Status             ResumeStatus `json:"status"`
	// Buckets lists every bucket that was unhealthy when the thread was
	// stopped. All of them, plus any other bucket applicable to the thread,
	// must be healthy before the thread resumes.
	Buckets   []BucketKey `json:"buckets"`
	Reason    string      `json:"reason,omitempty"`
	UpdatedAt time.Time   `json:"updatedAt"`
	// ResumedAt is set once the resume prompt was dispatched.
	ResumedAt *time.Time `json:"resumedAt,omitempty"`
}

// ActionRecord is one row of the audit log.
type ActionRecord struct {
	At       time.Time
	Kind     ActionKind
	Bucket   string
	ThreadID string
	DryRun   bool
	Detail   string
	Err      string
}

// LogPosition persists how far a provider log file has been read.
type LogPosition struct {
	Path   string
	Inode  uint64
	Offset int64
}

// Observation is one accepted quota reading kept for history and reports.
type Observation struct {
	Key         BucketKey  `json:"key"`
	ObservedAt  time.Time  `json:"observedAt"`
	UsedPercent float64    `json:"usedPercent"`
	ResetsAt    *time.Time `json:"resetsAt"`
	EventID     string     `json:"eventId"`
	// ThreadID and Model identify the session whose turn produced the
	// reading, for attributing quota consumption.
	ThreadID string `json:"threadId"`
	Model    string `json:"model"`
}

// ExecutionRole is the stable purpose of one provider session.
type ExecutionRole string

const (
	ExecutionRoleExecutor             ExecutionRole = "executor"
	ExecutionRoleRepairExecutor       ExecutionRole = "repair-executor"
	ExecutionRoleGateReviewer         ExecutionRole = "gate-reviewer"
	ExecutionRoleSupervisorActivation ExecutionRole = "supervisor-activation"

	// Compatibility names retain source compatibility while emitting stable role values.
	ExecutionRoleTask        = ExecutionRoleExecutor
	ExecutionRoleSupervision = ExecutionRoleSupervisorActivation
)

// UsageAttributionStatus says whether an authoritative V2 dispatch binding was
// present. Unknown sessions are explicit; callers must not infer identity from
// titles, prompts, paths, or thread ID shape.
type UsageAttributionStatus string

const (
	UsageAttributed   UsageAttributionStatus = "attributed"
	UsageUnattributed UsageAttributionStatus = "unattributed"
)

// UsageAttribution is the durable coordinator identity bound to a provider
// thread at dispatch. ActivationID is set for supervision execution; GateID is
// reserved for execution launched for a durable gate and is otherwise empty.
type UsageAttribution struct {
	Status          UsageAttributionStatus `json:"status"`
	WorkerID        string                 `json:"workerId,omitempty"`
	WorkflowRunID   string                 `json:"workflowRunId,omitempty"`
	TaskID          string                 `json:"taskId,omitempty"`
	AttemptID       string                 `json:"attemptId,omitempty"`
	AssignmentID    string                 `json:"assignmentId,omitempty"`
	AssignmentEpoch int64                  `json:"assignmentEpoch,omitempty"`
	ActivationID    string                 `json:"activationId,omitempty"`
	GateID          string                 `json:"gateId,omitempty"`
	Role            ExecutionRole          `json:"role,omitempty"`
}

// UsageExecutionSession is an authoritative dispatch binding expected to
// produce provider usage evidence. It contains identity only, never provider
// content, prompts, transcripts, or credentials.
type UsageExecutionSession struct {
	WorkerID           string           `json:"workerId"`
	ProviderInstanceID string           `json:"providerInstanceId"`
	ThreadID           string           `json:"threadId"`
	Attribution        UsageAttribution `json:"attribution"`
}

// UsageCoverage reports evidence deliberately excluded from a run-scoped result.
type UsageCoverage struct {
	State                  UsageCoverageState `json:"state"`
	Reasons                []string           `json:"reasons,omitempty"`
	Reason                 string             `json:"reason,omitempty"`
	ObservedFrom           *time.Time         `json:"observedFrom,omitempty"`
	ObservedThrough        *time.Time         `json:"observedThrough,omitempty"`
	RawSampleCount         int64              `json:"rawSampleCount"`
	NormalizedSampleCount  int64              `json:"normalizedSampleCount"`
	ExpectedSessionCount   int64              `json:"expectedSessionCount"`
	MissingLogSessionCount int64              `json:"missingLogSessionCount"`
	AttributedCount        int64              `json:"attributedCount"`
	UnattributedCount      int64              `json:"unattributedCount"`
	// UnscopedUnattributedCount is every sample without a dispatch binding in
	// the run's window, on any worker. It is context: most of it is work no
	// dispatch of the run could have produced, so it does not by itself make
	// the run's coverage partial.
	UnscopedUnattributedCount int64 `json:"unscopedUnattributedCount"`
	// RunWindowUnattributedCount is the part of the unscoped samples that could
	// be this run's own evidence: the same provider on a worker the run was
	// dispatched to, inside the run's window. It is what UnattributedCount
	// reports, and it keeps coverage partial.
	RunWindowUnattributedCount int64 `json:"runWindowUnattributedCount"`
	ExcludedOverlapCount       int64 `json:"excludedOverlapCount"`
	UnmatchedCallCount         int64 `json:"unmatchedCallCount"`
	DuplicateCount             int64 `json:"duplicateCount"`
	ResetCount                 int64 `json:"resetCount"`
	UnknownModelCount          int64 `json:"unknownModelCount"`
	MalformedCount             int64 `json:"malformedCount"`
	UnsupportedCount           int64 `json:"unsupportedCount"`
	DiagnosticCount            int64 `json:"diagnosticCount"`
	DiagnosticDroppedCount     int64 `json:"diagnosticDroppedCount"`
	MissingFieldCount          int64 `json:"missingFieldCount"`
	AmbiguousOverlapCount      int64 `json:"ambiguousOverlapCount"`
	CumulativeAmbiguityCount   int64 `json:"cumulativeAmbiguityCount"`
	LateCount                  int64 `json:"lateCount"`
	Truncated                  bool  `json:"truncated"`
}

type UsageReport struct {
	WorkflowRunID                     string                  `json:"workflowRunId,omitempty"`
	RunProgress                       ProgressState           `json:"runProgress,omitempty"`
	AcceptedOutcomeCount              int64                   `json:"acceptedOutcomeCount"`
	MeasuredCostPerAcceptedOutcomeUSD *float64                `json:"measuredCostPerAcceptedOutcomeUsd,omitempty"`
	Totals                            UsageTotals             `json:"totals"`
	ByTask                            []UsageAggregate        `json:"byTask,omitempty"`
	ByAttempt                         []UsageAggregate        `json:"byAttempt,omitempty"`
	ByRole                            []UsageAggregate        `json:"byRole,omitempty"`
	ByModel                           []UsageAggregate        `json:"byModel,omitempty"`
	ExpectedSessions                  []UsageExecutionSession `json:"expectedSessions,omitempty"`
	Samples                           []UsageSample           `json:"samples,omitempty"`
	NextCursor                        string                  `json:"nextCursor,omitempty"`
	Coverage                          UsageCoverage           `json:"coverage"`
}

// UsageSample is a token count reported by a provider for one API call or
// one turn, used to normalize quota consumption by work done.
type UsageSample struct {
	// WorkerID is assigned only at the authenticated worker-to-coordinator boundary.
	// Provider parsers and legacy rows leave it empty and therefore unattributed.
	WorkerID           string    `json:"workerId,omitempty"`
	ProviderInstanceID string    `json:"providerInstanceId"`
	ThreadID           string    `json:"threadId"`
	Model              string    `json:"model"`
	ObservedAt         time.Time `json:"observedAt"`
	SourceEventID      string    `json:"sourceEventId"`
	InputTokens        int64     `json:"inputTokens"`
	CacheWriteTokens   int64     `json:"cacheWriteTokens"`
	CacheReadTokens    int64     `json:"cacheReadTokens"`
	OutputTokens       int64     `json:"outputTokens"`
	// CostUSD is the provider's own cost figure when it reports one.
	CostUSD      float64 `json:"costUsd"`
	CostReported bool    `json:"costReported,omitempty"`
	// Kind is "call" for one API call, "turn" for a whole turn, or
	// "diagnostic" for bounded sanitized parse evidence carrying no content.
	Kind           string `json:"kind"`
	DiagnosticCode string `json:"diagnosticCode,omitempty"`
	// FieldPresence distinguishes an absent numeric field from a measured zero.
	FieldPresence UsageFieldPresence `json:"fieldPresence,omitempty"`
	// BoundaryID proves that a call belongs to a particular whole-turn summary.
	// It is empty when the provider's canonical event supplies no such identity.
	BoundaryID string `json:"boundaryId,omitempty"`
	// Incarnation and Sequence provide causal ordering for cumulative counters.
	// A reset is exact only when the provider supplies a new incarnation.
	Incarnation string `json:"incarnation,omitempty"`
	Sequence    int64  `json:"sequence,omitempty"`
	// CumulativeTokens is the provider's running total for the thread when
	// it reports one, used to drop repeated notifications of the same call.
	CumulativeTokens int64 `json:"cumulativeTokens,omitempty"`
	// Attribution is populated by durable storage queries, never by provider
	// parsing. Raw ingestion leaves it zero until the store performs the join.
	Attribution UsageAttribution `json:"attribution"`
}

// Sample kinds.
const (
	UsageKindCall       = "call"
	UsageKindTurn       = "turn"
	UsageKindDiagnostic = "diagnostic"
)

type UsageFieldPresence uint32

const (
	UsageFieldInput UsageFieldPresence = 1 << iota
	UsageFieldCacheWrite
	UsageFieldCacheRead
	UsageFieldOutput
)

const UsageFieldsAll = UsageFieldInput | UsageFieldCacheWrite | UsageFieldCacheRead | UsageFieldOutput

// FreshTokens are the tokens that are not cache reads: input, cache
// writes and output. Cache reads are reported separately because their
// weight in provider quotas is unknown.
func (u UsageSample) FreshTokens() int64 {
	return u.InputTokens + u.CacheWriteTokens + u.OutputTokens
}

// EpochFor derives the reset epoch identifier from a reset time.
func EpochFor(resetsAt *time.Time) string {
	if resetsAt == nil {
		return ""
	}
	return fmt.Sprintf("%d", resetsAt.Unix())
}
