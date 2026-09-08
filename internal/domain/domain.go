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
	UpdatedAt   time.Time  `json:"updatedAt"`
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
)

// Action is one instruction from the policy engine.
type Action struct {
	Kind   ActionKind
	Bucket BucketKey
	// Snapshot is the observation that triggered the action, or the last
	// accepted one for timer-driven actions.
	Snapshot QuotaSnapshot
	Reason   string
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
	SessionStatus       string
	LatestUserMessageAt *time.Time
	HasPendingApprovals bool
	HasPendingUserInput bool
	// BackgroundWork is "working" while native subagents or workflows run
	// after the turn settled, "monitoring" for watch loops, empty otherwise.
	BackgroundWork string
	ArchivedAt     *time.Time
	UpdatedAt      time.Time
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

// EpochFor derives the reset epoch identifier from a reset time.
func EpochFor(resetsAt *time.Time) string {
	if resetsAt == nil {
		return ""
	}
	return fmt.Sprintf("%d", resetsAt.Unix())
}
