package domain

import (
	"encoding/json"
	"time"
)

type AdminTargetType string

const (
	AdminTargetAttempt     AdminTargetType = "attempt"
	AdminTargetWorkflowRun AdminTargetType = "workflow-run"
	AdminTargetSchedule    AdminTargetType = "schedule"
	AdminTargetAssignment  AdminTargetType = "assignment"

	AuditTargetSubmission    AdminTargetType = "submission"
	AuditTargetQuotaPool     AdminTargetType = "quota-pool"
	AuditTargetTrigger       AdminTargetType = "trigger"
	AuditTargetCoordinator   AdminTargetType = "coordinator"
	AuditTargetWorker        AdminTargetType = "worker"
	AuditTargetWorkerCommand AdminTargetType = "worker-command"
	AuditTargetArtifact      AdminTargetType = "artifact"
	AuditTargetThrottle      AdminTargetType = "throttle"
)

type UnknownRecoveryOutcome string

const (
	UnknownRecoveryStopped UnknownRecoveryOutcome = "stopped"
	UnknownRecoveryFailed  UnknownRecoveryOutcome = "failed"
)

// UnknownAssignmentRecovery is evidence-bound operator intent to resolve one
// ambiguous assignment. Evidence bytes stay outside coordinator state; their
// stable identifier and SHA-256 bind the reviewed observation without leaking
// credentials or arbitrary report text into audit records.
type UnknownAssignmentRecovery struct {
	ID                      string                 `json:"id"`
	AssignmentID            string                 `json:"assignmentId"`
	CoordinatorEpoch        int64                  `json:"coordinatorEpoch"`
	ExpectedAssignmentEpoch int64                  `json:"expectedAssignmentEpoch"`
	ExpectedAttemptRevision int64                  `json:"expectedAttemptRevision"`
	Outcome                 UnknownRecoveryOutcome `json:"outcome"`
	EvidenceID              string                 `json:"evidenceId"`
	EvidenceSHA256          string                 `json:"evidenceSha256"`
	Actor                   string                 `json:"actor"`
	Reason                  string                 `json:"reason"`
	RecoveredAt             time.Time              `json:"recoveredAt"`
}

type UnknownAssignmentRecoveryDecision struct {
	Recovery   UnknownAssignmentRecovery `json:"recovery"`
	Assignment Assignment                `json:"assignment"`
	Attempt    Attempt                   `json:"attempt"`
	Event      AuditEvent                `json:"event"`
	Replay     bool                      `json:"replay"`
}

type AdminCommandKind string

const (
	AdminCommandStart       AdminCommandKind = "start"
	AdminCommandDelay       AdminCommandKind = "delay"
	AdminCommandPause       AdminCommandKind = "pause"
	AdminCommandResume      AdminCommandKind = "resume"
	AdminCommandCancel      AdminCommandKind = "cancel"
	AdminCommandRetry       AdminCommandKind = "retry"
	AdminCommandSkip        AdminCommandKind = "skip"
	AdminCommandScheduleRun AdminCommandKind = "schedule-run"
	AdminCommandDelayNext   AdminCommandKind = "delay-next"
	AdminCommandEnable      AdminCommandKind = "enable"
	AdminCommandDisable     AdminCommandKind = "disable"
)

type AuditEvent struct {
	ID            string          `json:"id"`
	Sequence      int64           `json:"sequence"`
	Kind          string          `json:"kind"`
	WorkflowRunID string          `json:"workflowRunId,omitempty"`
	TaskID        string          `json:"taskId,omitempty"`
	AttemptID     string          `json:"attemptId,omitempty"`
	TargetType    AdminTargetType `json:"targetType,omitempty"`
	TargetID      string          `json:"targetId,omitempty"`
	Actor         string          `json:"actor,omitempty"`
	Reason        string          `json:"reason,omitempty"`
	Detail        json.RawMessage `json:"detail,omitempty"`
	CreatedAt     time.Time       `json:"createdAt"`
}

type AdminTargetSnapshot struct {
	Type     AdminTargetType `json:"type"`
	ID       string          `json:"id"`
	Revision int64           `json:"revision"`
	Record   json.RawMessage `json:"record"`
}

type AdminCommandDecision struct {
	Command       AdminCommand         `json:"command"`
	Event         AuditEvent           `json:"event"`
	CurrentTarget *AdminTargetSnapshot `json:"currentTarget,omitempty"`
}

type AdminCommandOutcome struct {
	CommandID     string            `json:"commandId"`
	ExpectedState AdminCommandState `json:"expectedState"`
	State         AdminCommandState `json:"state"`
	Failure       string            `json:"failure,omitempty"`
	AppliedAt     time.Time         `json:"appliedAt"`
}

// AdminCommandApplication is the coordinator's deterministic, revision-fenced
// state transition for one pending command. The store commits the target changes,
// terminal command outcome, and audit event in one transaction.
type AdminCommandApplication struct {
	CommandID              string                     `json:"commandId"`
	ExpectedCommandState   AdminCommandState          `json:"expectedCommandState"`
	ExpectedTargetRevision int64                      `json:"expectedTargetRevision"`
	SafetyFingerprint      string                     `json:"-"`
	SafetyValidUntil       *time.Time                 `json:"-"`
	PauseIntent            *ThrottleAttemptTransition `json:"-"`
	Attempt                *Attempt                   `json:"attempt,omitempty"`
	RelatedAttempts        []Attempt                  `json:"relatedAttempts,omitempty"`
	NewAttempt             *Attempt                   `json:"newAttempt,omitempty"`
	WorkflowRun            *WorkflowRun               `json:"workflowRun,omitempty"`
	Schedule               *Schedule                  `json:"schedule,omitempty"`
	ScheduleTrigger        *ScheduleTriggerRequest    `json:"scheduleTrigger,omitempty"`
	State                  AdminCommandState          `json:"state"`
	Failure                string                     `json:"failure,omitempty"`
	AppliedAt              time.Time                  `json:"appliedAt"`
}
