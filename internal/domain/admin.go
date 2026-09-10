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
)

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
