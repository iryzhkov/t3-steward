package domain

import (
	"errors"
	"strings"
	"time"
)

type AttentionKind string

const (
	AttentionApproval  AttentionKind = "approval"
	AttentionDirection AttentionKind = "direction"
)

type AttentionRequest struct {
	Kind         AttentionKind `json:"kind"`
	Prompt       string        `json:"prompt"`
	AssignmentID string        `json:"assignmentId"`
}

func (r AttentionRequest) Validate() error {
	if r.Kind != AttentionApproval && r.Kind != AttentionDirection {
		return errors.New("attention request kind must be approval or direction")
	}
	if strings.TrimSpace(r.Prompt) == "" || r.Prompt != strings.TrimSpace(r.Prompt) || len(r.Prompt) > 4000 {
		return errors.New("attention request prompt must be trimmed, non-empty and at most 4000 bytes")
	}
	if strings.TrimSpace(r.AssignmentID) == "" || r.AssignmentID != strings.TrimSpace(r.AssignmentID) {
		return errors.New("attention request needs the exact assignment ID")
	}
	return nil
}

type AttentionDecisionKind string

const (
	AttentionApprove AttentionDecisionKind = "approve"
	AttentionResume  AttentionDecisionKind = "resume"
	AttentionHold    AttentionDecisionKind = "hold"
	AttentionStop    AttentionDecisionKind = "stop"
	AttentionChange  AttentionDecisionKind = "change"
)

type AttentionDecision struct {
	ID                 string                `json:"id"`
	WaitID             string                `json:"waitId"`
	RequestID          string                `json:"requestId"`
	WorkflowRunID      string                `json:"workflowRunId"`
	TaskID             string                `json:"taskId"`
	AttemptID          string                `json:"attemptId"`
	AssignmentID       string                `json:"assignmentId"`
	ThreadID           string                `json:"threadId"`
	RegisteredRevision int64                 `json:"registeredRevision"`
	Kind               AttentionDecisionKind `json:"kind"`
	Reason             string                `json:"reason"`
	Change             string                `json:"change,omitempty"`
}

func (d AttentionDecision) Validate() error {
	if strings.TrimSpace(d.ID) == "" || len(d.ID) > 128 || d.ID != strings.TrimSpace(d.ID) {
		return errors.New("attention decision needs a trimmed ID of at most 128 bytes")
	}
	if d.WaitID == "" || d.RequestID == "" || d.WorkflowRunID == "" || d.TaskID == "" ||
		d.AttemptID == "" || d.AssignmentID == "" || d.ThreadID == "" || d.RegisteredRevision < 1 {
		return errors.New("attention decision needs the exact wait, request, run, task, attempt, assignment, thread and registered revision")
	}
	switch d.Kind {
	case AttentionApprove, AttentionResume, AttentionHold, AttentionStop, AttentionChange:
	default:
		return errors.New("attention decision must be approve, resume, hold, stop or change")
	}
	if strings.TrimSpace(d.Reason) == "" || d.Reason != strings.TrimSpace(d.Reason) || len(d.Reason) > 4000 {
		return errors.New("attention decision reason must be trimmed, non-empty and at most 4000 bytes")
	}
	if d.Kind == AttentionChange && strings.TrimSpace(d.Change) == "" {
		return errors.New("a change decision needs the proposed change")
	}
	if d.Kind != AttentionChange && d.Change != "" {
		return errors.New("only a change decision may carry a proposed change")
	}
	return nil
}

type AttentionReceiptState string

const (
	AttentionReceived  AttentionReceiptState = "received"
	AttentionApplied   AttentionReceiptState = "applied"
	AttentionRejected  AttentionReceiptState = "rejected"
	AttentionDelivered AttentionReceiptState = "delivered"
)

type AttentionReceipt struct {
	Decision    AttentionDecision     `json:"decision"`
	RequestedBy string                `json:"requestedBy"`
	State       AttentionReceiptState `json:"state"`
	Failure     string                `json:"failure,omitempty"`
	ReceivedAt  time.Time             `json:"receivedAt"`
	AppliedAt   *time.Time            `json:"appliedAt,omitempty"`
}
