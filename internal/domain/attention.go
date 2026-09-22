package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	Kind             AttentionKind `json:"kind"`
	Prompt           string        `json:"prompt"`
	AssignmentID     string        `json:"assignmentId"`
	AssignmentEpoch  int64         `json:"assignmentEpoch,omitempty"`
	WorkerID         string        `json:"workerId,omitempty"`
	ContentDigest    string        `json:"contentDigest,omitempty"`
	DecisionDeadline time.Time     `json:"decisionDeadline,omitempty"`
}

// AttentionRequestContentDigest is the canonical digest of the operator-visible
// question. Coordinator-owned execution fences are carried beside it and may
// change only by creating a new wait.
func AttentionRequestContentDigest(kind AttentionKind, prompt string) string {
	raw, _ := json.Marshal(struct {
		Kind   AttentionKind `json:"kind"`
		Prompt string        `json:"prompt"`
	}{Kind: kind, Prompt: prompt})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
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
	enriched := r.AssignmentEpoch != 0 || r.WorkerID != "" || r.ContentDigest != "" || !r.DecisionDeadline.IsZero()
	if enriched {
		if r.AssignmentEpoch < 1 || strings.TrimSpace(r.WorkerID) == "" || r.WorkerID != strings.TrimSpace(r.WorkerID) ||
			r.ContentDigest != AttentionRequestContentDigest(r.Kind, r.Prompt) || r.DecisionDeadline.IsZero() {
			return errors.New("attention request has incomplete or invalid coordinator fences")
		}
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
	AssignmentEpoch    int64                 `json:"assignmentEpoch"`
	WorkerID           string                `json:"workerId"`
	ThreadID           string                `json:"threadId"`
	RegisteredRevision int64                 `json:"registeredRevision"`
	ContentDigest      string                `json:"contentDigest"`
	DecisionDeadline   time.Time             `json:"decisionDeadline"`
	Kind               AttentionDecisionKind `json:"kind"`
	Reason             string                `json:"reason"`
	Change             string                `json:"change,omitempty"`
}

func (d AttentionDecision) Validate() error {
	if strings.TrimSpace(d.ID) == "" || len(d.ID) > 128 || d.ID != strings.TrimSpace(d.ID) {
		return errors.New("attention decision needs a trimmed ID of at most 128 bytes")
	}
	if d.WaitID == "" || d.RequestID == "" || d.WorkflowRunID == "" || d.TaskID == "" ||
		d.AttemptID == "" || d.AssignmentID == "" || d.AssignmentEpoch < 1 || d.WorkerID == "" ||
		d.ThreadID == "" || d.RegisteredRevision < 1 || d.ContentDigest == "" || d.DecisionDeadline.IsZero() {
		return errors.New("attention decision needs the exact wait, request, run, task, attempt, assignment, assignment epoch, worker, thread, registered revision, content digest and deadline")
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
	AttentionObserved  AttentionReceiptState = "observed"
)

type AttentionReceipt struct {
	Decision      AttentionDecision     `json:"decision"`
	RequestedBy   string                `json:"requestedBy"`
	State         AttentionReceiptState `json:"state"`
	Failure       string                `json:"failure,omitempty"`
	ReceivedAt    time.Time             `json:"receivedAt"`
	AppliedAt     *time.Time            `json:"appliedAt,omitempty"`
	DeliveredAt   *time.Time            `json:"deliveredAt,omitempty"`
	ObservedAt    *time.Time            `json:"observedAt,omitempty"`
	CommandID     string                `json:"commandId,omitempty"`
	CommandDigest string                `json:"commandDigest,omitempty"`
}
