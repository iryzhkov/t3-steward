package domain

import (
	"errors"
	"time"
)

// ErrSubmissionConflict reports that an idempotency key already holds different
// content. The same content will always conflict; only new content can succeed.
var ErrSubmissionConflict = errors.New("submission idempotency key already has different content")

type SubmissionState string

const (
	SubmissionPending  SubmissionState = "pending"
	SubmissionAccepted SubmissionState = "accepted"
	// SubmissionQuarantined marks content that can never be accepted. It is
	// recorded once, with its reason, so that a source which re-reads the same
	// content every cycle reports the conflict once instead of forever.
	SubmissionQuarantined SubmissionState = "quarantined"
)

// SubmissionRecord is the durable idempotency decision for one accepted client
// request. The request digest and result identities are immutable.
type SubmissionRecord struct {
	Key        string          `json:"key"`
	Digest     string          `json:"digest"`
	WorkflowID string          `json:"workflowId"`
	RunID      string          `json:"runId"`
	State      SubmissionState `json:"state"`
	CreatedAt  time.Time       `json:"createdAt"`
	AcceptedAt *time.Time      `json:"acceptedAt,omitempty"`
	// Reason explains a quarantine. It is empty for a pending or accepted
	// record, whose content is its own explanation.
	Reason string `json:"reason,omitempty"`
}
