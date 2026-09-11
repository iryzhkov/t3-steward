package domain

import "time"

type SubmissionState string

const (
	SubmissionPending  SubmissionState = "pending"
	SubmissionAccepted SubmissionState = "accepted"
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
}
