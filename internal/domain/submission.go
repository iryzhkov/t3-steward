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

// QuarantineRelease is the outcome of an operator deliberately clearing an
// intake quarantine.
//
// It exists because the automatic release is bound to the file's content: a
// quarantine caused by configuration, such as a project no alias mapped, does
// not clear when the configuration is fixed, because the file's bytes did not
// change. Released reports whether there was a marker, so that repeating the
// operation is safe and says so rather than inventing a failure.
type QuarantineRelease struct {
	Key string `json:"key"`
	// Released is false when the key held no quarantine, which is the answer a
	// second release gets.
	Released bool `json:"released"`
	// Digest and Reason describe the marker that was cleared, and are empty
	// when there was none.
	Digest     string    `json:"digest,omitempty"`
	Reason     string    `json:"reason,omitempty"`
	ReleasedAt time.Time `json:"releasedAt"`
}

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
