package domain

import "time"

type ActivationDispatchFailure struct {
	ID              string    `json:"id"`
	Code            string    `json:"code"`
	SafeMessage     string    `json:"safeMessage"`
	NextAction      string    `json:"nextAction"`
	AssignmentID    string    `json:"assignmentId"`
	AssignmentEpoch int64     `json:"assignmentEpoch"`
	AttemptID       string    `json:"attemptId"`
	ActivationID    string    `json:"activationId"`
	ActivationEpoch int64     `json:"activationEpoch"`
	RunID           string    `json:"runId"`
	GraphRevision   int64     `json:"graphRevision"`
	EvidenceRef     string    `json:"evidenceRef,omitempty"`
	FirstSeenAt     time.Time `json:"firstSeenAt"`
	LastSeenAt      time.Time `json:"lastSeenAt"`
}
