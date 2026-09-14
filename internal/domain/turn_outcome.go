package domain

import "time"

// TurnOutcomeMarker is the structured completion marker from one finished turn.
type TurnOutcomeMarker string

const (
	TurnOutcomeDone     TurnOutcomeMarker = "done"
	TurnOutcomeContinue TurnOutcomeMarker = "continue"
	TurnOutcomeMissing  TurnOutcomeMarker = "missing"
	// TurnOutcomeWaiting is a turn that parked on a task-bound wait instead of
	// finishing. It is produced by wait registration, before any collection, and
	// it is the only honest answer to "what happened to that turn": the thread
	// stopped, the task did not.
	TurnOutcomeWaiting TurnOutcomeMarker = "waiting"
)

// TurnOutcome is a verified observation of one finished attempt turn.
type TurnOutcome struct {
	ID                     string            `json:"id"`
	AttemptID              string            `json:"attemptId"`
	Marker                 TurnOutcomeMarker `json:"marker"`
	VerificationPassed     bool              `json:"verificationPassed"`
	Failure                string            `json:"failure,omitempty"`
	FinalSummaryArtifactID string            `json:"finalSummaryArtifactId,omitempty"`
	ObservedAt             time.Time         `json:"observedAt"`
}

// TurnOutcomeTransition atomically advances a canonical attempt and, when
// present, the active throttle projection which determined its control state.
type TurnOutcomeTransition struct {
	OutcomeID               string                     `json:"outcomeId"`
	ExpectedAttemptRevision int64                      `json:"expectedAttemptRevision"`
	Attempt                 Attempt                    `json:"attempt"`
	Throttle                *ThrottleAttemptTransition `json:"throttle,omitempty"`
}
