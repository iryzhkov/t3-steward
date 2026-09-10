package domain

import "time"

// TurnOutcomeMarker is the structured completion marker from one finished turn.
type TurnOutcomeMarker string

const (
	TurnOutcomeDone     TurnOutcomeMarker = "done"
	TurnOutcomeContinue TurnOutcomeMarker = "continue"
	TurnOutcomeMissing  TurnOutcomeMarker = "missing"
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
