package domain

import "time"

// ScheduleTriggerSource distinguishes recurring firings from explicit operator runs.
type ScheduleTriggerSource string

const (
	ScheduleTriggerScheduled ScheduleTriggerSource = "scheduled"
	ScheduleTriggerManual    ScheduleTriggerSource = "manual"
)

// ScheduleTriggerRequest is a transport-neutral request to observe a schedule firing.
// IDs are supplied by the caller so retries remain deterministic across restarts.
type ScheduleTriggerRequest struct {
	ScheduleID    string
	TriggerID     string
	WorkflowRunID string
	NominalAt     time.Time
	ObservedAt    time.Time
	Source        ScheduleTriggerSource
	Misfired      bool
}

// ScheduleTriggerResult is the durable outcome of observing a schedule firing.
// WorkflowRun is present only when the trigger was accepted.
type ScheduleTriggerResult struct {
	Trigger     Trigger
	WorkflowRun *WorkflowRun
	Replay      bool
}

// OccurrenceKey returns the durable idempotency key for a trigger request.
func (r ScheduleTriggerRequest) OccurrenceKey() string {
	if r.Source == ScheduleTriggerManual {
		return r.ScheduleID + "/manual/" + r.TriggerID
	}
	return r.ScheduleID + "/" + r.NominalAt.UTC().Format(time.RFC3339Nano)
}
