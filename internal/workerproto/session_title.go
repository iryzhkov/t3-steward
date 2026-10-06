package workerproto

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
)

// CapabilitySessionTitles advertises that this worker build understands the
// coordinator's session-state statement on the snapshot exchange and keeps its
// threads' titles in step with it. Both sides decode the snapshot request
// strictly, so the coordinator sends the statement only to a worker that
// advertises this, the way it asks for quota observations.
const CapabilitySessionTitles = "session-titles-v1"

// MaxSessionStates bounds one statement for the same reason MaxParkedAssignments
// does: a malformed coordinator message must not grow a worker's journal.
const MaxSessionStates = 1024

// MaxCampaignProgressTotal bounds the executable task count a statement may
// claim. It is far above any real campaign and keeps the title bounded.
const MaxCampaignProgressTotal = 100000

// SessionTitleMaxBytes bounds a live session title. The name parts are bounded
// separately so that bounding never cuts the lifecycle suffix.
const SessionTitleMaxBytes = 256

// SessionState is the lifecycle a session title shows. Every value is derived by
// the coordinator from its own recorded attempt state, never from model output.
type SessionState string

const (
	SessionStarting   SessionState = "starting"
	SessionRunning    SessionState = "running"
	SessionWaiting    SessionState = "waiting"
	SessionCollecting SessionState = "collecting"
	SessionCompleted  SessionState = "completed"
	SessionFailed     SessionState = "failed"
	SessionCancelled  SessionState = "cancelled"
)

// Valid reports whether the state is one a title may show.
func (s SessionState) Valid() bool {
	switch s {
	case SessionStarting, SessionRunning, SessionWaiting, SessionCollecting, SessionCompleted, SessionFailed, SessionCancelled:
		return true
	default:
		return false
	}
}

// CampaignProgress is campaign progress, not task progress: how many of the
// run's executable tasks the coordinator recorded as succeeded, out of all of
// them. The synthetic sink node is never counted. A statement omits it when the
// coordinator has no authoritative count.
type CampaignProgress struct {
	Completed int `json:"completed"`
	Total     int `json:"total"`
}

// AssignmentSessionState is the coordinator's statement of one execution's
// recorded lifecycle. It is fenced on the assignment epoch: a worker applies it
// only to the execution it names.
type AssignmentSessionState struct {
	AssignmentID    string            `json:"assignmentId"`
	AssignmentEpoch int64             `json:"assignmentEpoch"`
	AttemptID       string            `json:"attemptId"`
	AttemptRevision int64             `json:"attemptRevision"`
	State           SessionState      `json:"state"`
	Progress        *CampaignProgress `json:"progress,omitempty"`
}

// validateSessionStates refuses a statement whole, like the parked list: a
// title is descriptive, but a worker must not store what it cannot trust.
func validateSessionStates(request SnapshotRequest) error {
	if !request.SessionStatesReported && len(request.SessionStates) != 0 {
		return errors.New("worker protocol: session states listed without the reported flag")
	}
	if len(request.SessionStates) > MaxSessionStates {
		return fmt.Errorf("worker protocol: %d session states exceed the limit of %d", len(request.SessionStates), MaxSessionStates)
	}
	seen := make(map[string]struct{}, len(request.SessionStates))
	for _, state := range request.SessionStates {
		if state.AssignmentID == "" || state.AssignmentEpoch < 1 || state.AttemptID == "" || state.AttemptRevision < 0 {
			return errors.New("worker protocol: session state identity is incomplete")
		}
		if !state.State.Valid() {
			return fmt.Errorf("worker protocol: session state %q is not a lifecycle state", state.State)
		}
		if progress := state.Progress; progress != nil &&
			(progress.Total < 1 || progress.Total > MaxCampaignProgressTotal || progress.Completed < 0 || progress.Completed > progress.Total) {
			return errors.New("worker protocol: session progress is not a bounded count")
		}
		if _, duplicate := seen[state.AssignmentID]; duplicate {
			return fmt.Errorf("worker protocol: session states repeat %q", state.AssignmentID)
		}
		seen[state.AssignmentID] = struct{}{}
	}
	return nil
}

// SessionTitle is the live title of an execution's thread:
//
//	[Steward] <workflow>[: <task>] · <role> · <state>[ · campaign n/m] · run <suffix>
//
// The role comes only from frozen package metadata: supervision for an
// activation, review for a review judge, executor otherwise. Names fall back to
// the execution identity, are sanitised and bounded, and the run suffix keeps
// concurrent runs of one workflow apart. The same input always gives the same
// title, which is what makes updates idempotent.
func SessionTitle(pkg ExecutionPackage, state SessionState, progress *CampaignProgress) string {
	workflow, task, role := "", "", "executor"
	if pkg.Display != nil {
		workflow, task = pkg.Display.WorkflowName, pkg.Display.TaskName
		if pkg.Display.ReviewJudge {
			role = "review"
		}
	}
	workflow = displayText(workflow, 32, 80)
	if workflow == "" {
		workflow = displayText(pkg.Identity.WorkflowID, 32, 80)
	}
	if workflow == "" {
		workflow = "workflow"
	}
	name := workflow
	if pkg.Supervision != nil {
		role = "supervision"
	} else {
		task = displayText(task, 24, 64)
		if task == "" {
			task = displayText(pkg.Identity.TaskID, 24, 64)
		}
		if task != "" {
			name += ": " + task
		}
	}
	title := "[Steward] " + name + " · " + role + " · " + string(state)
	if progress != nil && progress.Total > 0 {
		title += " · campaign " + strconv.Itoa(progress.Completed) + "/" + strconv.Itoa(progress.Total)
	}
	run := sha256.Sum256([]byte(pkg.Identity.WorkflowRunID))
	return title + " · run " + hex.EncodeToString(run[:3])
}
