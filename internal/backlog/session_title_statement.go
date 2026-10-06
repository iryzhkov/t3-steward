package backlog

import (
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// sessionStateRetention is how long a finished execution keeps being stated, so
// that its thread can show the terminal state the coordinator recorded. The
// worker keeps the record for longer than this, so nothing is stated about an
// execution it has already forgotten.
const sessionStateRetention = 24 * time.Hour

// SessionStateForAttempt maps the coordinator's recorded attempt state onto the
// lifecycle a session title shows. It reads only durable progress and control
// state; nothing a model wrote can reach it. A state with no faithful title,
// such as skipped, reports false and the title is left as it is.
func SessionStateForAttempt(attempt domain.Attempt) (workerproto.SessionState, bool) {
	switch attempt.Progress {
	case domain.ProgressSucceeded:
		return workerproto.SessionCompleted, true
	case domain.ProgressFailed:
		return workerproto.SessionFailed, true
	case domain.ProgressCancelled:
		return workerproto.SessionCancelled, true
	case domain.ProgressVerifying:
		return workerproto.SessionCollecting, true
	case domain.ProgressWaitingExternal, domain.ProgressNeedsInput:
		return workerproto.SessionWaiting, true
	case domain.ProgressQueued, domain.ProgressBlocked, domain.ProgressReady:
		return workerproto.SessionStarting, true
	case domain.ProgressActive:
		switch attempt.Control {
		case domain.ControlRunning, domain.ControlResuming, domain.ControlDraining:
			return workerproto.SessionRunning, true
		case domain.ControlPaused, domain.ControlPausedUncheckpointed, domain.ControlWaitingExternal:
			return workerproto.SessionWaiting, true
		case domain.ControlStopped:
			// The turn ended and the result has not been verified yet.
			return workerproto.SessionCollecting, true
		default:
			return workerproto.SessionStarting, true
		}
	default:
		return "", false
	}
}

// stateSessionTitles adds the recorded lifecycle and campaign progress of one
// worker's current and recently finished executions to its snapshot request.
// It is called only for a worker that advertises CapabilitySessionTitles. It
// never fails: a title is descriptive, and nothing about it may block the
// exchange that carries admission, dispatch and collection.
func stateSessionTitles(request *workerproto.SnapshotRequest, records sqlite.CoordinatorRecords, workerID string, now time.Time) {
	attempts := make(map[string]domain.Attempt, len(records.Attempts))
	for _, attempt := range records.Attempts {
		attempts[attempt.ID] = attempt
	}
	progress := map[string]*workerproto.CampaignProgress{}
	states := []workerproto.AssignmentSessionState{}
	for _, assignment := range records.Assignments {
		if assignment.WorkerID != workerID {
			continue
		}
		attempt, ok := attempts[assignment.AttemptID]
		if !ok {
			continue
		}
		switch assignment.State {
		case domain.AssignmentClaimed, domain.AssignmentUnknown:
		case domain.AssignmentCompleted, domain.AssignmentReleased:
			// A released execution is stated only once its attempt is over:
			// before that, the attempt may be continuing on another thread.
			if now.Sub(assignment.UpdatedAt) > sessionStateRetention ||
				(assignment.State == domain.AssignmentReleased && !attempt.Progress.Terminal()) {
				continue
			}
		default:
			continue
		}
		state, ok := SessionStateForAttempt(attempt)
		if !ok {
			continue
		}
		runProgress, known := progress[attempt.WorkflowRunID]
		if !known {
			runProgress = campaignProgress(records, attempt.WorkflowRunID)
			progress[attempt.WorkflowRunID] = runProgress
		}
		states = append(states, workerproto.AssignmentSessionState{
			AssignmentID: assignment.ID, AssignmentEpoch: assignment.Epoch,
			AttemptID: attempt.ID, AttemptRevision: attempt.Revision,
			State: state, Progress: runProgress,
		})
	}
	sort.Slice(states, func(i, j int) bool { return states[i].AssignmentID < states[j].AssignmentID })
	if len(states) > workerproto.MaxSessionStates {
		states = states[:workerproto.MaxSessionStates]
	}
	request.SessionStatesReported = true
	request.SessionStates = states
}

// campaignProgress counts a run's executable tasks and how many of them the
// coordinator recorded as succeeded in their latest attempt. The sink is not
// executable work and is never counted. It returns nil when there is no
// authoritative count.
func campaignProgress(records sqlite.CoordinatorRecords, runID string) *workerproto.CampaignProgress {
	var run *domain.WorkflowRun
	for index := range records.WorkflowRuns {
		if records.WorkflowRuns[index].ID == runID {
			run = &records.WorkflowRuns[index]
			break
		}
	}
	if run == nil {
		return nil
	}
	latest := map[string]domain.Attempt{}
	for _, attempt := range domain.DeclaredTaskAttempts(records.Attempts) {
		if attempt.WorkflowRunID != runID {
			continue
		}
		if previous, ok := latest[attempt.TaskID]; !ok || attempt.Number > previous.Number {
			latest[attempt.TaskID] = attempt
		}
	}
	result := workerproto.CampaignProgress{}
	for _, task := range domain.TasksForRun(*run, records.Tasks) {
		if task.Name == domain.SinkTaskName || (run.Sink != nil && task.ID == run.Sink.ID) {
			continue
		}
		result.Total++
		if attempt, ok := latest[task.ID]; ok && attempt.Progress == domain.ProgressSucceeded {
			result.Completed++
		}
	}
	if result.Total < 1 || result.Total > workerproto.MaxCampaignProgressTotal {
		return nil
	}
	return &result
}
