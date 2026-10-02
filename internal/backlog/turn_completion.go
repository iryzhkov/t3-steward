package backlog

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// ActivationTurnOutcome reports how one supervision activation's turn ended.
//
// An activation is not a task, and its turn completion is not a task result. It
// publishes no outputs, releases no dependents and satisfies no verification.
// The one thing it establishes is whether the overseer process ran to the end
// of its turn without the provider failing or leaving work in flight, which is
// why the provider-level check is the same one tasks use.
//
// What it deliberately cannot establish is a decision. A supervisor process
// exiting successfully is not gate acceptance: acceptance exists only where the
// coordinator recorded a structured decision under a live lease, a matching
// epoch and an expected revision. That count is passed in by the caller from
// its own decision records, never read out of the transcript, so no amount of
// agreeable prose in the summary can turn a no-decision activation into a
// decided one.
//
// The returned reason is empty when the turn itself was clean; it explains the
// provider-level failure otherwise. An activation whose turn failed still ends
// with an outcome, because the activation record must say how it ended.
func ActivationTurnOutcome(archive []byte, threadID, summary string, recordedDecisions int) (domain.ActivationOutcome, string, error) {
	reason, err := ResultCompletionFailure(archive, threadID, summary)
	if err != nil {
		return domain.ActivationOutcomeNone, "", err
	}
	if recordedDecisions > 0 {
		// The decisions are already durable, so the turn's own ending cannot
		// retract them; a failed turn after a recorded decision is still decided.
		return domain.ActivationOutcomeDecided, reason, nil
	}
	return domain.ActivationOutcomeNoDecision, reason, nil
}

// SessionNotReadyFailure is the reason a result is refused when the provider
// session did not end its turn cleanly.
const SessionNotReadyFailure = "provider session is not ready without an active turn or error"

// ResultCompletionFailureWithPause is ResultCompletionFailure for an attempt
// whose thread was paused by the quota watchdog on the worker host. The
// refusal of a session that is not ready stands, but it names the pause,
// so that "paused by quota watchdog: claudeAgent/claude/seven_day at 97%"
// is read instead of an unexplained session failure. pauseReason is the
// worker's pause summary; empty means no pause was recorded.
func ResultCompletionFailureWithPause(archive []byte, threadID, summary, pauseReason string) (string, error) {
	reason, err := ResultCompletionFailure(archive, threadID, summary)
	if err != nil || reason != SessionNotReadyFailure || strings.TrimSpace(pauseReason) == "" {
		return reason, err
	}
	return "paused by quota watchdog: " + strings.TrimSpace(pauseReason) + "; " + reason, nil
}

// TurnStartFailedActivity is the thread activity kind T3 appends when it
// refuses to start a provider turn: the provider rejected the turn input (as
// with a prompt over the input limit), the user message was not found, or the
// provider session could not take the turn. T3 then leaves the session in
// error with no active turn and starts nothing, so no turn ever reaches a
// terminal state the worker would otherwise wait for. Observed in T3 0.0.38
// (compat.MinServerVersion..compat.MaxServerVersion).
const TurnStartFailedActivity = "provider.turn.start.failed"

// runtimeErrorActivity is the activity T3 records for a provider runtime error
// inside a turn; its payload carries the provider's message.
const runtimeErrorActivity = "runtime.error"

// TurnStartFailure is a turn start T3 refused that no later turn superseded.
type TurnStartFailure struct {
	ActivityID string
	Detail     string
	CreatedAt  time.Time
}

// threadArchive is the part of T3's thread detail a completion judgement
// reads.
type threadArchive struct {
	Thread struct {
		ID         string `json:"id"`
		LatestTurn *struct {
			TurnID      string     `json:"turnId"`
			State       string     `json:"state"`
			RequestedAt *time.Time `json:"requestedAt"`
			StartedAt   *time.Time `json:"startedAt"`
			CompletedAt *time.Time `json:"completedAt"`
		} `json:"latestTurn"`
		Session *struct {
			ThreadID     string  `json:"threadId"`
			Status       string  `json:"status"`
			ActiveTurnID *string `json:"activeTurnId"`
			LastError    *string `json:"lastError"`
		} `json:"session"`
		Activities []struct {
			ID      string `json:"id"`
			Kind    string `json:"kind"`
			Payload struct {
				Detail  string `json:"detail"`
				Message string `json:"message"`
			} `json:"payload"`
			TurnID    *string `json:"turnId"`
			CreatedAt string  `json:"createdAt"`
		} `json:"activities"`
		HasPendingApprovals bool    `json:"hasPendingApprovals"`
		HasPendingUserInput bool    `json:"hasPendingUserInput"`
		BackgroundLiveness  *string `json:"backgroundLiveness"`
	} `json:"thread"`
}

// maxProviderDetail bounds the provider text a failure reason carries; T3's
// own details are short, and the reason is printed on one line.
const maxProviderDetail = 2000

func providerDetail(text string) string {
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > maxProviderDetail {
		text = strings.ToValidUTF8(text[:maxProviderDetail], "") + "..."
	}
	return text
}

// latestTurnStartFailure finds the newest refused turn start, unless a turn
// was requested after it: a refusal that a later turn superseded is history.
func (a threadArchive) latestTurnStartFailure() (TurnStartFailure, bool) {
	var latest TurnStartFailure
	found := false
	for _, activity := range a.Thread.Activities {
		if activity.Kind != TurnStartFailedActivity {
			continue
		}
		created, err := time.Parse(time.RFC3339Nano, activity.CreatedAt)
		if err != nil {
			continue
		}
		if !found || !created.Before(latest.CreatedAt) {
			latest = TurnStartFailure{ActivityID: activity.ID, Detail: providerDetail(activity.Payload.Detail), CreatedAt: created}
			found = true
		}
	}
	if !found {
		return TurnStartFailure{}, false
	}
	if turn := a.Thread.LatestTurn; turn != nil && turn.TurnID != "" {
		since := turn.RequestedAt
		if since == nil {
			since = turn.StartedAt
		}
		if since == nil || !latest.CreatedAt.After(*since) {
			return TurnStartFailure{}, false
		}
	}
	if latest.Detail == "" {
		latest.Detail = "T3 recorded no detail"
	}
	return latest, true
}

// failedTurnDetail is what T3 recorded about why the latest turn failed: the
// turn's own runtime error, or else the session's last error.
func (a threadArchive) failedTurnDetail() string {
	turn := a.Thread.LatestTurn
	if turn != nil && turn.TurnID != "" {
		detail := ""
		for _, activity := range a.Thread.Activities {
			if activity.Kind == runtimeErrorActivity && activity.TurnID != nil && *activity.TurnID == turn.TurnID {
				if text := providerDetail(activity.Payload.Message); text != "" {
					detail = text
				}
			}
		}
		if detail != "" {
			return detail
		}
	}
	if session := a.Thread.Session; session != nil && session.LastError != nil {
		return providerDetail(*session.LastError)
	}
	return ""
}

// LatestTurnStartFailure reports the turn start T3 refused for the thread in
// archive, when no later turn superseded it.
func LatestTurnStartFailure(archive []byte) (TurnStartFailure, bool, error) {
	var snapshot threadArchive
	if err := json.Unmarshal(archive, &snapshot); err != nil {
		return TurnStartFailure{}, false, fmt.Errorf("thread archive is invalid: %w", err)
	}
	failure, ok := snapshot.latestTurnStartFailure()
	return failure, ok, nil
}

// TurnStartRefusedFailure is the prefix of the reason an attempt fails with
// when T3 refused to start its turn.
const TurnStartRefusedFailure = "T3 refused to start the provider turn"

// ResultCompletionFailure validates provider completion independently of prose.
// A legacy done marker is optional and never overrides unsuccessful execution.
func ResultCompletionFailure(archive []byte, threadID, summary string) (string, error) {
	var snapshot threadArchive
	if err := json.Unmarshal(archive, &snapshot); err != nil {
		return "", fmt.Errorf("result import thread archive is invalid: %w", err)
	}
	if reason, failed := backlogFailedReason(summary); failed {
		return reason, nil
	}
	for _, line := range strings.Split(summary, "\n") {
		switch strings.ToLower(strings.TrimSpace(line)) {
		case "backlog status: continue", "backlog status: needs-input":
			return "agent reported unfinished work: " + strings.TrimSpace(line), nil
		}
	}
	thread := snapshot.Thread
	if threadID == "" || thread.ID != threadID {
		return "thread completion identity is missing or mismatched", nil
	}
	// A refused start is checked before the latest turn: after a refusal of a
	// later turn (a wake, a resume) the latest turn is an earlier one that may
	// well have completed.
	if refused, ok := snapshot.latestTurnStartFailure(); ok {
		return TurnStartRefusedFailure + ": " + refused.Detail, nil
	}
	turn := thread.LatestTurn
	if turn == nil || turn.TurnID == "" || turn.State != "completed" {
		if detail := snapshot.failedTurnDetail(); detail != "" {
			return "provider turn did not complete successfully: " + detail, nil
		}
		return "provider turn did not complete successfully", nil
	}
	if turn.StartedAt == nil || turn.CompletedAt == nil || turn.StartedAt.IsZero() || turn.CompletedAt.IsZero() || turn.CompletedAt.Before(*turn.StartedAt) {
		return "provider completion timestamps are missing or invalid", nil
	}
	session := thread.Session
	if session == nil || session.ThreadID != threadID || session.Status != "ready" || session.ActiveTurnID != nil || (session.LastError != nil && *session.LastError != "") {
		return SessionNotReadyFailure, nil
	}
	if thread.HasPendingApprovals || thread.HasPendingUserInput || (thread.BackgroundLiveness != nil && *thread.BackgroundLiveness == "working") {
		return "thread still has pending input, approval, or background work", nil
	}
	return "", nil
}
