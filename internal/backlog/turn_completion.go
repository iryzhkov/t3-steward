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

// ResultCompletionFailure validates provider completion independently of prose.
// A legacy done marker is optional and never overrides unsuccessful execution.
func ResultCompletionFailure(archive []byte, threadID, summary string) (string, error) {
	var snapshot struct {
		Thread struct {
			ID         string `json:"id"`
			LatestTurn *struct {
				TurnID      string     `json:"turnId"`
				State       string     `json:"state"`
				StartedAt   *time.Time `json:"startedAt"`
				CompletedAt *time.Time `json:"completedAt"`
			} `json:"latestTurn"`
			Session *struct {
				ThreadID     string  `json:"threadId"`
				Status       string  `json:"status"`
				ActiveTurnID *string `json:"activeTurnId"`
				LastError    *string `json:"lastError"`
			} `json:"session"`
			HasPendingApprovals bool    `json:"hasPendingApprovals"`
			HasPendingUserInput bool    `json:"hasPendingUserInput"`
			BackgroundLiveness  *string `json:"backgroundLiveness"`
		} `json:"thread"`
	}
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
	turn := thread.LatestTurn
	if turn == nil || turn.TurnID == "" || turn.State != "completed" {
		return "provider turn did not complete successfully", nil
	}
	if turn.StartedAt == nil || turn.CompletedAt == nil || turn.StartedAt.IsZero() || turn.CompletedAt.IsZero() || turn.CompletedAt.Before(*turn.StartedAt) {
		return "provider completion timestamps are missing or invalid", nil
	}
	session := thread.Session
	if session == nil || session.ThreadID != threadID || session.Status != "ready" || session.ActiveTurnID != nil || (session.LastError != nil && *session.LastError != "") {
		return "provider session is not ready without an active turn or error", nil
	}
	if thread.HasPendingApprovals || thread.HasPendingUserInput || (thread.BackgroundLiveness != nil && *thread.BackgroundLiveness == "working") {
		return "thread still has pending input, approval, or background work", nil
	}
	return "", nil
}
