package backlog

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

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
