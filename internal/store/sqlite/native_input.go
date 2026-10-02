package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// nativeInputReasonBytes bounds the question or answer text kept in one event.
const nativeInputReasonBytes = 4000

// NativeUserInputEventID is the idempotent audit identity of one native
// question or answer: the attempt and T3's own activity ID.
func NativeUserInputEventID(attemptID, activityID string) string {
	return "native-input:" + attemptID + ":" + activityID
}

// RecordNativeUserInput keeps, in the run's events, the questions a task asked
// with its provider's own question tool (AskUserQuestion) in its own thread
// and the answers given there. The run then holds the decision even though no
// ask was registered for it.
//
// The reporting worker must hold the attempt's assignment and name the
// attempt's own thread. Each activity is recorded once, under an ID derived
// from T3's activity ID; a repeated report adds nothing.
func (s *Store) RecordNativeUserInput(ctx context.Context, workerID, attemptID, threadID string, events []domain.UserInputEvent, now time.Time) (int, error) {
	if workerID == "" || attemptID == "" || threadID == "" || now.IsZero() {
		return 0, errors.New("a native user-input report needs the worker, the attempt, the thread and a timestamp")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	attempt, err := loadAttemptTx(ctx, tx, attemptID)
	if err != nil {
		return 0, err
	}
	assignment, err := loadAssignmentTx(ctx, tx, attempt.AssignmentID)
	if err != nil {
		return 0, err
	}
	if assignment.WorkerID != workerID {
		return 0, fmt.Errorf("worker %q does not run attempt %q", workerID, attemptID)
	}
	if attempt.ThreadID != threadID {
		return 0, fmt.Errorf("thread %q is not attempt %q's thread", threadID, attemptID)
	}
	requests := map[string]domain.UserInputEvent{}
	recorded := 0
	for _, event := range events {
		if event.ActivityID == "" {
			return recorded, errors.New("a native user-input event needs its T3 activity ID")
		}
		if event.Kind == domain.UserInputRequested {
			requests[event.RequestID] = event
		}
		id := NativeUserInputEventID(attemptID, event.ActivityID)
		if _, found, err := loadAuditEventTx(ctx, tx, id); err != nil {
			return recorded, err
		} else if found {
			continue
		}
		at := event.At
		if at.IsZero() {
			at = now
		}
		kind, outcome, reason := "native-question", "asked", nativeQuestionText(event.Questions)
		if event.Kind == domain.UserInputResolved {
			kind = "native-answer"
			reason, outcome = nativeAnswerText(requests[event.RequestID].Questions, event)
		}
		reason = domain.TruncateUTF8(reason, nativeInputReasonBytes)
		if strings.TrimSpace(reason) == "" {
			reason = "(empty)"
		}
		identity := event.RequestID
		if identity == "" {
			identity = event.ActivityID
		}
		if _, err := insertNativeAuditEventTx(ctx, tx, nativeAuditInput{
			ID: id, Kind: kind, WorkflowRunID: attempt.WorkflowRunID, TaskID: attempt.TaskID, AttemptID: attempt.ID,
			TargetType: domain.AdminTargetAttempt, TargetID: attempt.ID, Actor: "worker:" + workerID,
			Reason: strings.TrimSpace(reason), CreatedAt: at.UTC(),
			Detail: nativeAuditDetail{IdempotencyIdentity: strings.TrimSpace(identity), Outcome: outcome},
		}); err != nil {
			return recorded, err
		}
		recorded++
	}
	return recorded, tx.Commit()
}

func nativeQuestionText(questions []domain.UserInputQuestion) string {
	parts := make([]string, 0, len(questions))
	for _, question := range questions {
		labels := make([]string, 0, len(question.Options))
		for _, option := range question.Options {
			labels = append(labels, option.Label)
		}
		text := strings.TrimSpace(question.Question)
		if len(labels) != 0 {
			text += " [" + strings.Join(labels, " | ") + "]"
		}
		parts = append(parts, text)
	}
	return strings.Join(parts, "; ")
}

func nativeAnswerText(questions []domain.UserInputQuestion, event domain.UserInputEvent) (string, string) {
	if len(event.Answers) == 0 {
		return "the question card was dismissed without an answer", "dismissed"
	}
	parts := []string{}
	if len(questions) == 0 {
		for key, value := range event.Answers {
			parts = append(parts, fmt.Sprintf("%s: %v", key, value))
		}
	}
	for _, question := range questions {
		parts = append(parts, strings.TrimSpace(question.Question)+": "+strings.Join(event.AnswerValues(question), ", "))
	}
	return strings.Join(parts, "; "), "answered"
}
