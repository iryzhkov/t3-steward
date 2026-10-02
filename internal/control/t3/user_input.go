package t3

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/t3api"
)

// userInputTurns is how many recent turns are read for user-input activity. T3
// pins the activities of an open request into every window, so a pending
// question is seen however old it is; the window bounds the resolved history.
const userInputTurns = 4

// UserInputEvents returns the user-input.requested and user-input.resolved
// activities of a thread's recent turns, oldest first. These are the only
// record of a native question card and its answer: T3 has no command that
// reads them otherwise.
func (c *Control) UserInputEvents(ctx context.Context, threadID string) ([]domain.UserInputEvent, error) {
	detail, err := c.client.ThreadDetail(ctx, threadID, userInputTurns)
	if err != nil {
		return nil, err
	}
	return UserInputEventsOf(detail.Activities)
}

// UserInputEventsOf decodes the user-input activities among a thread's
// activities. An activity whose payload does not decode is an error rather
// than skipped: a question or an answer read as absent is a decision lost.
func UserInputEventsOf(activities []t3api.Activity) ([]domain.UserInputEvent, error) {
	var events []domain.UserInputEvent
	for _, activity := range activities {
		var kind domain.UserInputEventKind
		switch activity.Kind {
		case "user-input.requested":
			kind = domain.UserInputRequested
		case "user-input.resolved":
			kind = domain.UserInputResolved
		default:
			continue
		}
		var payload struct {
			RequestID string                     `json:"requestId"`
			Questions []domain.UserInputQuestion `json:"questions"`
			Answers   map[string]any             `json:"answers"`
		}
		if len(activity.Payload) != 0 {
			if err := json.Unmarshal(activity.Payload, &payload); err != nil {
				return nil, fmt.Errorf("decode %s activity %s: %w", activity.Kind, activity.ID, err)
			}
		}
		event := domain.UserInputEvent{
			Kind: kind, ActivityID: activity.ID, RequestID: payload.RequestID,
			Questions: payload.Questions, Answers: payload.Answers,
		}
		if activity.TurnID != nil {
			event.TurnID = *activity.TurnID
		}
		if at, err := time.Parse(time.RFC3339Nano, activity.CreatedAt); err == nil {
			event.At = at.UTC()
		}
		events = append(events, event)
	}
	return events, nil
}

// ArchiveThread archives a thread this steward created, such as an ask relay
// thread whose question has been settled. Archiving also stops its session,
// which withdraws a question card nobody answered.
func (c *Control) ArchiveThread(ctx context.Context, threadID string) error {
	if c.DryRun {
		c.log.Info("dry-run: would archive thread", "thread", threadID)
		return nil
	}
	return c.dispatchThreadArchiveState(ctx, "thread.archive", threadID)
}

// StartAskRelay creates an ask relay thread and starts its one turn. The
// thread ID and every command ID are derived from the ask, so a retry after an
// ambiguous response repeats the same commands rather than opening a second
// thread.
func (c *Control) StartAskRelay(ctx context.Context, relay domain.AskRelayStart) error {
	_, err := c.CreateAndStartThread(ctx, NewThreadInput{
		ThreadID: relay.ThreadID, DispatchToken: "ask-relay:" + relay.AskID,
		ProjectID: relay.ProjectID, Title: relay.Title,
		ModelSelection: map[string]any{"instanceId": relay.Instance, "model": relay.Model},
		RuntimeMode:    "full-access", InteractionMode: "default",
		Prompt: relay.Prompt,
	})
	return err
}
