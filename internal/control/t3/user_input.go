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

// AskRelayRuntimeMode is the most restricted runtime mode T3 offers. It maps
// to the Claude SDK's default permission mode, in which Claude Code still
// allows read-only tools (Read, Glob, Grep) without asking; only other tools,
// such as edits and commands, need the owner's click. It limits what a relay
// agent that strays from its one instruction can change, not what it can
// read. T3 cannot restrict which tools the model tries, what it says, or how
// it phrases the question, so the real guard is the steward's own check of
// the card before it accepts an answer to it.
const AskRelayRuntimeMode = "approval-required"

// CreateAskRelayThread creates an ask relay thread with no turn. The thread
// ID and the command ID are derived from the ask, so a retry after an
// ambiguous response repeats the same command.
func (c *Control) CreateAskRelayThread(ctx context.Context, relay domain.AskRelayStart) error {
	token := "ask-relay:" + relay.AskID
	create := map[string]any{
		"type": "thread.create", "commandId": deterministicID(token, "thread.create"),
		"threadId": relay.ThreadID, "projectId": relay.ProjectID, "title": relay.Title,
		"modelSelection":  map[string]any{"instanceId": relay.Instance, "model": relay.Model},
		"runtimeMode":     AskRelayRuntimeMode,
		"interactionMode": "default", "branch": nil, "worktreePath": nil, "createdAt": now(),
	}
	if c.DryRun {
		c.log.Info("dry-run: would create ask relay thread", "thread", relay.ThreadID)
		return nil
	}
	if _, err := c.client.Dispatch(ctx, create); err != nil {
		return fmt.Errorf("create ask relay thread %s: %w", relay.ThreadID, err)
	}
	return nil
}

// StartAskRelayTurn starts the relay thread's one turn, with command and
// message IDs derived from the ask.
func (c *Control) StartAskRelayTurn(ctx context.Context, relay domain.AskRelayStart) error {
	token := "ask-relay:" + relay.AskID
	turn := map[string]any{
		"type": "thread.turn.start", "commandId": deterministicID(token, "thread.turn.start"),
		"threadId": relay.ThreadID,
		"message": map[string]any{
			"messageId": deterministicID(token, "message"), "role": "user",
			"text": relay.Prompt, "attachments": []any{},
		},
		"modelSelection":  map[string]any{"instanceId": relay.Instance, "model": relay.Model},
		"titleSeed":       relay.Title,
		"runtimeMode":     AskRelayRuntimeMode,
		"interactionMode": "default", "createdAt": now(),
	}
	if c.DryRun {
		c.log.Info("dry-run: would start the ask relay turn", "thread", relay.ThreadID)
		return nil
	}
	if _, err := c.client.Dispatch(ctx, turn); err != nil {
		return fmt.Errorf("start the ask relay turn on %s: %w", relay.ThreadID, err)
	}
	return nil
}
