package backlogadmin

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type NodeWaitOperation struct {
	Action  string                 `json:"action"`
	Request domain.NodeWaitRequest `json:"request"`
	ID      string                 `json:"id,omitempty"`
	// Task carries a task-bound registration. It rides the node-wait operation
	// rather than a new transport method because the two are the same admin
	// privilege over the same coordinator records; only what they park differs.
	Task   *domain.TaskWaitRegistration `json:"task,omitempty"`
	Result *domain.TaskWaitResult       `json:"result,omitempty"`
	From   string                       `json:"from,omitempty"`
	To     string                       `json:"to,omitempty"`
}
type NodeWaitResponse struct {
	Waits     []domain.NodeWait            `json:"waits"`
	TaskWaits []domain.TaskWait            `json:"taskWaits,omitempty"`
	TaskWakes []domain.TaskWaitWakeContext `json:"taskWakes,omitempty"`
	Changed   bool                         `json:"changed,omitempty"`
}
type nodeWaitStore interface {
	RegisterNodeWait(context.Context, domain.NodeWaitRequest, string, string, time.Time) (domain.NodeWait, error)
	ListNodeWaits(context.Context) ([]domain.NodeWait, error)
	SettleNodeWaits(context.Context, time.Time) error
	TransitionNodeWake(context.Context, string, string, string, time.Time) (bool, error)
}

type taskWaitStore interface {
	RegisterTaskWait(context.Context, domain.TaskWaitRegistration, time.Time) (domain.TaskWait, error)
	ListTaskWaits(context.Context) ([]domain.TaskWait, error)
	CancelTaskWait(context.Context, string, time.Time) (domain.TaskWait, error)
}

func (s *Service) NodeWait(ctx context.Context, principal Principal, op NodeWaitOperation) (NodeWaitResponse, error) {
	var result NodeWaitResponse
	action := Action{Kind: QueryKind("node-wait"), WorkflowRunID: op.Request.Target.RunID, TaskID: op.Request.Target.TaskID}
	if op.Task != nil {
		action.WorkflowRunID, action.TaskID = op.Task.WorkflowRunID, op.Task.TaskID
	}
	if err := s.authorizer.Authorize(ctx, principal, action); err != nil {
		return result, err
	}
	if op.Action == "settle-task" || op.Action == "expire-task" || op.Action == "wake-task" || op.Action == "pending-task" || op.Action == "transition-task" {
		return s.taskWaitRuntime(ctx, op)
	}
	if op.Action == "register-task" || op.Action == "list-task" || op.Action == "cancel-task" {
		return s.taskWait(ctx, op)
	}
	store, ok := s.reader.(nodeWaitStore)
	if !ok {
		return result, errors.New("native waits unavailable")
	}
	switch op.Action {
	case "register":
		host, err := os.Hostname()
		if err != nil {
			return result, err
		}
		w, err := store.RegisterNodeWait(ctx, op.Request, principal.ID, host, s.now())
		if err != nil {
			return result, err
		}
		result.Waits = []domain.NodeWait{w}
		return result, nil
	case "list", "cancel", "run-now":
		if op.Action != "list" && op.ID == "" {
			return result, errors.New("native wait ID required")
		}
		waits, err := store.ListNodeWaits(ctx)
		if err != nil {
			return result, err
		}
		for _, w := range waits {
			if op.ID != "" && w.Request.ID != op.ID {
				continue
			}
			if op.Action == "cancel" {
				changed, err := store.TransitionNodeWake(ctx, w.Request.ID, w.Delivery, "cancelled", s.now())
				if err != nil {
					return result, err
				}
				if !changed {
					return result, errors.New("native wait changed concurrently; inspect current delivery state")
				}
				w.Delivery = "cancelled"
			}
			result.Waits = append(result.Waits, w)
		}
		if op.Action != "list" && len(result.Waits) != 1 {
			return result, errors.New("native wait ID not found")
		}
		if op.Action == "run-now" {
			if err := store.SettleNodeWaits(ctx, s.now()); err != nil {
				return result, err
			}
			return s.NodeWait(ctx, principal, NodeWaitOperation{Action: "list", ID: op.ID})
		}
		return result, nil
	default:
		return result, errors.New("unknown native wait action")
	}
}

// taskWait registers, lists or cancels the coordinator-owned waits that park an
// attempt. Registration is refused rather than adjusted when the attempt is
// terminal or its revision has moved: the caller is an agent that can act on a
// plain command error, and guessing on its behalf is how a thread acquires a
// reason to keep working for a task it no longer owns.
func (s *Service) taskWait(ctx context.Context, op NodeWaitOperation) (NodeWaitResponse, error) {
	var result NodeWaitResponse
	store, ok := s.reader.(taskWaitStore)
	if !ok {
		return result, errors.New("task-bound waits unavailable")
	}
	switch op.Action {
	case "register-task":
		if op.Task == nil {
			return result, errors.New("task-bound wait registration is missing")
		}
		wait, err := store.RegisterTaskWait(ctx, *op.Task, s.now())
		if err != nil {
			return result, err
		}
		result.TaskWaits = []domain.TaskWait{wait}
		return result, nil
	case "cancel-task":
		if op.ID == "" {
			return result, errors.New("task-bound wait ID required")
		}
		wait, err := store.CancelTaskWait(ctx, op.ID, s.now())
		if err != nil {
			return result, err
		}
		result.TaskWaits = []domain.TaskWait{wait}
		return result, nil
	default:
		waits, err := store.ListTaskWaits(ctx)
		if err != nil {
			return result, err
		}
		for _, wait := range waits {
			if op.ID == "" || wait.ID == op.ID {
				result.TaskWaits = append(result.TaskWaits, wait)
			}
		}
		return result, nil
	}
}

func (c LocalClient) NodeWait(ctx context.Context, op NodeWaitOperation) (NodeWaitResponse, error) {
	var response localResponse
	err := c.call(ctx, localRequest{Version: LocalTransportVersion, Operation: "node-wait", NodeWait: &op}, &response)
	if err != nil {
		return NodeWaitResponse{}, err
	}
	if response.NodeWait == nil {
		return NodeWaitResponse{}, errors.New("missing native wait response")
	}
	return *response.NodeWait, nil
}
