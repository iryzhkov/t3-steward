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
}
type NodeWaitResponse struct {
	Waits []domain.NodeWait `json:"waits"`
}
type nodeWaitStore interface {
	RegisterNodeWait(context.Context, domain.NodeWaitRequest, string, string, time.Time) (domain.NodeWait, error)
	ListNodeWaits(context.Context) ([]domain.NodeWait, error)
	SettleNodeWaits(context.Context, time.Time) error
	TransitionNodeWake(context.Context, string, string, string, time.Time) (bool, error)
}

func (s *Service) NodeWait(ctx context.Context, principal Principal, op NodeWaitOperation) (NodeWaitResponse, error) {
	var result NodeWaitResponse
	if err := s.authorizer.Authorize(ctx, principal, Action{Kind: QueryKind("node-wait"), WorkflowRunID: op.Request.Target.RunID, TaskID: op.Request.Target.TaskID}); err != nil {
		return result, err
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
