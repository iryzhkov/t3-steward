package backlogadmin

import (
	"context"
	"errors"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// These operations retain the same administrator authorization as registration.
// The coordinator owns time, resumption and compare-and-swap delivery transitions.
type taskWaitRuntimeStore interface {
	SettleTaskWait(context.Context, string, domain.TaskWaitResult, time.Time) (domain.TaskWait, error)
	ExpireTaskWaits(context.Context, time.Time) ([]domain.TaskWait, error)
	WakeTaskWaits(context.Context, time.Time) ([]domain.TaskWaitWakeContext, error)
	TaskWakesAwaitingDelivery(context.Context, time.Time) ([]domain.TaskWaitWakeContext, error)
	TransitionTaskWake(context.Context, string, string, string, time.Time) (bool, error)
}

func (s *Service) taskWaitRuntime(ctx context.Context, op NodeWaitOperation) (NodeWaitResponse, error) {
	var result NodeWaitResponse
	store, ok := s.reader.(taskWaitRuntimeStore)
	if !ok {
		return result, errors.New("task-bound wait runtime unavailable")
	}
	var err error
	switch op.Action {
	case "settle-task":
		if op.ID == "" || op.Result == nil {
			return result, errors.New("task-bound wait ID and result required")
		}
		var record domain.TaskWait
		record, err = store.SettleTaskWait(ctx, op.ID, *op.Result, s.now())
		if err == nil {
			result.TaskWaits = []domain.TaskWait{record}
		}
	case "expire-task":
		result.TaskWaits, err = store.ExpireTaskWaits(ctx, s.now())
	case "wake-task":
		result.TaskWakes, err = store.WakeTaskWaits(ctx, s.now())
	case "pending-task":
		result.TaskWakes, err = store.TaskWakesAwaitingDelivery(ctx, s.now())
	case "transition-task":
		if op.ID == "" || op.From == "" || op.To == "" {
			return result, errors.New("task-bound wait ID and delivery transition required")
		}
		result.Changed, err = store.TransitionTaskWake(ctx, op.ID, op.From, op.To, s.now())
	default:
		err = errors.New("unknown task-bound wait runtime action")
	}
	return result, err
}
