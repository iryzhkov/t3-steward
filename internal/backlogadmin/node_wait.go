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
	// Host is the host whose steward delivers the wake of this registration.
	// A T3 thread exists only on the host that opened it, and a wake is sent by
	// the wait runner whose NodeHost matches the wait's host, so a registration
	// that does not state the calling host is delivered into the coordinator's
	// own T3, where the calling thread does not exist.
	//
	// It rides the operation rather than the request because the request is
	// compared field by field when a registration ID is replayed, and the
	// delivery host is a fact about who registered rather than about what was
	// registered. An empty host keeps the previous behaviour, the coordinator's
	// own hostname, which is what every client before v0.11.0-rc.71 meant and
	// what a coordinator-local client still means.
	//
	// On a "list" it means the other half of the same fact: keep only the waits
	// recorded for that host. A wait runner can act on no other, so asking for
	// them is asking the coordinator to encode and send history that is thrown
	// away on arrival.
	Host string `json:"host,omitempty"`
	// Undelivered narrows a "list" to the waits whose wake has still to be
	// delivered, dropping the delivered and cancelled ones.
	//
	// coordinator_node_waits is append-only: nothing deletes a row, so an
	// unfiltered list is the coordinator's entire node-wait history and it grows
	// for the life of the deployment. The wait runner of every host that is not
	// the coordinator asks for this list on every tick, so the unfiltered answer
	// is what would eventually meet the carrier's message limit.
	//
	// Both filters are optional, and an operation that states neither narrows
	// nothing, which is what every client before v0.11.0-rc.71 sends and what
	// "t3-steward wait list" still sends. An rc.70 coordinator decodes this
	// envelope with unknown fields disallowed and refuses either of them whole,
	// so a client that cannot be sure of the coordinator's release has to be
	// able to fall back to the unfiltered list rather than fail.
	Undelivered bool `json:"undelivered,omitempty"`
}

// NodeWaitTransitionAction moves a node wake through its delivery states on
// behalf of the host that sends it. It is named here rather than written twice
// because the host that asks and the coordinator that answers are different
// programs, and a spelling that drifted would fail only against a live
// coordinator.
const NodeWaitTransitionAction = "transition-node"

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
		// The calling host decides where the wake can be delivered, so it is
		// taken from the registration when the client stated one. Falling back
		// to this host is what every client that states nothing has always got.
		host := op.Host
		if host == "" {
			var err error
			if host, err = os.Hostname(); err != nil {
				return result, err
			}
		}
		w, err := store.RegisterNodeWait(ctx, op.Request, principal.ID, host, s.now())
		if err != nil {
			return result, err
		}
		result.Waits = []domain.NodeWait{w}
		return result, nil
	case NodeWaitTransitionAction:
		// The delivery state machine of a node wake, for the steward of the host
		// the wait names. That steward holds the thread and none of the
		// coordinator's records, so the claim that fences one send has to be
		// made here, exactly as "transition-task" fences a task wake.
		if op.ID == "" || op.From == "" || op.To == "" {
			return result, errors.New("a native wake transition needs the wait ID, its current delivery state and the next one")
		}
		changed, err := store.TransitionNodeWake(ctx, op.ID, op.From, op.To, s.now())
		if err != nil {
			return result, err
		}
		result.Changed = changed
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
			if op.Action == "list" && !listedNodeWait(w, op) {
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

// listedNodeWait applies the narrowing a list asked for. Both filters are
// optional and an operation that states neither keeps every wait, which is the
// answer every client before v0.11.0-rc.71 gets and the one "t3-steward wait
// list" still wants: an operator reading the coordinator's waits is reading its
// history on purpose.
//
// A wait runner is the opposite case. It can act only on the waits recorded for
// its own host whose delivery has not ended, so everything else in an
// unfiltered answer is encoded by the coordinator, carried over the admin
// transport and discarded on arrival, once per tick per host, from a table that
// only ever grows.
func listedNodeWait(w domain.NodeWait, op NodeWaitOperation) bool {
	if op.Host != "" && w.Host != op.Host {
		return false
	}
	if op.Undelivered && (w.Delivery == "delivered" || w.Delivery == "cancelled") {
		return false
	}
	return true
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
