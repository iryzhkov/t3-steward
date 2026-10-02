package main

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
	"os"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// A local poll remains on the worker. Only the coordinator records cross the
// configured administrator transport. Build the client at use so credential
// resolution errors are retried without disabling ordinary interactive waits.
var taskWaitWorkerHome = os.UserHomeDir

func configureTaskWaitTransport(runner *wait.Runner, cfg config.Config) error {
	runner.DisableTaskWaitRuntime = true
	runner.AssignedTaskWakesOnly = true
	runner.TaskWorkerID = cfg.BacklogV2.LocalWorker.ID
	if cfg.BacklogV2.CoordinatorClient.Configured() {
		runner.TaskStore = remoteTaskWaitStore{cfg: cfg}
		home, err := taskWaitWorkerHome()
		if err != nil {
			return err
		}
		bootstrap, _, err := workerruntime.LoadWorkerBootstrap(home)
		if err != nil {
			if runner.TaskWorkerID == "" || !errors.Is(err, os.ErrNotExist) {
				runner.TaskWorkerID = ""
				return err
			}
		} else {
			if bootstrap.CoordinatorID != cfg.BacklogV2.CoordinatorClient.CoordinatorID || (runner.TaskWorkerID != "" && runner.TaskWorkerID != bootstrap.WorkerID) {
				runner.TaskWorkerID = ""
				return errors.New("task wait worker bootstrap disagrees with configured worker or coordinator")
			}
			runner.TaskWorkerID = bootstrap.WorkerID
		}
	}
	runner.DisableTaskWaitRuntime = false
	return nil
}

type remoteTaskWaitStore struct{ cfg config.Config }

var _ wait.TaskWaitStore = remoteTaskWaitStore{}

func (s remoteTaskWaitStore) call(ctx context.Context, op backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
	transport, err := newCoordinatorTransport(s.cfg)
	if err != nil {
		return backlogadmin.NodeWaitResponse{}, err
	}
	return transport.client.NodeWait(ctx, op)
}
func (s remoteTaskWaitStore) SettleTaskWait(ctx context.Context, id string, result domain.TaskWaitResult, _ time.Time) (domain.TaskWait, error) {
	response, err := s.call(ctx, backlogadmin.NodeWaitOperation{Action: "settle-task", ID: id, Result: &result})
	if err != nil {
		return domain.TaskWait{}, err
	}
	if len(response.TaskWaits) != 1 {
		return domain.TaskWait{}, errors.New("coordinator did not return exactly one settled task wait")
	}
	return response.TaskWaits[0], nil
}
func (s remoteTaskWaitStore) ExpireTaskWaits(ctx context.Context, _ time.Time) ([]domain.TaskWait, error) {
	response, err := s.call(ctx, backlogadmin.NodeWaitOperation{Action: "expire-task"})
	return response.TaskWaits, err
}
func (s remoteTaskWaitStore) WakeTaskWaits(ctx context.Context, _ time.Time) ([]domain.TaskWaitWakeContext, error) {
	response, err := s.call(ctx, backlogadmin.NodeWaitOperation{Action: "wake-task"})
	return response.TaskWakes, err
}
func (s remoteTaskWaitStore) TaskWakesAwaitingDelivery(ctx context.Context, _ time.Time) ([]domain.TaskWaitWakeContext, error) {
	response, err := s.call(ctx, backlogadmin.NodeWaitOperation{Action: "pending-task"})
	return response.TaskWakes, err
}
func (s remoteTaskWaitStore) TransitionTaskWake(ctx context.Context, id, from, to string, _ time.Time) (bool, error) {
	response, err := s.call(ctx, backlogadmin.NodeWaitOperation{Action: "transition-task", ID: id, From: from, To: to})
	return response.Changed, err
}

// ListTaskWaits is the reconcile surface: the worker reads the coordinator's
// records to notice a bound wait settled without its check.
func (s remoteTaskWaitStore) ListTaskWaits(ctx context.Context) ([]domain.TaskWait, error) {
	response, err := s.call(ctx, backlogadmin.NodeWaitOperation{Action: "list-task"})
	return response.TaskWaits, err
}

var _ wait.TaskWaitLister = remoteTaskWaitStore{}

var _ wait.AskRelayStore = remoteTaskWaitStore{}

// AskRelayWork lists the asks this worker's steward relays.
func (s remoteTaskWaitStore) AskRelayWork(ctx context.Context, workerID string) ([]domain.TaskWait, error) {
	response, err := s.call(ctx, backlogadmin.NodeWaitOperation{Action: backlogadmin.AskRelayWorkAction, Worker: workerID})
	return response.TaskWaits, err
}

// RecordAskRelay records a relay thread's state on the coordinator.
func (s remoteTaskWaitStore) RecordAskRelay(ctx context.Context, waitID string, relay domain.AskRelay, _ time.Time) (domain.TaskWait, error) {
	response, err := s.call(ctx, backlogadmin.NodeWaitOperation{Action: backlogadmin.AskRelayRecordAction, ID: waitID, Relay: &relay})
	if err != nil {
		return domain.TaskWait{}, err
	}
	if len(response.TaskWaits) != 1 {
		return domain.TaskWait{}, errors.New("coordinator did not return exactly one ask")
	}
	return response.TaskWaits[0], nil
}

// AnswerAsk records an answer read from the relay thread. The coordinator
// takes the principal from the authenticated frame, never from here.
func (s remoteTaskWaitStore) AnswerAsk(ctx context.Context, answer domain.AskAnswer, _ string, _ bool, _ time.Time) (domain.TaskWait, error) {
	response, err := s.call(ctx, backlogadmin.NodeWaitOperation{Action: backlogadmin.AskAnswerAction, Answer: &answer})
	if err != nil {
		return domain.TaskWait{}, err
	}
	if len(response.TaskWaits) != 1 {
		return domain.TaskWait{}, errors.New("coordinator did not return exactly one ask")
	}
	return response.TaskWaits[0], nil
}
