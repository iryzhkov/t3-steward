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
