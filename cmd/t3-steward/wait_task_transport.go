package main

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
	"os"
	"path/filepath"
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

// errNoTaskWorkerIdentity is the configuration that names no worker on this
// host: no worker bootstrap and no backlog_v2.local_worker.id. Task wakes and
// ask relays are delivered only by the steward of the worker that runs the
// task, so a task running on such a host is never woken.
var errNoTaskWorkerIdentity = errors.New("no worker identity on this host (no ~/.config/t3-steward/worker-bootstrap.json and no backlog_v2.local_worker.id): " +
	"task wakes and ask relays for tasks running on this host are not delivered")

// resolveTaskWaitWorker establishes which worker this host's steward speaks
// for: the worker bootstrap, checked against the configured local worker and
// against the coordinator, which is the coordinator client's on a worker and
// this host's own on the coordinator host.
func resolveTaskWaitWorker(cfg config.Config) (string, error) {
	configured := cfg.BacklogV2.LocalWorker.ID
	home, err := taskWaitWorkerHome()
	if err != nil {
		return "", err
	}
	bootstrap, _, err := workerruntime.LoadWorkerBootstrap(home)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		if configured == "" {
			return "", errNoTaskWorkerIdentity
		}
		return configured, nil
	}
	coordinator := cfg.BacklogV2.Coordinator.ID
	if cfg.BacklogV2.CoordinatorClient.Configured() {
		coordinator = cfg.BacklogV2.CoordinatorClient.CoordinatorID
	}
	if (coordinator != "" && bootstrap.CoordinatorID != coordinator) || (configured != "" && configured != bootstrap.WorkerID) {
		return "", errors.New("task wait worker bootstrap disagrees with configured worker or coordinator")
	}
	return bootstrap.WorkerID, nil
}

// configureTaskWaitTransport binds this host's wait runner to its worker. A
// worker host reaches the coordinator's records over its coordinator client;
// the coordinator host, which configures no client, keeps its own store.
//
// A worker host without an identity fences the task-wait runtime, because it
// holds none of the coordinator's records and could only act for the wrong
// worker. The coordinator host keeps it running without one: settling,
// expiring and committing wakes are the coordinator's own duties, and only
// delivery into this host's threads needs the identity. Either way the error
// is returned for the caller to report.
func configureTaskWaitTransport(runner *wait.Runner, cfg config.Config) error {
	runner.DisableTaskWaitRuntime = true
	runner.AssignedTaskWakesOnly = true
	workerID, err := resolveTaskWaitWorker(cfg)
	runner.TaskWorkerID = workerID
	if cfg.BacklogV2.CoordinatorClient.Configured() {
		runner.TaskStore = remoteTaskWaitStore{cfg: cfg}
		if err != nil {
			runner.TaskWorkerID = ""
			return err
		}
	}
	runner.DisableTaskWaitRuntime = false
	if err != nil {
		runner.TaskWorkerID = ""
		return err
	}
	return nil
}

// checkTaskWaitIdentity is the check line for the same question: whether the
// tasks this host runs can be woken and their asks relayed. A host that is
// neither a coordinator nor a configured worker runs no tasks. A coordinator
// client selects an admin transport, not a worker role.
func checkTaskWaitIdentity(cfg config.Config) (string, string) {
	workerID, err := resolveTaskWaitWorker(cfg)
	switch {
	case err == nil:
		return "ok", "task wakes and ask relays for tasks on this host: delivered here as worker " + workerID
	case errors.Is(err, errNoTaskWorkerIdentity) && cfg.BacklogV2.Coordinator.ID == "":
		// The resolver already checked the bootstrap and local worker ID.
		// A persistent worker configuration still declares a worker host even
		// when its identity is missing. Only confirmed absence is harmless.
		home, homeErr := taskWaitWorkerHome()
		if homeErr != nil {
			return "FAIL", "task wakes and ask relays: " + homeErr.Error()
		}
		_, statErr := os.Stat(filepath.Join(home, ".config/t3-steward/persistent-worker.yaml"))
		if errors.Is(statErr, os.ErrNotExist) {
			return "ok", "task wakes and ask relays: nothing to deliver on this host (not a worker)"
		}
		if statErr != nil {
			return "FAIL", "task wakes and ask relays: " + statErr.Error()
		}
	}
	return "FAIL", "task wakes and ask relays: " + err.Error()
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

var _ wait.NativeInputStore = remoteTaskWaitStore{}

// RecordNativeUserInput reports a task thread's native question activity.
func (s remoteTaskWaitStore) RecordNativeUserInput(ctx context.Context, workerID, attemptID, threadID string, events []domain.UserInputEvent, _ time.Time) (int, error) {
	_, err := s.call(ctx, backlogadmin.NodeWaitOperation{Action: backlogadmin.NativeInputAction,
		Worker: workerID, ID: attemptID, ThreadID: threadID, Events: events})
	return len(events), err
}

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
