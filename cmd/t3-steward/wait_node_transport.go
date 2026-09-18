package main

import (
	"context"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// configureNodeWaitTransport lets a host that is not the coordinator deliver
// the node wakes registered for it.
//
// A node wait is a coordinator record and its wake is a message into a T3
// thread, and a T3 thread exists only on the host that opened it. The two live
// on different machines whenever the caller is not on the coordinator, so the
// steward of the calling host reads the coordinator's rows over the same admin
// transport the CLI already uses, claims the ones its own host is named on, and
// sends the wake locally. This is the shape task wakes already have; see
// configureTaskWaitTransport.
//
// A coordinator host keeps its own store, which is the authority and needs no
// transport.
func configureNodeWaitTransport(runner *wait.Runner, cfg config.Config) {
	if cfg.BacklogV2.Mode == "coordinator" || !cfg.BacklogV2.CoordinatorClient.Configured() {
		return
	}
	runner.NodeStore = remoteNodeWaitStore{cfg: cfg}
}

// remoteNodeWaitStore is the coordinator's node-wait surface as seen from
// another host. Build the client at use, so a credential that cannot be
// resolved right now is retried on the next tick rather than disabling
// delivery for the life of the process.
type remoteNodeWaitStore struct{ cfg config.Config }

var _ wait.NodeStore = remoteNodeWaitStore{}

func (s remoteNodeWaitStore) call(ctx context.Context, op backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
	transport, err := newCoordinatorTransport(s.cfg)
	if err != nil {
		return backlogadmin.NodeWaitResponse{}, err
	}
	return transport.client.NodeWait(ctx, op)
}

// SettleNodeWaits does nothing here. Settling a node wait reads the
// coordinator's own run records and is its own tick's work; a second host
// asking for it would only ask the coordinator to do what it is already doing.
func (s remoteNodeWaitStore) SettleNodeWaits(context.Context, time.Time) error { return nil }

// ListNodeWaits reads every live registration. The runner keeps the ones its
// host is named on, which is the same filter it applies to a local store; no
// narrowing travels in the request, because a field this coordinator might not
// know would turn every tick into a refusal on a coordinator one release
// behind.
func (s remoteNodeWaitStore) ListNodeWaits(ctx context.Context) ([]domain.NodeWait, error) {
	response, err := s.call(ctx, backlogadmin.NodeWaitOperation{Action: "list"})
	return response.Waits, err
}

// TransitionNodeWake claims a wake before it is sent and records what became of
// it afterwards. A coordinator that does not know the action answers with an
// error, which is the honest outcome: it also never records another host on a
// wait, so it has none of this host's rows to hand out in the first place.
func (s remoteNodeWaitStore) TransitionNodeWake(ctx context.Context, id, from, to string, _ time.Time) (bool, error) {
	response, err := s.call(ctx, backlogadmin.NodeWaitOperation{Action: backlogadmin.NodeWaitTransitionAction, ID: id, From: from, To: to})
	return response.Changed, err
}
