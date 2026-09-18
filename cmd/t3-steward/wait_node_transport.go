package main

import (
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
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
func configureNodeWaitTransport(runner *wait.Runner, cfg config.Config, log *slog.Logger) {
	if cfg.BacklogV2.Mode == "coordinator" || !cfg.BacklogV2.CoordinatorClient.Configured() {
		return
	}
	runner.NodeStore = &remoteNodeWaitStore{cfg: cfg, log: log}
}

// remoteNodeWaitStore is the coordinator's node-wait surface as seen from
// another host. Build the client at use, so a credential that cannot be
// resolved right now is retried on the next tick rather than disabling
// delivery for the life of the process.
type remoteNodeWaitStore struct {
	cfg config.Config
	log *slog.Logger
	// exchange is the transport seam. It is nil in the daemon, where the client
	// is built at use from the configuration; a test sets it to answer as a
	// coordinator of a chosen release would, which is the only way to state what
	// this host does when the coordinator refuses the narrowed list.
	exchange func(context.Context, backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error)
	// unfiltered records that this coordinator refused a narrowed list because
	// it does not know the fields that narrow it. The refusal is a property of
	// the coordinator's release, so it is remembered rather than rediscovered
	// on every tick; a coordinator that is upgraded underneath a running
	// steward is picked up when that steward is restarted, which is the same
	// restart the release notes already ask for.
	unfiltered atomic.Bool
}

var (
	_ wait.NodeStore     = (*remoteNodeWaitStore)(nil)
	_ wait.HostNodeStore = (*remoteNodeWaitStore)(nil)
)

func (s *remoteNodeWaitStore) call(ctx context.Context, op backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
	if s.exchange != nil {
		return s.exchange(ctx, op)
	}
	transport, err := newCoordinatorTransport(s.cfg)
	if err != nil {
		return backlogadmin.NodeWaitResponse{}, err
	}
	return transport.client.NodeWait(ctx, op)
}

// SettleNodeWaits does nothing here. Settling a node wait reads the
// coordinator's own run records and is its own tick's work; a second host
// asking for it would only ask the coordinator to do what it is already doing.
func (s *remoteNodeWaitStore) SettleNodeWaits(context.Context, time.Time) error { return nil }

// ListNodeWaitsForHost asks for the waits this host's runner can act on, and
// falls back to the whole list when the coordinator cannot narrow one.
//
// The narrowed form is what this call is for. coordinator_node_waits is
// append-only, so the unfiltered answer is the coordinator's entire node-wait
// history; asking for it once per tick from every non-coordinator host makes
// the cost of a wake grow with the age of the fleet, and it is the runner that
// knows the answer can be bounded.
//
// A coordinator that predates the filter decodes this envelope with unknown
// fields disallowed and refuses it whole. That is a refusal about the shape of
// the request rather than about the waits, so it is answered by asking again in
// the shape that coordinator understands: the behaviour degrades to what this
// host had before the filter existed instead of failing, and node wakes keep
// being delivered against an older coordinator.
func (s *remoteNodeWaitStore) ListNodeWaitsForHost(ctx context.Context, host string) ([]domain.NodeWait, error) {
	if host == "" || s.unfiltered.Load() {
		return s.ListNodeWaits(ctx)
	}
	response, err := s.call(ctx, backlogadmin.NodeWaitOperation{Action: "list", Host: host, Undelivered: true})
	if err == nil {
		return response.Waits, nil
	}
	if !refusedForAnUnknownField(err) {
		return nil, err
	}
	s.unfiltered.Store(true)
	if s.log != nil {
		s.log.Warn("this coordinator cannot narrow the node-wait list, so its whole node-wait history is read on every tick; restart this steward after the coordinator is upgraded",
			"host", host, "err", err)
	}
	return s.ListNodeWaits(ctx)
}

// ListNodeWaits reads every registration the coordinator holds. It is what the
// wait.NodeStore contract asks for and what an older coordinator can answer;
// the runner keeps the waits its host is named on, which is the same filter it
// applies to a local store.
func (s *remoteNodeWaitStore) ListNodeWaits(ctx context.Context) ([]domain.NodeWait, error) {
	response, err := s.call(ctx, backlogadmin.NodeWaitOperation{Action: "list"})
	return response.Waits, err
}

// refusedForAnUnknownField reports whether the coordinator refused the request
// because its decoder met a field it does not know, which is how a coordinator
// one release behind refuses a narrowing it cannot apply. Both carriers decode
// the operation envelope with unknown fields disallowed and report the refusal
// as a protocol-class error naming the field, so both conditions are required:
// any other protocol failure is a real one and is returned to the caller
// unchanged rather than answered by asking for more.
func refusedForAnUnknownField(err error) bool {
	return backlogadmin.ClassOf(err) == backlogadmin.ClassProtocol &&
		strings.Contains(err.Error(), "unknown field")
}

// TransitionNodeWake claims a wake before it is sent and records what became of
// it afterwards. A coordinator that does not know the action answers with an
// error, which is the honest outcome: it also never records another host on a
// wait, so it has none of this host's rows to hand out in the first place.
func (s *remoteNodeWaitStore) TransitionNodeWake(ctx context.Context, id, from, to string, _ time.Time) (bool, error) {
	response, err := s.call(ctx, backlogadmin.NodeWaitOperation{Action: backlogadmin.NodeWaitTransitionAction, ID: id, From: from, To: to})
	return response.Changed, err
}
