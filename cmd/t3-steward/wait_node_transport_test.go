package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// Only a host that is not the coordinator reads node waits over a transport.
// The coordinator's own store is the authority, and a second reader of the same
// rows on the same machine would be one more way for a wake to be sent twice.
func TestOnlyANonCoordinatorHostReadsNodeWaitsOverTheTransport(t *testing.T) {
	client := config.V2CoordinatorClient{
		CoordinatorID: "normandy-coordinator", Address: "test.invalid",
		Connection: "ssh", Credential: "secretref:f03-admin/test",
	}
	for _, tc := range []struct {
		name   string
		mode   string
		client config.V2CoordinatorClient
		want   bool
	}{
		{"a worker that administers a coordinator", "worker", client, true},
		{"the coordinator itself", "coordinator", client, false},
		{"a host with no coordinator at all", "worker", config.V2CoordinatorClient{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Default()
			cfg.BacklogV2.Mode = tc.mode
			cfg.BacklogV2.CoordinatorClient = tc.client
			runner := wait.New(nil, nil, nil)
			configureNodeWaitTransport(runner, cfg, nil)
			if got := runner.NodeStore != nil; got != tc.want {
				t.Fatalf("node wait transport configured = %t, want %t", got, tc.want)
			}
		})
	}
}

// rc70NodeWaitEnvelope is the node-wait operation exactly as v0.11.0-rc.70
// declares it, which is what a coordinator one release behind decodes this
// request into, with unknown fields disallowed.
type rc70NodeWaitEnvelope struct {
	Action  string                       `json:"action"`
	Request domain.NodeWaitRequest       `json:"request"`
	ID      string                       `json:"id,omitempty"`
	Task    *domain.TaskWaitRegistration `json:"task,omitempty"`
	Result  *domain.TaskWaitResult       `json:"result,omitempty"`
	From    string                       `json:"from,omitempty"`
	To      string                       `json:"to,omitempty"`
}

// coordinatorNodeWaits is a coordinator's node-wait table as this host sees it:
// one wait it must deliver, one it already delivered, and one addressed to
// another host. Only the first is any of this host's business.
func coordinatorNodeWaits(host string) []domain.NodeWait {
	return []domain.NodeWait{
		{Request: domain.NodeWaitRequest{ID: "nw-live"}, Host: host, Delivery: "pending"},
		{Request: domain.NodeWaitRequest{ID: "nw-done"}, Host: host, Delivery: "delivered"},
		{Request: domain.NodeWaitRequest{ID: "nw-elsewhere"}, Host: "other-host", Delivery: "pending"},
	}
}

// answerAsCoordinator is the coordinator's side of a node-wait list: it applies
// the narrowing the operation asked for, as backlogadmin.Service does.
func answerAsCoordinator(op backlogadmin.NodeWaitOperation, host string) backlogadmin.NodeWaitResponse {
	var response backlogadmin.NodeWaitResponse
	for _, w := range coordinatorNodeWaits(host) {
		if op.Host != "" && w.Host != op.Host {
			continue
		}
		if op.Undelivered && (w.Delivery == "delivered" || w.Delivery == "cancelled") {
			continue
		}
		response.Waits = append(response.Waits, w)
	}
	return response
}

// The list a wait runner asks for is bounded by what it can act on. The
// coordinator's node-wait table is append-only and this list is issued on every
// tick by every host that is not the coordinator, so an unfiltered answer is the
// coordinator's whole history, encoded and carried once per tick for the life of
// the deployment.
func TestTheNodeWaitListIsBoundedToThisHostsUndeliveredWaits(t *testing.T) {
	var asked []backlogadmin.NodeWaitOperation
	store := &remoteNodeWaitStore{
		exchange: func(_ context.Context, op backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
			asked = append(asked, op)
			return answerAsCoordinator(op, "caller-host"), nil
		},
	}
	waits, err := store.ListNodeWaitsForHost(context.Background(), "caller-host")
	if err != nil {
		t.Fatal(err)
	}
	if len(asked) != 1 || asked[0].Action != "list" || asked[0].Host != "caller-host" || !asked[0].Undelivered {
		t.Fatalf("the runner asked for %+v, want one list narrowed to this host's undelivered waits", asked)
	}
	if len(waits) != 1 || waits[0].Request.ID != "nw-live" {
		t.Fatalf("the coordinator answered with %+v, want only nw-live", waits)
	}
}

// A coordinator that cannot apply the narrowing must still be usable. It
// decodes the operation envelope with unknown fields disallowed, so it refuses
// the narrowed list whole; this host then asks in the shape that coordinator
// understands and keeps delivering wakes, which is exactly what it did before
// the filter existed.
func TestAnOlderCoordinatorThatCannotNarrowTheListIsStillAnswered(t *testing.T) {
	var asked []backlogadmin.NodeWaitOperation
	store := &remoteNodeWaitStore{
		exchange: func(_ context.Context, op backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
			asked = append(asked, op)
			raw, err := json.Marshal(op)
			if err != nil {
				t.Fatal(err)
			}
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.DisallowUnknownFields()
			var old rc70NodeWaitEnvelope
			if err := decoder.Decode(&old); err != nil {
				return backlogadmin.NodeWaitResponse{}, &backlogadmin.TransportError{
					Class: backlogadmin.ClassProtocol, Operation: "node-wait", Coordinator: "normandy",
					Err: errors.New("invalid operation envelope: " + err.Error()),
				}
			}
			return answerAsCoordinator(backlogadmin.NodeWaitOperation{Action: old.Action}, "caller-host"), nil
		},
	}
	waits, err := store.ListNodeWaitsForHost(context.Background(), "caller-host")
	if err != nil {
		t.Fatalf("an older coordinator failed the list instead of degrading: %v", err)
	}
	if len(waits) != 3 {
		t.Fatalf("the fallback list returned %+v, want the whole table an rc.70 coordinator answers with", waits)
	}
	if len(asked) != 2 || !asked[0].Undelivered || asked[1].Undelivered || asked[1].Host != "" {
		t.Fatalf("the client asked %+v, want the narrowed list and then the rc.70 shape", asked)
	}

	// The refusal is a fact about that coordinator's release, so it is not
	// rediscovered on every tick: the next list is asked for in the shape this
	// coordinator can answer, and costs one round trip rather than two.
	if _, err := store.ListNodeWaitsForHost(context.Background(), "caller-host"); err != nil {
		t.Fatal(err)
	}
	if len(asked) != 3 || asked[2].Undelivered || asked[2].Host != "" {
		t.Fatalf("the client asked %+v, want one unfiltered list on the second tick", asked)
	}
}

// A protocol failure that is not a refusal of the request's shape is a real
// failure and is reported, rather than answered by asking the coordinator for
// more than was wanted.
func TestAFailedNodeWaitListIsNotRetriedUnfiltered(t *testing.T) {
	calls := 0
	store := &remoteNodeWaitStore{
		exchange: func(context.Context, backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
			calls++
			return backlogadmin.NodeWaitResponse{}, &backlogadmin.TransportError{
				Class: backlogadmin.ClassUnavailable, Operation: "node-wait", Coordinator: "normandy",
				Err: errors.New("coordinator exchange: connection refused"),
			}
		},
	}
	if _, err := store.ListNodeWaitsForHost(context.Background(), "caller-host"); err == nil {
		t.Fatal("an unreachable coordinator was reported as an empty wait list")
	}
	if calls != 1 {
		t.Fatalf("the client made %d calls, want one", calls)
	}
}
