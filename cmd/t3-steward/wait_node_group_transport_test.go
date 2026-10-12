package main

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

func TestRemoteNodeWaitStoreClaimsGroupOverTheTransport(t *testing.T) {
	ctx, now := context.Background(), time.Now()
	db, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{{
		ID: "run-1", WorkflowID: "wf", Progress: domain.ProgressSucceeded, CreatedAt: now, UpdatedAt: now,
		Sink: &domain.SinkTask{ID: "sink", Progress: domain.ProgressSucceeded},
	}}}); err != nil {
		t.Fatal(err)
	}
	service, err := backlogadmin.New(db, localAdminAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	members := []string{"a", "b", "c"}
	for _, id := range members {
		_, err := db.RegisterNodeWait(ctx, domain.NodeWaitRequest{ID: id, ThreadID: "thread", Target: domain.NodeRef{RunID: "run-1", TaskID: "sink"}, Timeout: time.Hour}, "tester", "host", now)
		if err != nil {
			t.Fatal(err)
		}
	}
	remote := &remoteNodeWaitGroupStore{remoteNodeWaitStore: &remoteNodeWaitStore{
		exchange: func(ctx context.Context, op backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
			raw, err := json.Marshal(op)
			if err != nil {
				t.Fatal(err)
			}
			var wire backlogadmin.NodeWaitOperation
			if err := json.Unmarshal(raw, &wire); err != nil {
				t.Fatal(err)
			}
			return service.NodeWait(ctx, backlogadmin.Principal{ID: "tester", Roles: []string{backlogadmin.RemoteAdminRole}}, wire)
		},
	}}
	if changed, err := remote.ClaimNodeWakeGroup(ctx, "a", "pending", "group", "payload", members, now); err != nil || !changed {
		t.Fatal(changed, err)
	}
	listed, err := remote.ListNodeWaitsForHost(ctx, "host")
	if err != nil || len(listed) != 3 {
		t.Fatal(listed, err)
	}
	for _, w := range listed {
		if w.Delivery != "sending" || w.DeliveryID != "group" || w.DeliveryPayload != "payload" || w.DeliveryPayloadDigest == "" || !reflect.DeepEqual(w.DeliveryGroupMembers, members) {
			t.Fatal(w)
		}
	}
}

func TestRemoteNodeWaitStoreWithoutOptInIsNotAGroupClaimer(t *testing.T) {
	cfg := config.Default()
	cfg.BacklogV2.Mode = "worker"
	cfg.BacklogV2.CoordinatorClient = config.V2CoordinatorClient{CoordinatorID: "c", Address: "test.invalid", Connection: "ssh", Credential: "secretref:test/admin"}
	runner := wait.New(nil, nil, nil)
	configureNodeWaitTransport(runner, cfg, nil)
	if runner.NodeStore == nil {
		t.Fatal("transport missing")
	}
	if _, ok := runner.NodeStore.(wait.NodeGroupClaimer); ok {
		t.Fatal("default opted in")
	}
	configureNodeWaitTransport(runner, cfg, nil, true)
	if _, ok := runner.NodeStore.(wait.NodeGroupClaimer); !ok {
		t.Fatal("opt-in missing")
	}
}

func TestOlderCoordinatorRefusingTheGroupClaimIsRemembered(t *testing.T) {
	for _, refusal := range []error{
		&backlogadmin.TransportError{Class: backlogadmin.ClassProtocol, Err: errors.New("unknown field \"payload\"")},
		errors.New("unknown native wait action"),
	} {
		calls := 0
		remote := &remoteNodeWaitGroupStore{remoteNodeWaitStore: &remoteNodeWaitStore{exchange: func(context.Context, backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
			calls++
			return backlogadmin.NodeWaitResponse{}, refusal
		}}}
		for i := 0; i < 2; i++ {
			changed, err := remote.ClaimNodeWakeGroup(context.Background(), "a", "pending", "group", "payload", []string{"a"}, time.Now())
			if changed || !errors.Is(err, wait.ErrNodeGroupClaimUnsupported) {
				t.Fatal(changed, err)
			}
		}
		if calls != 1 {
			t.Fatalf("claims sent %d times", calls)
		}
	}
	failure := errors.New("unreachable")
	remote := &remoteNodeWaitGroupStore{remoteNodeWaitStore: &remoteNodeWaitStore{exchange: func(context.Context, backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
		return backlogadmin.NodeWaitResponse{}, failure
	}}}
	if _, err := remote.ClaimNodeWakeGroup(context.Background(), "a", "pending", "group", "payload", []string{"a"}, time.Now()); err != failure || remote.unsupported.Load() {
		t.Fatal(err)
	}
}
