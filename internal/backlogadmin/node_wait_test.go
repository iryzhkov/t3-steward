package backlogadmin

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type racingNativeStore struct{ *sqlite.Store }

func (s racingNativeStore) TransitionNodeWake(context.Context, string, string, string, time.Time) (bool, error) {
	return false, nil
}

type nativeTransport struct {
	*localTransportService
	native *Service
}

func (s nativeTransport) NodeWait(ctx context.Context, p Principal, op NodeWaitOperation) (NodeWaitResponse, error) {
	return s.native.NodeWait(ctx, p, op)
}

func TestNativeWaitAdminSocketRegistrationAndAuthorization(t *testing.T) {
	store := openAdminTestStore(t)
	seedAdminTestStore(t, store)
	auth := &allowAuthorizer{}
	service, err := New(store, auth)
	if err != nil {
		t.Fatal(err)
	}
	client, cancel, done := startLocalTransport(t, uint32(os.Getuid()), nativeTransport{&localTransportService{}, service})
	defer stopLocalTransport(t, cancel, done)
	op := NodeWaitOperation{Action: "register", Request: domain.NodeWaitRequest{ID: "nw-test", ThreadID: "thread", Name: "implement", Target: domain.NodeRef{RunID: "run-1", TaskID: "implement"}, Timeout: time.Hour}}
	response, err := client.NodeWait(context.Background(), op)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Waits) != 1 || response.Waits[0].Actor == "" || response.Waits[0].Request.Target.TaskID != "task-implement" {
		t.Fatalf("response=%+v", response)
	}
	again, err := client.NodeWait(context.Background(), op)
	if err != nil || len(again.Waits) != 1 {
		t.Fatal(again, err)
	}
	if _, err := client.NodeWait(context.Background(), NodeWaitOperation{Action: "cancel"}); err == nil {
		t.Fatal("empty cancel accepted")
	}
	waits, err := store.ListNodeWaits(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if waits[0].Delivery == "cancelled" {
		t.Fatal("empty cancel mutated waits")
	}
	// Exercise denial directly; the socket still supplies the trusted peer identity.
	denied, err := New(store, &allowAuthorizer{err: errors.New("denied")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := denied.NodeWait(context.Background(), Principal{}, op); err == nil {
		t.Fatal("unauthorized wait accepted")
	}
	racing, err := New(racingNativeStore{store}, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := racing.NodeWait(context.Background(), Principal{}, NodeWaitOperation{Action: "cancel", ID: "nw-test"}); err == nil {
		t.Fatal("lost cancellation race reported success")
	}
	if _, err := client.NodeWait(context.Background(), NodeWaitOperation{Action: "cancel", ID: "nw-test"}); err != nil {
		t.Fatal(err)
	}
}
