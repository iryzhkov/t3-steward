package backlogadmin

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// rc70NodeWaitOperation is the node-wait operation exactly as v0.11.0-rc.70
// (644221e) declares it. Both transports decode this envelope with unknown
// fields disallowed -- the owner-only local socket in local_transport.go and
// the remote carrier in remote_server.go -- so an operation carrying a field
// outside this set is refused whole by an rc.70 coordinator, and a client one
// release ahead that always sent it could start nothing at all.
type rc70NodeWaitOperation struct {
	Action  string                       `json:"action"`
	Request domain.NodeWaitRequest       `json:"request"`
	ID      string                       `json:"id,omitempty"`
	Task    *domain.TaskWaitRegistration `json:"task,omitempty"`
	Result  *domain.TaskWaitResult       `json:"result,omitempty"`
	From    string                       `json:"from,omitempty"`
	To      string                       `json:"to,omitempty"`
}

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

// A wake is delivered by the wait runner whose host matches the wait's, into
// that host's own T3. A client that is not on the coordinator therefore has to
// be able to say which host holds the thread, and the coordinator has to record
// what it was told rather than its own name.
func TestARegistrationRecordsTheCallingHostAndNotTheCoordinatorsOwn(t *testing.T) {
	store := openAdminTestStore(t)
	seedAdminTestStore(t, store)
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	request := domain.NodeWaitRequest{ID: "nw-caller", ThreadID: "thread", Name: "implement", Target: domain.NodeRef{RunID: "run-1", TaskID: "implement"}, Timeout: time.Hour}
	response, err := service.NodeWait(ctx, Principal{ID: "tester"}, NodeWaitOperation{Action: "register", Request: request, Host: "caller-host"})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Waits) != 1 || response.Waits[0].Host != "caller-host" {
		t.Fatalf("the registration was recorded for %+v, want the calling host caller-host", response.Waits)
	}
	if coordinator == "caller-host" {
		t.Fatal("this machine is called caller-host, so the test cannot tell the two hosts apart")
	}

	// A registration that names no host is what every client before this
	// release sends, and it still records the coordinator's own host.
	legacy := request
	legacy.ID = "nw-legacy"
	response, err = service.NodeWait(ctx, Principal{ID: "tester"}, NodeWaitOperation{Action: "register", Request: legacy})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Waits) != 1 || response.Waits[0].Host != coordinator {
		t.Fatalf("a registration that names no host was recorded for %+v, want %s", response.Waits, coordinator)
	}
}

// The compatibility hazard in both directions, stated rather than assumed.
//
// Forward: a registration that states no calling host is byte for byte the
// rc.70 shape, so a client of this release keeps working against a coordinator
// one release behind, and the calling host is the only thing it gives up.
// Backward: a registration from an rc.70 client carries no host, and this
// release's coordinator records its own hostname for it, which is exactly what
// rc.70 did.
//
// The host field itself is refused by an rc.70 coordinator, which is why it is
// sent only to a coordinator whose release is known to be new enough.
func TestARegistrationWithoutTheCallingHostIsTheRC70Shape(t *testing.T) {
	operation := NodeWaitOperation{
		Action:  "register",
		Request: domain.NodeWaitRequest{ID: "nw-1", ThreadID: "thread", Name: "run-1/__sink", Timeout: time.Hour},
	}
	raw, err := json.Marshal(operation)
	if err != nil {
		t.Fatal(err)
	}
	var old rc70NodeWaitOperation
	if err := decodeStrict(raw, &old); err != nil {
		t.Fatalf("a registration that states no calling host carries a field rc.70 refuses: %v\n%s", err, raw)
	}
	if old.Action != "register" || old.Request.ID != "nw-1" {
		t.Fatalf("rc.70 reads the registration as %+v", old)
	}

	operation.Host = "caller-host"
	if raw, err = json.Marshal(operation); err != nil {
		t.Fatal(err)
	}
	err = decodeStrict(raw, &old)
	if err == nil || !strings.Contains(err.Error(), `unknown field "host"`) {
		t.Fatalf("strict rc.70 decode of a registration that states the calling host: %v, want a refusal naming host", err)
	}

	// The transitions the delivering host sends carry no new field at all, so
	// an rc.70 coordinator refuses them for the action it does not have rather
	// than for a shape it cannot read. Its error is a plain one the runner
	// logs, not a refusal of anything the caller asked for.
	if raw, err = json.Marshal(NodeWaitOperation{Action: "transition-node", ID: "nw-1", From: "pending", To: "sending"}); err != nil {
		t.Fatal(err)
	}
	if err := decodeStrict(raw, &old); err != nil {
		t.Fatalf("a wake transition carries a field rc.70 refuses: %v\n%s", err, raw)
	}

	// The same hazard governs the narrowed list. An rc.70 coordinator refuses it
	// whole rather than answering it unnarrowed, which is why the client has to
	// be able to ask again in the shape below rather than treat the refusal as a
	// failure of the list.
	if raw, err = json.Marshal(NodeWaitOperation{Action: "list", Host: "caller-host", Undelivered: true}); err != nil {
		t.Fatal(err)
	}
	err = decodeStrict(raw, &old)
	if err == nil || !strings.Contains(err.Error(), `unknown field "`) {
		t.Fatalf("strict rc.70 decode of a narrowed list: %v, want a refusal naming the field", err)
	}
	if raw, err = json.Marshal(NodeWaitOperation{Action: "list"}); err != nil {
		t.Fatal(err)
	}
	if err := decodeStrict(raw, &old); err != nil {
		t.Fatalf("the unnarrowed list carries a field rc.70 refuses: %v\n%s", err, raw)
	}
}

// The list a wait runner asks for is narrowed by the coordinator, because only
// the coordinator can narrow it: coordinator_node_waits is append-only, nothing
// deletes a row, and the answer travels over the admin carrier once per tick
// from every host that is not the coordinator. A runner can act on no wait but
// its own host's, and on none whose delivery has ended.
func TestANodeWaitListIsNarrowedToTheHostAndTheWaitsStillToDeliver(t *testing.T) {
	store := openAdminTestStore(t)
	seedAdminTestStore(t, store)
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	principal := Principal{ID: "tester"}
	register := func(id, host string) domain.NodeWait {
		t.Helper()
		response, err := service.NodeWait(ctx, principal, NodeWaitOperation{
			Action: "register", Host: host,
			Request: domain.NodeWaitRequest{ID: id, ThreadID: "thread", Name: "implement",
				Target: domain.NodeRef{RunID: "run-1", TaskID: "implement"}, Timeout: time.Hour},
		})
		if err != nil {
			t.Fatal(err)
		}
		return response.Waits[0]
	}
	live := register("nw-live", "caller-host")
	over := register("nw-over", "caller-host")
	register("nw-elsewhere", "other-host")
	if _, err := service.NodeWait(ctx, principal, NodeWaitOperation{
		Action: NodeWaitTransitionAction, ID: over.Request.ID, From: over.Delivery, To: "cancelled",
	}); err != nil {
		t.Fatal(err)
	}

	narrowed, err := service.NodeWait(ctx, principal, NodeWaitOperation{Action: "list", Host: "caller-host", Undelivered: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(narrowed.Waits) != 1 || narrowed.Waits[0].Request.ID != live.Request.ID {
		t.Fatalf("the narrowed list answered with %+v, want only %s", narrowed.Waits, live.Request.ID)
	}

	// A list that narrows nothing is what every client before v0.11.0-rc.71
	// sends and what "t3-steward wait list" still sends: the whole table.
	all, err := service.NodeWait(ctx, principal, NodeWaitOperation{Action: "list"})
	if err != nil {
		t.Fatal(err)
	}
	if len(all.Waits) != 3 {
		t.Fatalf("an unfiltered list answered with %d waits, want all 3", len(all.Waits))
	}

	// The filters belong to "list" alone. A cancellation names one wait by ID
	// and must not be narrowed away by a host that is not its own.
	cancelled, err := service.NodeWait(ctx, principal, NodeWaitOperation{Action: "cancel", ID: "nw-elsewhere", Host: "caller-host", Undelivered: true})
	if err != nil || len(cancelled.Waits) != 1 || cancelled.Waits[0].Delivery != "cancelled" {
		t.Fatalf("cancelling a wait of another host answered %+v: %v", cancelled.Waits, err)
	}
}

// The steward of the calling host claims a wake before it sends it and records
// what became of it afterwards. It holds none of the coordinator's records, so
// that transition is an operation of its own, carrying the store's fence and
// its refusals unchanged.
func TestTheDeliveringHostCanTransitionANodeWake(t *testing.T) {
	store := openAdminTestStore(t)
	seedAdminTestStore(t, store)
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	registered, err := service.NodeWait(ctx, Principal{ID: "tester"}, NodeWaitOperation{
		Action: "register", Host: "caller-host",
		Request: domain.NodeWaitRequest{ID: "nw-transition", ThreadID: "thread", Name: "implement", Target: domain.NodeRef{RunID: "run-1", TaskID: "implement"}, Timeout: time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	from := registered.Waits[0].Delivery
	// The store's own fence still decides. This wait is not settled, so a claim
	// to send it is refused here exactly as it is on the coordinator's host,
	// rather than being answered as an unremarkable "nothing changed".
	if _, err := service.NodeWait(ctx, Principal{ID: "tester"}, NodeWaitOperation{Action: "transition-node", ID: "nw-transition", From: from, To: "sending"}); err == nil {
		t.Fatal("a send was claimed for a wait that has not settled")
	}
	response, err := service.NodeWait(ctx, Principal{ID: "tester"}, NodeWaitOperation{Action: "transition-node", ID: "nw-transition", From: from, To: "cancelled"})
	if err != nil || !response.Changed {
		t.Fatalf("the transition did not take: changed=%t err=%v", response.Changed, err)
	}
	// The same transition a second time is a lost race, reported as unchanged
	// rather than as a second delivery.
	response, err = service.NodeWait(ctx, Principal{ID: "tester"}, NodeWaitOperation{Action: "transition-node", ID: "nw-transition", From: from, To: "cancelled"})
	if err != nil || response.Changed {
		t.Fatalf("a repeated transition reported changed=%t err=%v", response.Changed, err)
	}
	for _, incomplete := range []NodeWaitOperation{
		{Action: "transition-node", From: "sending", To: "delivered"},
		{Action: "transition-node", ID: "nw-transition", To: "delivered"},
		{Action: "transition-node", ID: "nw-transition", From: "sending"},
	} {
		if _, err := service.NodeWait(ctx, Principal{ID: "tester"}, incomplete); err == nil {
			t.Fatalf("an incomplete transition was accepted: %+v", incomplete)
		}
	}
}
