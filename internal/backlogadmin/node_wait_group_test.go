package backlogadmin

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type nodeClaimlessStore struct {
	Reader
	nodeWaitStore
}

func nodeClaimFixture(t *testing.T) (*Service, *sqlite.Store, NodeWaitOperation) {
	t.Helper()
	store := openAdminTestStore(t)
	seedAdminTestStore(t, store)
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return adminTestNow }
	for _, id := range []string{"a", "b", "c"} {
		_, err := service.NodeWait(context.Background(), Principal{ID: "tester"}, NodeWaitOperation{Action: "register",
			Request: domain.NodeWaitRequest{ID: id, ThreadID: "thread", Target: domain.NodeRef{RunID: "run-1", TaskID: "implement"}, Timeout: time.Hour}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := store.SettleNodeWaits(context.Background(), adminTestNow.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	return service, store, NodeWaitOperation{Action: NodeWaitClaimGroupAction, ID: "a", From: "pending", DeliveryID: "group", Payload: "frozen bytes", Members: []string{"a", "b", "c"}}
}

func TestNodeWaitClaimGroupActionFreezesEveryMember(t *testing.T) {
	service, store, op := nodeClaimFixture(t)
	ctx := context.Background()
	answer, err := service.NodeWait(ctx, Principal{}, op)
	if err != nil || !answer.Changed {
		t.Fatal(answer, err)
	}
	listed, err := service.NodeWait(ctx, Principal{}, NodeWaitOperation{Action: "list"})
	if err != nil || len(listed.Waits) != 3 {
		t.Fatal(listed, err)
	}
	for _, w := range listed.Waits {
		if w.SettledAt == nil || w.Delivery != "sending" || w.DeliveryID != op.DeliveryID || w.DeliveryPayload != op.Payload ||
			w.DeliveryPayloadDigest != fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(op.Payload))) || !reflect.DeepEqual(w.DeliveryGroupMembers, op.Members) {
			t.Fatalf("not frozen: %+v", w)
		}
	}
	claimless, _ := New(nodeClaimlessStore{service.reader, store}, &allowAuthorizer{})
	if _, err := claimless.NodeWait(ctx, Principal{}, op); err == nil || err.Error() != "native wait group claim unavailable" {
		t.Fatal("missing group-claim capability", err)
	}
	denied, _ := New(service.reader, &allowAuthorizer{err: fmt.Errorf("denied")})
	if _, err := denied.NodeWait(ctx, Principal{}, op); err == nil {
		t.Fatal("unauthorized claim accepted")
	}
}

func TestNodeWaitClaimGroupActionRefusesMalformed(t *testing.T) {
	service, store, valid := nodeClaimFixture(t)
	ctx := context.Background()
	before, _ := store.ListNodeWaits(ctx)
	for _, change := range []func(*NodeWaitOperation){
		func(o *NodeWaitOperation) { o.ID = "" },
		func(o *NodeWaitOperation) { o.From = "" },
		func(o *NodeWaitOperation) { o.DeliveryID = "" },
		func(o *NodeWaitOperation) { o.Payload = "" },
		func(o *NodeWaitOperation) { o.Members = nil },
		func(o *NodeWaitOperation) { o.Members = []string{"b", "c"} },
		func(o *NodeWaitOperation) { o.Members = []string{"a", "a"} },
	} {
		op := valid
		change(&op)
		if _, err := service.NodeWait(ctx, Principal{}, op); err == nil {
			t.Fatalf("accepted %+v", op)
		}
		after, err := store.ListNodeWaits(ctx)
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatal("malformed claim mutated rows", err)
		}
	}
}

func TestNodeWaitClaimGroupActionReplay(t *testing.T) {
	service, store, op := nodeClaimFixture(t)
	ctx := context.Background()
	if answer, err := service.NodeWait(ctx, Principal{}, op); err != nil || !answer.Changed {
		t.Fatal(answer, err)
	}
	if answer, err := service.NodeWait(ctx, Principal{}, op); err != nil || answer.Changed {
		t.Fatal("sending replay", answer, err)
	}
	for _, id := range op.Members {
		if changed, err := store.TransitionNodeWake(ctx, id, "sending", "offline", adminTestNow); err != nil || !changed {
			t.Fatal(changed, err)
		}
	}
	op.From = "offline"
	for _, change := range []func(*NodeWaitOperation){
		func(o *NodeWaitOperation) { o.Payload += "changed" },
		func(o *NodeWaitOperation) { o.Members = []string{"a", "b"} },
	} {
		different := op
		change(&different)
		if _, err := service.NodeWait(ctx, Principal{}, different); err == nil || !strings.Contains(err.Error(), "frozen delivery differs") {
			t.Fatal(err)
		}
	}
	if answer, err := service.NodeWait(ctx, Principal{}, op); err != nil || !answer.Changed {
		t.Fatal("identical replay", answer, err)
	}
}
