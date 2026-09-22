package sqlite

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Task-bound --wake all is scoped to the attempt, and the attempt's set must
// be all local kinds or all coordinator kinds: a coordinator kind cannot join
// a local all set and the other way round. The refusal names both members.
// Within one side the set works; each never groups and mixes freely.
func TestTaskBoundWakeAllRefusesMixedKindsAndNamesBothMembers(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	otherRun(t, store, now, domain.ProgressActive)
	local := taskWaitRegistration(attempt, "local-all", domain.WakeAll)
	first, err := store.RegisterTaskWait(ctx, local, now)
	if err != nil {
		t.Fatal(err)
	}
	parked := loadAttempt(t, store, attempt.ID)
	node := nodeRegistration(parked, "node-all", domain.NodeRef{RunID: "r2", TaskID: "deploy"}, domain.NodeStateTerminal)
	node.Wake = domain.WakeAll
	_, err = store.RegisterTaskWait(ctx, node, now)
	if err == nil {
		t.Fatal("a coordinator kind joined a local --wake all set")
	}
	for _, want := range []string{first.ID, "shell", "node", "local", "coordinator"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal does not name %q: %v", want, err)
		}
	}
	// Another local kind joins the local set.
	timed := taskWaitRegistration(parked, "time-all", domain.WakeAll)
	timed.Kind, timed.Condition = domain.WaitKindTime, "time at "+now.Add(time.Hour).UTC().Format(time.RFC3339)
	if _, err := store.RegisterTaskWait(ctx, timed, now); err != nil {
		t.Fatalf("a local kind was refused from a local all set: %v", err)
	}
	// An each wait of a coordinator kind is not a member of the set and is
	// accepted beside it.
	each := nodeRegistration(loadAttempt(t, store, attempt.ID), "node-each", domain.NodeRef{RunID: "r2", TaskID: "deploy"}, domain.NodeStateTerminal)
	if _, err := store.RegisterTaskWait(ctx, each, now); err != nil {
		t.Fatalf("an each wait was refused beside an all set: %v", err)
	}

	// The other way round: a coordinator all set refuses a local member.
	store2, attempt2, now2 := taskWaitFixture(t)
	otherRun(t, store2, now2, domain.ProgressActive)
	nodeAll := nodeRegistration(attempt2, "node-all", domain.NodeRef{RunID: "r2", TaskID: "deploy"}, domain.NodeStateTerminal)
	nodeAll.Wake = domain.WakeAll
	if _, err := store2.RegisterTaskWait(ctx, nodeAll, now2); err != nil {
		t.Fatal(err)
	}
	shellAll := taskWaitRegistration(loadAttempt(t, store2, attempt2.ID), "shell-all", domain.WakeAll)
	if _, err := store2.RegisterTaskWait(ctx, shellAll, now2); err == nil || !strings.Contains(err.Error(), "tw-node-all") {
		t.Fatalf("a local kind joined a coordinator all set: %v", err)
	}
	below := 10.0
	quotaAll := quotaRegistration(loadAttempt(t, store2, attempt2.ID), "quota-all", domain.QuotaWaitCondition{Pool: "claude", Below: &below})
	quotaAll.Wake = domain.WakeAll
	quotaFixture(t, store2, now2, domain.PhaseStopped, 95)
	if _, err := store2.RegisterTaskWait(ctx, quotaAll, now2); err != nil {
		t.Fatalf("a coordinator kind was refused from a coordinator all set: %v", err)
	}
}

// Interactive node waits group like local waits do: with --group and --wake
// all the thread is woken once, when every member has settled, with one
// message that carries every member.
func TestInteractiveNodeWaitsGroupWithWakeAll(t *testing.T) {
	ctx := context.Background()
	store, _, now := taskWaitFixture(t)
	otherRun(t, store, now, domain.ProgressActive)
	for _, id := range []string{"nw-a", "nw-b"} {
		request := domain.NodeWaitRequest{ID: id, ThreadID: "thread-1", Name: id, Target: domain.NodeRef{RunID: "r2", TaskID: "deploy"}, Timeout: time.Hour, Group: "pair", Wake: domain.WakeAll}
		if id == "nw-b" {
			request.State = domain.NodeStateActive
		}
		if _, err := store.RegisterNodeWait(ctx, request, "operator", "host", now); err != nil {
			t.Fatal(err)
		}
	}
	// nw-b (active) settled at registration; nw-a (terminal) has not. No wake.
	clock := now.Add(time.Minute)
	runner, control := fleetRunner(t, store, &clock, nil)
	runner.NodeHost = "host"
	runner.Tick(ctx, nil, nil)
	if len(control.sends) != 0 {
		t.Fatalf("woken with one of two settled: %v", control.texts)
	}
	otherRun(t, store, now, domain.ProgressSucceeded)
	clock = now.Add(2 * time.Minute)
	runner.Tick(ctx, nil, nil)
	if len(control.sends) != 1 {
		t.Fatalf("sends = %v texts=%v", control.sends, control.texts)
	}
	text := control.texts[0]
	if !strings.HasPrefix(text, "t3-steward-wait kind=node outcome=met wait=nw-a") || !strings.Contains(text, "count=2") || !strings.Contains(text, "nw-b") {
		t.Fatalf("the group wake does not carry both members: %q", text)
	}
	// The sender resolves its own delivery by observing the message, as every
	// node wake does; the other member rode in it and is delivered with it.
	clock = now.Add(3 * time.Minute)
	runner.Tick(ctx, nil, nil)
	if len(control.sends) != 1 {
		t.Fatalf("a second message was sent: %v", control.sends)
	}
	waits, _ := store.ListNodeWaits(ctx)
	for _, w := range waits {
		if w.Delivery != "delivered" {
			t.Fatalf("member %s is %s after the group wake", w.Request.ID, w.Delivery)
		}
	}
}

func TestNodeWakeClaimFreezesPayloadMembershipAndSingleSender(t *testing.T) {
	ctx := context.Background()
	store, _, now := taskWaitFixture(t)
	otherRun(t, store, now, domain.ProgressSucceeded)
	for _, id := range []string{"nw-a", "nw-b"} {
		request := domain.NodeWaitRequest{ID: id, ThreadID: "thread-1", Name: id, Target: domain.NodeRef{RunID: "r2", TaskID: "deploy"}, Timeout: time.Hour}
		if _, err := store.RegisterNodeWait(ctx, request, "operator", "host", now); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.SettleNodeWaits(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	members := []string{"nw-a", "nw-b"}
	type claimResult struct {
		won bool
		err error
	}
	start := make(chan struct{})
	results := make(chan claimResult, 2)
	for range 2 {
		go func() {
			<-start
			won, err := store.ClaimNodeWakeGroup(ctx, "nw-a", "pending", "delivery-a", "frozen bytes", members, now.Add(time.Minute))
			results <- claimResult{won: won, err: err}
		}()
	}
	close(start)
	winners := 0
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.won {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("concurrent claim winners = %d, want 1", winners)
	}
	waits, err := store.ListNodeWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, wake := range waits {
		if wake.Delivery != "sending" || wake.DeliveryID != "delivery-a" || wake.DeliveryPayload != "frozen bytes" ||
			wake.DeliveryPayloadDigest == "" || !reflect.DeepEqual(wake.DeliveryGroupMembers, members) || wake.DeliveryAttempts != 1 {
			t.Fatalf("wake was not frozen atomically: %+v", wake)
		}
	}
	if ok, err := store.TransitionNodeWake(ctx, "nw-a", "sending", "offline", now.Add(2*time.Minute)); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if ok, err := store.ClaimNodeWakeGroup(ctx, "nw-a", "offline", "delivery-a", "changed bytes", members, now.Add(4*time.Minute)); err == nil || ok || !strings.Contains(err.Error(), "frozen delivery differs") {
		t.Fatalf("changed replay = %v, %v", ok, err)
	}
}
