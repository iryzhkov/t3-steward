package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// An interactive group is all local kinds or all coordinator kinds. A local
// registration into a group that already holds a coordinator wait on this
// thread is refused, naming both members, before anything is registered.
func TestInteractiveGroupRefusesMixingLocalAndCoordinatorKinds(t *testing.T) {
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	local := []wait.Wait{{ID: "w-1", ThreadID: "thread-1", Kind: domain.WaitKindShell, Group: "deploy", Wake: wait.WakeAll, Status: wait.StatusWaiting, CreatedAt: now}}
	native := []domain.NodeWait{{Request: domain.NodeWaitRequest{ID: "nw-1", ThreadID: "thread-1", Group: "deploy", Wake: domain.WakeAll, Target: domain.NodeRef{RunID: "r", TaskID: "t"}}}}
	err := refuseMixedGroup("thread-1", "deploy", domain.WaitKindNode, local, nil)
	if err == nil || !strings.Contains(err.Error(), "w-1") || !strings.Contains(err.Error(), "shell") || !strings.Contains(err.Error(), "node") {
		t.Fatalf("a coordinator kind joined a local group: %v", err)
	}
	err = refuseMixedGroup("thread-1", "deploy", domain.WaitKindTime, nil, native)
	if err == nil || !strings.Contains(err.Error(), "nw-1") || !strings.Contains(err.Error(), "time") {
		t.Fatalf("a local kind joined a coordinator group: %v", err)
	}
	if err := refuseMixedGroup("thread-1", "deploy", domain.WaitKindGitHub, local, nil); err != nil {
		t.Fatalf("a local kind was refused from a local group: %v", err)
	}
	if err := refuseMixedGroup("thread-1", "deploy", domain.WaitKindQuota, nil, native); err != nil {
		t.Fatalf("a coordinator kind was refused from a coordinator group: %v", err)
	}
	// Another thread's group, or a settled member, does not bind.
	if err := refuseMixedGroup("thread-2", "deploy", domain.WaitKindNode, local, nil); err != nil {
		t.Fatalf("another thread's group bound this one: %v", err)
	}
	local[0].Status = wait.StatusWoken
	if err := refuseMixedGroup("thread-1", "deploy", domain.WaitKindNode, local, nil); err != nil {
		t.Fatalf("a woken member bound the group: %v", err)
	}
}

// --group and --wake all are accepted for the coordinator kinds and travel to
// the registration.
func TestCoordinatorWaitSpecCarriesGroupAndWake(t *testing.T) {
	spec, err := parseCoordinatorWaitSpec([]string{"--node", "run-2", "--group", "pair", "--wake", "all"})
	if err != nil || spec.Group != "pair" || spec.WakeMode != "all" {
		t.Fatalf("spec = %+v err=%v", spec, err)
	}
	cfg, _ := taskWaitCLIFixture(t)
	if err := cmdWaitAdd(context.Background(), cfg, nil, []string{"--group", "g", "--wake", "all", "--node", "run-1"}); err == nil {
		t.Fatal("a coordinator kind reached the local add path")
	}
}
