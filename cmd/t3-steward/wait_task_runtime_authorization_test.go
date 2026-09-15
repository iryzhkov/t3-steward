package main

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
	"time"
)

func TestTaskWaitRuntimeRetainsAuthorizationAndRevisionFences(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	service, err := backlogadmin.New(store, localAdminAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"settle-task", "expire-task", "wake-task", "pending-task", "transition-task"} {
		if _, err := service.NodeWait(ctx, backlogadmin.Principal{ID: "untrusted", Roles: []string{"worker"}}, backlogadmin.NodeWaitOperation{Action: action, ID: "unknown"}); err == nil {
			t.Fatalf("%s bypassed administrator authorization", action)
		}
	}
	t.Setenv(domain.TaskWaitEnvAttemptRevision, "8")
	if err := cmdTaskWaitAdd(ctx, cfg, []string{"--task", "current", "--request-id", "future", "--", "false"}); err == nil {
		t.Fatal("future revision registered")
	}
	waits, _ := store.ListTaskWaits(ctx)
	checks, _ := store.ListWaits(ctx, "")
	if len(waits) != 0 || len(checks) != 0 {
		t.Fatal("refused future registration wrote coordinator or local poll")
	}
	principal := backlogadmin.Principal{ID: "local:test", Roles: []string{backlogadmin.LocalAdminRole}}
	for _, op := range []backlogadmin.NodeWaitOperation{
		{Action: "settle-task", ID: "unknown"},
		{Action: "settle-task", Result: &domain.TaskWaitResult{Outcome: domain.TaskWaitMet, ObservedAt: time.Now()}},
		{Action: "transition-task", ID: "unknown"},
	} {
		if _, err := service.NodeWait(ctx, principal, op); err == nil {
			t.Fatalf("malformed runtime operation accepted: %+v", op)
		}
	}
}

func TestTaskWaitRegistrationReplayPreservesSettledPoll(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	args := []string{"--task", "current", "--request-id", "retry", "--", "false"}
	if err := cmdTaskWaitAdd(ctx, cfg, args); err != nil {
		t.Fatal(err)
	}
	checks, _ := store.ListWaits(ctx, "")
	check := checks[0]
	check.Status = "met"
	check.Runs = 17
	if err := store.SaveWait(ctx, check); err != nil {
		t.Fatal(err)
	}
	if err := cmdTaskWaitAdd(ctx, cfg, args); err != nil {
		t.Fatal(err)
	}
	checks, _ = store.ListWaits(ctx, "")
	if len(checks) != 1 || checks[0].Status != "met" || checks[0].Runs != 17 {
		t.Fatalf("replay reset or duplicated poll: %+v", checks)
	}
}
