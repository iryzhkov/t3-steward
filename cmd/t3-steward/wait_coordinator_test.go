package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// --node <run>[/<task>] --state parses into a structured condition; the
// default state is terminal and a run alone means its sink.
func TestCoordinatorWaitSpecParsesNode(t *testing.T) {
	spec, err := parseCoordinatorWaitSpec([]string{"--task", "current", "--node", "run-2"})
	if err != nil {
		t.Fatal(err)
	}
	if spec.Kind != domain.WaitKindNode || spec.Node == nil || spec.Node.Target.RunID != "run-2" || spec.Node.Target.TaskID != domain.SinkTaskName || spec.Node.State != domain.NodeStateTerminal {
		t.Fatalf("spec = %+v node=%+v", spec, spec.Node)
	}
	spec, err = parseCoordinatorWaitSpec([]string{"--node", "run-2/deploy", "--state", "paused", "--timeout", "2h", "--or-timeout"})
	if err != nil {
		t.Fatal(err)
	}
	if spec.Node.Target.TaskID != "deploy" || spec.Node.State != domain.NodeStatePaused || spec.Timeout != 2*time.Hour || !spec.OrTimeout {
		t.Fatalf("spec = %+v node=%+v", spec, spec.Node)
	}
	for _, args := range [][]string{
		{"--node", "run-2", "--state", "finished"},
		{"--node", "run-2", "--", "true"},
		{"--node", "run-2", "--for", "1h"},
		{"--node", ""},
	} {
		if _, err := parseCoordinatorWaitSpec(args); err == nil {
			t.Fatalf("%v was accepted", args)
		}
	}
	if !coordinatorWaitArgs([]string{"--task", "current", "--node", "run-2"}) || coordinatorWaitArgs([]string{"--task", "current", "--", "true"}) {
		t.Fatal("coordinator kinds are not routed by their flags")
	}
}

// --task current --node parks the attempt on a coordinator record and leaves
// no local row; the coordinator's refusals are reported as command errors.
func TestTaskBoundNodeWaitRegistersWithoutALocalRow(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	tasks := []domain.Task{{ID: "task-2", WorkflowID: "workflow-2", Name: "deploy", Class: domain.TaskClassRequired}}
	run, err := domain.BindRunSink(domain.WorkflowRun{ID: "run-2", WorkflowID: "workflow-2", Revision: 1, Progress: domain.ProgressActive, CreatedAt: now, UpdatedAt: now}, tasks)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}, Tasks: tasks}); err != nil {
		t.Fatal(err)
	}
	err = cmdTaskWaitAdd(ctx, cfg, []string{"--task", "current", "--request-id", "self", "--node", "run-1"})
	if err == nil || !strings.Contains(err.Error(), "own run") {
		t.Fatalf("a task parked on its own run: %v", err)
	}
	if err := cmdTaskWaitAdd(ctx, cfg, []string{"--task", "current", "--request-id", "other", "--node", "run-2/deploy", "--state", "succeeded"}); err != nil {
		t.Fatal(err)
	}
	waits, err := store.ListTaskWaits(ctx)
	if err != nil || len(waits) != 1 {
		t.Fatalf("waits=%v err=%v", waits, err)
	}
	if waits[0].Kind != domain.WaitKindNode || waits[0].Node == nil || waits[0].Node.Target.TaskID != "task-2" || waits[0].Node.State != domain.NodeStateSucceeded {
		t.Fatalf("coordinator record = %+v node=%+v", waits[0], waits[0].Node)
	}
	if rows, err := store.ListWaits(ctx, ""); err != nil || len(rows) != 0 {
		t.Fatalf("a coordinator kind left a local row: %v %v", rows, err)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil || records.Attempts[0].Progress != domain.ProgressWaitingExternal {
		t.Fatalf("the attempt is not parked: %+v err=%v", records.Attempts, err)
	}
}
