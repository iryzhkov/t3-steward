package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestBlockingControlsObserveExactCommandOnce(t *testing.T) {
	for _, verb := range []string{"cancel", "retry", "skip", "start", "resume", "pause", "delay", "rewake"} {
		t.Run(verb, func(t *testing.T) {
			command := backlogadmin.Command{ID: "control-1", State: domain.AdminCommandPending}
			fake := &fakeAdminMutationService{
				mutateResponse: backlogadmin.MutationResponse{Command: command},
				queryResponses: []backlogadmin.Response{
					{Task: &backlogadmin.TaskDetail{Attempt: &domain.Attempt{Revision: 4}}},
					{Commands: []backlogadmin.Command{command}},
					{Commands: []backlogadmin.Command{{ID: "other", State: domain.AdminCommandPending}, {ID: "control-1", State: domain.AdminCommandApplied}}},
				},
			}
			var out bytes.Buffer
			cli := backlogAdminCLI{service: fake, mutator: fake, stdout: &out, newCommandID: func() (string, error) { return "control-1", nil }}
			args := []string{verb, "run-1/task", "--reason", "test", "--wait", "--timeout", "2s", "--json"}
			if verb == "delay" {
				args = append(args, "--until", "2030-01-01T00:00:00Z")
			}
			if err := cli.runBacklog(context.Background(), args); err != nil {
				t.Fatal(err)
			}
			if len(fake.mutations) != 1 || len(fake.queries) != 3 || fake.mutations[0].ExpectedRevision != 4 {
				t.Fatalf("mutations=%v queries=%v", fake.mutations, fake.queries)
			}
			for _, q := range fake.queries[1:] {
				if q.Kind != backlogadmin.QueryCommands || q.CommandID != "control-1" {
					t.Fatalf("query=%v", q)
				}
			}
			var doc mutationDocument
			if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
				t.Fatal(err)
			}
			if doc.Command.State != domain.AdminCommandApplied {
				t.Fatalf("out=%s", out.String())
			}
		})
	}
}

func TestBlockingControlTimeoutAndReadOnlyReattach(t *testing.T) {
	command := backlogadmin.Command{ID: "control-1", State: domain.AdminCommandPending}
	fake := &fakeAdminMutationService{mutateResponse: backlogadmin.MutationResponse{Command: command}, queryResponse: backlogadmin.Response{Commands: []backlogadmin.Command{command}}}
	var out bytes.Buffer
	cli := backlogAdminCLI{service: fake, mutator: fake, stdout: &out}
	err := cli.runBacklog(context.Background(), []string{"cancel", "run-1/task", "--reason", "test", "--command-id", "control-1", "--expected-revision", "4", "--wait", "--timeout", "1ms"})
	if err == nil || exitCodeFor(err) != 1 || !strings.Contains(err.Error(), "backlog command show control-1 --wait") || len(fake.mutations) != 1 {
		t.Fatalf("err=%v mutations=%v", err, fake.mutations)
	}
	fake.queryResponse.Commands[0].State = domain.AdminCommandApplied
	out.Reset()
	if err := cli.runBacklog(context.Background(), []string{"command", "show", "control-1", "--wait", "--json"}); err != nil {
		t.Fatal(err)
	}
	if len(fake.mutations) != 1 || !strings.Contains(out.String(), "\"applied\"") {
		t.Fatalf("reattach mutated: %v out=%s", fake.mutations, out.String())
	}
}

func TestBlockingCampaignCancelAppliedAndRejected(t *testing.T) {
	for _, state := range []domain.AdminCommandState{domain.AdminCommandApplied, domain.AdminCommandRejected} {
		t.Run(string(state), func(t *testing.T) {
			var out bytes.Buffer
			var sent []backlogadmin.Mutation
			cli := cancelRunCLI(&out, cancelRunDetail(), &sent)
			cli.query = func(_ context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
				if len(sent) != 1 || q.CommandID != sent[0].ID || q.Kind != backlogadmin.QueryCommands {
					t.Fatalf("sent=%v query=%v", sent, q)
				}
				return backlogadmin.Response{Commands: []backlogadmin.Command{{ID: sent[0].ID, State: state, Failure: "evidence"}}}, nil
			}
			if err := cli.run(context.Background(), []string{"cancel", "run-1", "--reason", "test", "--wait", "--json"}); err != nil {
				t.Fatal(err)
			}
			var doc struct{ Outcome backlogadmin.Command }
			if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
				t.Fatal(err)
			}
			if doc.Outcome.State != state || strings.Contains(out.String(), "willCancel") {
				t.Fatalf("out=%s", out.String())
			}
		})
	}
}

func TestBlockingTaskSelectorDoesNotWaitForOtherTasksOrSink(t *testing.T) {
	f := newTaskResultFixture(t)
	f.detail.Summary.Run.Progress = domain.ProgressActive
	f.detail.Tasks = append(f.detail.Tasks, taskResultTaskDetail("other", "other", domain.ProgressActive))
	if err := f.run("run-1/task", "--wait", "--timeout", "1s"); err != nil {
		t.Fatal(err)
	}
	f.stdout.Reset()
	err := f.run("run-1", "--wait", "--timeout", "1ms")
	if err == nil || exitCodeFor(err) != 1 {
		t.Fatalf("err=%v", err)
	}
	f.stdout.Reset()
	err = f.run("run-1/missing", "--wait")
	if err == nil || !strings.Contains(err.Error(), "has no task") {
		t.Fatalf("err=%v", err)
	}
}

func TestBlockingCampaignShowTimeoutAndInterruptedReattach(t *testing.T) {
	reads, renders := 0, 0
	terminal := false
	var out bytes.Buffer
	cli := campaignCLI{stdout: &out, stderr: &out,
		detail: func(context.Context, string) (backlogadmin.WorkflowDetail, error) {
			reads++
			state := domain.ProgressActive
			if terminal {
				state = domain.ProgressSucceeded
			}
			return backlogadmin.WorkflowDetail{Summary: backlogadmin.WorkflowSummary{Run: domain.WorkflowRun{Progress: state}}}, nil
		},
		admin: func([]string) error { renders++; return nil },
	}
	err := cli.run(context.Background(), []string{"show", "run-1", "--wait", "--timeout", "1ms"})
	if err == nil || exitCodeFor(err) != 1 || renders != 0 || reads < 1 {
		t.Fatalf("err=%v reads=%d renders=%d", err, reads, renders)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = cli.run(ctx, []string{"show", "run-1", "--wait"})
	if err == nil || exitCodeFor(err) != 130 || renders != 0 {
		t.Fatalf("err=%v renders=%d", err, renders)
	}
	terminal = true
	if err = cli.run(context.Background(), []string{"show", "run-1", "--wait"}); err != nil || renders != 1 {
		t.Fatalf("reattach: err=%v renders=%d", err, renders)
	}
}
