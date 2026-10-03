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

func TestBlockingTaskResultPreservesVerdictsAndOutput(t *testing.T) {
	for _, state := range []domain.ProgressState{domain.ProgressSucceeded, domain.ProgressFailed, domain.ProgressCancelled, domain.ProgressSkipped} {
		t.Run(string(state), func(t *testing.T) {
			f := newTaskResultFixture(t)
			f.detail.Tasks[0].Attempt.Progress = state
			cli := f.cli()
			query := cli.query
			calls := 0
			cli.query = func(ctx context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
				calls++
				response, err := query(ctx, q)
				if calls == 1 {
					copy := *response.Workflow
					copy.Summary.Run.Progress = domain.ProgressActive
					copy.Tasks = []backlogadmin.TaskDetail{taskResultTaskDetail("task", "task-1", domain.ProgressActive)}
					copy.Artifacts = nil
					response.Workflow = &copy
				}
				return response, err
			}
			err := cli.run(context.Background(), []string{"run-1", "--wait", "--timeout", "2s", "--json"})
			want := 0
			if state == domain.ProgressFailed || state == domain.ProgressCancelled {
				want = 2
			}
			if (err != nil && exitCodeFor(err) != want) || (err == nil && want != 0) || calls != 2 {
				t.Fatalf("err=%v calls=%d want verdict=%d and two queries", err, calls, want)
			}
			waited := f.stdout.String()
			f.stdout.Reset()
			_ = f.run("run-1", "--json")
			if waited != f.stdout.String() {
				t.Fatalf("wait changed result: %s versus %s", waited, f.stdout.String())
			}
		})
	}
}

func TestBlockingTaskResultTimeoutAndReattach(t *testing.T) {
	f := newTaskResultFixture(t)
	f.detail.Tasks[0].Attempt.Progress = domain.ProgressActive
	f.detail.Summary.Run.Progress = domain.ProgressActive
	f.detail.Artifacts = nil
	err := f.run("run-1", "--wait", "--timeout", "1ms", "--json")
	if err == nil || exitCodeFor(err) != 1 || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("err=%v", err)
	}
	if f.document(t).Outcome != string(domain.ProgressActive) {
		t.Fatal("wrong timeout progress")
	}
	f.stdout.Reset()
	f.detail.Tasks[0].Attempt.Progress = domain.ProgressSucceeded
	f.detail.Summary.Run.Progress = domain.ProgressSucceeded
	if err := f.run("run-1", "--wait"); err != nil {
		t.Fatalf("reattach: %v", err)
	}
}

func TestBlockingCampaignShowWaitsBeforeRendering(t *testing.T) {
	var out bytes.Buffer
	reads, renders := 0, 0
	cli := campaignCLI{
		stdout: &out, stderr: &out,
		detail: func(context.Context, string) (backlogadmin.WorkflowDetail, error) {
			reads++
			progress := domain.ProgressActive
			if reads == 2 {
				progress = domain.ProgressFailed
			}
			return backlogadmin.WorkflowDetail{Summary: backlogadmin.WorkflowSummary{Run: domain.WorkflowRun{ID: "run-1", Progress: progress}}}, nil
		},
		admin: func(args []string) error {
			renders++
			if reads != 2 || strings.Contains(strings.Join(args, " "), "--wait") {
				t.Fatalf("premature render or forwarded wait: %v reads=%d", args, reads)
			}
			return nil
		},
	}
	if err := cli.run(context.Background(), []string{"show", "run-1", "--wait", "--timeout", "2s"}); err != nil {
		t.Fatal(err)
	}
	if renders != 1 {
		t.Fatalf("renders=%d", renders)
	}
}

func TestBlockingControlFlags(t *testing.T) {
	for _, verb := range []string{"cancel", "retry", "skip", "start", "resume", "pause", "delay", "rewake"} {
		args := []string{verb, "run-1/task", "--reason", "test", "--wait", "--timeout", "2s"}
		if verb == "delay" {
			args = append(args, "--until", "2030-01-01T00:00:00Z")
		}
		if _, err := parseBacklogMutation(args); err != nil {
			t.Errorf("%s: %v", verb, err)
		}
	}
	var out bytes.Buffer
	var sent []backlogadmin.Mutation
	cli := cancelRunCLI(&out, cancelRunDetail(), &sent)
	// An immediately rejected command needs no status query.
	cli.mutate = func(_ context.Context, m backlogadmin.Mutation) (backlogadmin.MutationResponse, error) {
		sent = append(sent, m)
		return backlogadmin.MutationResponse{Command: backlogadmin.Command{ID: m.ID, State: domain.AdminCommandRejected, Failure: "stale revision"}}, nil
	}
	if err := cli.run(context.Background(), []string{"cancel", "run-1", "--reason", "test", "--wait", "--timeout", "2s"}); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 || !strings.Contains(out.String(), "rejected") || strings.Contains(out.String(), "will cancel") {
		t.Fatalf("sent=%v out=%s", sent, out.String())
	}
}

func TestTaskRunDryRunDerivesWithoutSubmitting(t *testing.T) {
	h := newTaskRunHarness()
	if err := h.cli().run(context.Background(), []string{"--dry-run", "--model", "opus", "--json", "--", "Investigate issue"}); err != nil {
		t.Fatal(err)
	}
	if len(h.requests) != 0 || len(h.notified) != 0 || len(h.viability) != 0 {
		t.Fatalf("dry run had effects: requests=%v notifications=%v viability=%v", h.requests, h.notified, h.viability)
	}
	var doc map[string]any
	if err := json.Unmarshal(h.stdout.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"project", "ref", "route", "idempotencyKey", "notifyThread", "promptCharacters"} {
		if doc[key] == nil {
			t.Errorf("missing %s: %s", key, h.stdout.String())
		}
	}
	if doc["project"] != "steward" || doc["ref"] != "main" || doc["notifyThread"] != "thread-1" {
		t.Fatalf("doc=%v", doc)
	}
}
