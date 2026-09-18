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

// cancelRunCLI is a campaign CLI with only the two seams a run cancel needs:
// the run detail it fences on and the mutation it submits.
func cancelRunCLI(out *bytes.Buffer, detail backlogadmin.WorkflowDetail, sent *[]backlogadmin.Mutation) campaignCLI {
	return campaignCLI{
		stdout: out, stderr: out, principal: "operator",
		detail: func(context.Context, string) (backlogadmin.WorkflowDetail, error) {
			return detail, nil
		},
		mutate: func(_ context.Context, mutation backlogadmin.Mutation) (backlogadmin.MutationResponse, error) {
			*sent = append(*sent, mutation)
			return backlogadmin.MutationResponse{
				Version: backlogadmin.Version,
				Command: backlogadmin.Command{
					ID: mutation.ID, Kind: mutation.Kind, TargetType: domain.AdminTargetAttempt,
					TargetID: "attempt-alpha", ExpectedRevision: mutation.ExpectedRevision,
					State: domain.AdminCommandPending, Reason: mutation.Reason,
				},
			}, nil
		},
		admin: func(args []string) error {
			out.WriteString("forwarded " + strings.Join(args, " "))
			return nil
		},
	}
}

func cancelRunDetail() backlogadmin.WorkflowDetail {
	task := func(name, id, attempt string, progress domain.ProgressState, revision int64) backlogadmin.TaskDetail {
		return backlogadmin.TaskDetail{
			Task: domain.Task{ID: id, Name: name},
			Attempt: &domain.Attempt{
				ID: attempt, WorkflowRunID: "run-1", TaskID: id,
				Progress: progress, Revision: revision,
			},
		}
	}
	return backlogadmin.WorkflowDetail{
		Summary: backlogadmin.WorkflowSummary{
			Run: domain.WorkflowRun{ID: "run-1", Progress: domain.ProgressActive},
		},
		Tasks: []backlogadmin.TaskDetail{
			task("alpha", "task-alpha", "attempt-alpha", domain.ProgressActive, 2),
			task("beta", "task-beta", "attempt-beta", domain.ProgressReady, 6),
			task("gamma", "task-gamma", "attempt-gamma", domain.ProgressSucceeded, 4),
		},
	}
}

// Cancelling a whole run took one command per task. The run form sends one
// command, fenced on the anchor attempt the coordinator will apply it to, and
// says which tasks it covers.
func TestCampaignCancelRunSendsOneScopedCommand(t *testing.T) {
	var out bytes.Buffer
	var sent []backlogadmin.Mutation
	cli := cancelRunCLI(&out, cancelRunDetail(), &sent)
	if err := cli.run(context.Background(), []string{"cancel", "run-1", "--reason", "obsolete"}); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 {
		t.Fatalf("mutations = %+v, want exactly one", sent)
	}
	mutation := sent[0]
	if mutation.Kind != domain.AdminCommandCancel || mutation.WorkflowRunID != "run-1" || mutation.TaskID != "" {
		t.Fatalf("mutation = %+v", mutation)
	}
	var payload struct {
		Scope string `json:"scope"`
	}
	if err := json.Unmarshal(mutation.Payload, &payload); err != nil || payload.Scope != backlogadmin.MutationScopeRun {
		t.Fatalf("payload = %s (%v)", mutation.Payload, err)
	}
	// The fence is the anchor attempt's own revision, which is the one the
	// coordinator will apply the command against.
	if mutation.ExpectedRevision != 2 {
		t.Fatalf("expected revision = %d, want the anchor attempt's 2", mutation.ExpectedRevision)
	}
	text := out.String()
	for _, want := range []string{"alpha", "beta"} {
		if !strings.Contains(text, want) {
			t.Fatalf("the output does not name the cancelled task %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "gamma") {
		t.Fatalf("a terminal task was reported as cancelled:\n%s", text)
	}
}

// The task form is untouched: it still forwards to the backlog path.
func TestCampaignCancelTaskStillForwards(t *testing.T) {
	var out bytes.Buffer
	var sent []backlogadmin.Mutation
	cli := cancelRunCLI(&out, cancelRunDetail(), &sent)
	if err := cli.run(context.Background(), []string{"cancel", "run-1/alpha", "--reason", "just this one"}); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 0 {
		t.Fatalf("the task form sent a scoped mutation: %+v", sent)
	}
	if !strings.HasPrefix(out.String(), "forwarded cancel run-1/alpha") {
		t.Fatalf("output = %q", out.String())
	}
}

func TestCampaignCancelRunRefusals(t *testing.T) {
	t.Run("a reason is required", func(t *testing.T) {
		var out bytes.Buffer
		var sent []backlogadmin.Mutation
		cli := cancelRunCLI(&out, cancelRunDetail(), &sent)
		err := cli.run(context.Background(), []string{"cancel", "run-1"})
		if err == nil || !strings.Contains(err.Error(), "--reason") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("a terminal run has nothing to cancel", func(t *testing.T) {
		var out bytes.Buffer
		var sent []backlogadmin.Mutation
		detail := cancelRunDetail()
		for i := range detail.Tasks {
			detail.Tasks[i].Attempt.Progress = domain.ProgressSucceeded
		}
		cli := cancelRunCLI(&out, detail, &sent)
		err := cli.run(context.Background(), []string{"cancel", "run-1", "--reason", "too late"})
		if err == nil || !strings.Contains(err.Error(), "terminal") {
			t.Fatalf("error = %v", err)
		}
		if len(sent) != 0 {
			t.Fatalf("a command was sent for a terminal run: %+v", sent)
		}
	})
}

func TestCampaignCancelRunJSONIsOneDocument(t *testing.T) {
	var out bytes.Buffer
	var sent []backlogadmin.Mutation
	cli := cancelRunCLI(&out, cancelRunDetail(), &sent)
	if err := cli.run(context.Background(), []string{"cancel", "run-1", "--reason", "obsolete", "--json"}); err != nil {
		t.Fatal(err)
	}
	var document campaignCancelRunDocument
	if err := json.Unmarshal(out.Bytes(), &document); err != nil {
		t.Fatalf("--json did not print one document: %v\n%s", err, out.String())
	}
	if document.Run != "run-1" || strings.Join(document.Tasks, ",") != "alpha,beta" {
		t.Fatalf("document = %+v", document)
	}
	if document.Command.State != domain.AdminCommandPending {
		t.Fatalf("command = %+v", document.Command)
	}
}
