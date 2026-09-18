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

// cancelRunCLI is a campaign CLI with only the three seams a run cancel needs:
// the run detail it fences on, the mutation it submits, and the release the
// coordinator reports for itself, which decides whether the run form can be
// applied at all. The release defaults to one that can.
func cancelRunCLI(out *bytes.Buffer, detail backlogadmin.WorkflowDetail, sent *[]backlogadmin.Mutation) campaignCLI {
	return campaignCLI{
		stdout: out, stderr: out, principal: "operator",
		release: func(context.Context) (string, error) { return campaignRunCancelRelease, nil },
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
	// The text form says "will cancel" for the same reason the JSON key is
	// willCancel: the command is pending, and nothing has been applied yet.
	if !strings.Contains(text, "will cancel") || !strings.Contains(text, "backlog commands run-1") {
		t.Fatalf("the text form does not separate the intention from the outcome:\n%s", text)
	}
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

// A coordinator of the previous release decodes this request, ignores the
// scope, resolves the empty task to the run itself and then fails at
// application: the operator gets a command id and a silent failure some ticks
// later. The client refuses to send it and names the form that release applies.
func TestCampaignCancelRunIsRefusedAgainstAnOlderCoordinator(t *testing.T) {
	var out bytes.Buffer
	var sent []backlogadmin.Mutation
	cli := cancelRunCLI(&out, cancelRunDetail(), &sent)
	cli.release = func(context.Context) (string, error) { return "v0.11.0-rc.69", nil }
	err := cli.run(context.Background(), []string{"cancel", "run-1", "--reason", "obsolete"})
	if err == nil {
		t.Fatal("a run-scoped cancel was sent to a coordinator that cannot apply it")
	}
	for _, want := range []string{
		"v0.11.0-rc.69", campaignRunCancelRelease,
		"t3-steward campaign cancel run-1/<task> --reason TEXT",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal does not name %q: %v", want, err)
		}
	}
	if len(sent) != 0 {
		t.Fatalf("a command was sent anyway: %+v", sent)
	}
}

// The same read admits every release that can apply it, including a final
// release and a later minor version, which sort above their candidates.
func TestCampaignCancelRunIsSentToACoordinatorThatCanApplyIt(t *testing.T) {
	for _, release := range []string{campaignRunCancelRelease, "v0.11.0-rc.71", "v0.11.0", "v0.12.0-rc.1"} {
		t.Run(release, func(t *testing.T) {
			var out bytes.Buffer
			var sent []backlogadmin.Mutation
			cli := cancelRunCLI(&out, cancelRunDetail(), &sent)
			cli.release = func(context.Context) (string, error) { return release, nil }
			if err := cli.run(context.Background(), []string{"cancel", "run-1", "--reason", "obsolete"}); err != nil {
				t.Fatal(err)
			}
			if len(sent) != 1 {
				t.Fatalf("mutations = %+v, want exactly one", sent)
			}
		})
	}
}

// A release this rule cannot read is not refused -- that would break the verb
// against every build whose release string it does not understand -- but it is
// warned about on stderr, because a command that is accepted and never applied
// is what the check exists to prevent.
func TestCampaignCancelRunWarnsWhenTheReleaseCannotBeRead(t *testing.T) {
	var stdout, stderr bytes.Buffer
	var sent []backlogadmin.Mutation
	cli := cancelRunCLI(&stdout, cancelRunDetail(), &sent)
	cli.stderr = &stderr
	cli.release = func(context.Context) (string, error) { return "dev", nil }
	if err := cli.run(context.Background(), []string{"cancel", "run-1", "--reason", "obsolete"}); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 {
		t.Fatalf("mutations = %+v, want exactly one", sent)
	}
	for _, want := range []string{"warning:", "never applied", "campaign cancel run-1/<task>"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("the warning does not say %q:\n%s", want, stderr.String())
		}
	}
	if strings.Contains(stdout.String(), "warning") {
		t.Fatalf("the warning landed on stdout, where it would break --json:\n%s", stdout.String())
	}
}

func TestReleaseAtLeastOrdersCandidatesAndFinalReleases(t *testing.T) {
	for _, testCase := range []struct {
		release, minimum string
		atLeast, known   bool
	}{
		{"v0.11.0-rc.70", "v0.11.0-rc.70", true, true},
		{"v0.11.0-rc.69", "v0.11.0-rc.70", false, true},
		{"v0.11.0-rc.9", "v0.11.0-rc.70", false, true},
		{"v0.11.0-rc.100", "v0.11.0-rc.70", true, true},
		{"v0.11.0", "v0.11.0-rc.70", true, true},
		{"v0.10.9", "v0.11.0-rc.70", false, true},
		{"0.11.0-rc.70", "v0.11.0-rc.70", true, true},
		{"dev", "v0.11.0-rc.70", false, false},
		{"", "v0.11.0-rc.70", false, false},
		{"v0.11.0-beta.1", "v0.11.0-rc.70", false, false},
	} {
		atLeast, known := releaseAtLeast(testCase.release, testCase.minimum)
		if atLeast != testCase.atLeast || known != testCase.known {
			t.Fatalf("releaseAtLeast(%q, %q) = %t, %t; want %t, %t",
				testCase.release, testCase.minimum, atLeast, known, testCase.atLeast, testCase.known)
		}
	}
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
	// The key is willCancel. Under "tasks", beside "run", it would sit exactly
	// where "task run" prints the tasks that exist, and an automated caller
	// would read an intention computed from a possibly stale read as the
	// outcome of a command that is still pending.
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(out.Bytes(), &keys); err != nil {
		t.Fatal(err)
	}
	if _, present := keys["willCancel"]; !present {
		t.Fatalf("the document has no willCancel key: %s", out.String())
	}
	if _, present := keys["tasks"]; present {
		t.Fatalf("the document still names an intention \"tasks\": %s", out.String())
	}
}
