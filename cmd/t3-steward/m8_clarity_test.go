package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"io"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

func TestM8CampaignStatusAlias(t *testing.T) {
	var got []string
	c := campaignCLI{stdout: &bytes.Buffer{}, admin: func(args []string) error { got = args; return nil }}
	if err := c.run(context.Background(), []string{"status", "run-1", "--json"}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, " ") != "show run-1 --json" {
		t.Fatalf("forwarded %v", got)
	}
}

func TestM8GuessedTaskVerbs(t *testing.T) {
	for _, verb := range []string{"list", "statuz", "reslt", "submit", "cancel"} {
		for _, args := range [][]string{{verb}, {verb, "--help"}} {
			err := cmdTask(globalFlags{}, args)
			if err == nil {
				t.Fatalf("%s was accepted", verb)
			}
			for _, want := range []string{"t3-steward campaign list", "t3-steward backlog list"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("%s: %v lacks %q", verb, err, want)
				}
			}
			if verb == "statuz" && !strings.Contains(err.Error(), "t3-steward campaign show") {
				t.Errorf("no status alternative: %v", err)
			}
			if verb == "cancel" && !strings.Contains(err.Error(), "t3-steward campaign cancel") {
				t.Errorf("no cancellation alternative: %v", err)
			}
			if verb == "submit" && !strings.Contains(err.Error(), "t3-steward campaign submit") {
				t.Errorf("no submit alternative: %v", err)
			}
			if verb == "reslt" && !strings.Contains(err.Error(), "t3-steward task result") {
				t.Errorf("no closest command: %v", err)
			}
		}
	}
}

func TestM8EventsTaskSelector(t *testing.T) {
	q, _, err := parseBacklogAdminQuery([]string{"events", "run-1/review"})
	if err != nil {
		t.Fatal(err)
	}
	if q.Kind != backlogadmin.QueryEvents || q.WorkflowRunID != "run-1" || q.TaskID != "review" {
		t.Fatalf("query = %+v", q)
	}
}

func TestM8ShortHelp(t *testing.T) {
	for _, path := range helpPagePaths() {
		t.Run(path, func(t *testing.T) {
			var short, full bytes.Buffer
			args := append(strings.Fields(path), "--help")
			if answered, err := admitHelp(&short, nil, args); !answered || err != nil {
				t.Fatalf("short: %v, %v", answered, err)
			}
			if answered, err := admitHelp(&full, nil, append(args, "full")); !answered || err != nil {
				t.Fatalf("full: %v, %v", answered, err)
			}
			if lines := strings.Count(short.String(), "\n"); lines > 26 {
				t.Errorf("short help has %d lines", lines)
			}
			if strings.HasPrefix(short.String(), "Usage:") || strings.HasPrefix(short.String(), "Coordinator submission command:") {
				t.Error("short help confuses purpose with usage or a command list")
			}
			if short.String() == full.String() {
				t.Error("full help is not separately available")
			}
			if len(helpPageChildren(strings.Fields(path))) > 0 {
				purpose := strings.SplitN(short.String(), "\n\n", 2)[0]
				if strings.Contains(purpose, "Commands:") || strings.Contains(purpose, "serve Run") {
					t.Error("purpose repeats the command list")
				}
			}
			if !strings.Contains(short.String(), "--help full") {
				t.Error("short help lacks full reference command")
			}
		})
	}
}

func TestM8CampaignFullHelpUsesTopicReferences(t *testing.T) {
	var out bytes.Buffer
	if _, err := admitCampaignHelp(&out, []string{"--help", "full"}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"backlog list", "backlog events"} {
		page, _ := helpPageFor(path)
		want := "--state open|terminal"
		if path == "backlog events" {
			want = "<workflow-run>[/<task>]"
		}
		if !strings.Contains(strings.Join(page.Usage, " "), want) {
			t.Errorf("%s synopsis lacks %s", path, want)
		}
	}
	for _, want := range []string{"status <run>", "--state open|terminal", "campaign help authoring", "--register-only"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q", want)
		}
	}
	if strings.Contains(out.String(), "cat > demo/workflow.yaml") {
		t.Error("family help duplicates authoring topic")
	}
}

func TestM8RecoveryHelpBeforeConfiguration(t *testing.T) {
	g := globalFlags{configPath: "absent-m8.yaml"}
	for _, path := range [][]string{{"recovery"}, {"recovery", "retry"}} {
		out := captureStdout(t, func() {
			if err := cmdCampaign(g, append(path, "--help")); err != nil {
				t.Fatal(err)
			}
		})
		if !strings.Contains(out, "--help full") {
			t.Fatalf("missing full reference: %s", out)
		}
	}
}

func TestM8ListStates(t *testing.T) {
	for _, state := range []string{"open", "terminal"} {
		q, _, err := parseBacklogAdminQuery([]string{"list", "--state", state})
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range []domain.ProgressState{domain.ProgressActive, domain.ProgressSucceeded, domain.ProgressFailed, domain.ProgressCancelled, domain.ProgressSkipped, domain.ProgressWaitingExternal} {
			found := false
			for _, filter := range q.Filter.Progress {
				found = found || p == filter
			}
			terminal := p == domain.ProgressSucceeded || p == domain.ProgressFailed || p == domain.ProgressCancelled || p == domain.ProgressSkipped
			if found != (terminal == (state == "terminal")) {
				t.Errorf("%s includes %s = %t", state, p, found)
			}
		}
	}
	for _, args := range [][]string{{"list", "--state"}, {"list", "--state", "wrong"}, {"list", "--state", "open", "--state", "terminal"}, {"list", "--state", "open", "--progress", "active"}} {
		_, _, err := parseBacklogAdminQuery(args)
		if err == nil || !strings.Contains(err.Error(), "t3-steward campaign list") {
			t.Fatalf("%v: %v", args, err)
		}
	}
}

func TestM8ListJSONShape(t *testing.T) {
	var out bytes.Buffer
	service := &fakeAdminQueryService{response: backlogadmin.Response{Version: backlogadmin.Version, Kind: backlogadmin.QueryWorkflows}}
	cli := backlogAdminCLI{service: service, stdout: &out}
	if err := cli.runBacklog(context.Background(), []string{"list", "--json"}); err != nil {
		t.Fatal(err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	if string(doc["schemaVersion"]) != "1" || string(doc["workflows"]) != "[]" {
		t.Fatalf("unstable empty list: %s", out.String())
	}
}

func TestM8ListQuotesProjectNames(t *testing.T) {
	var out bytes.Buffer
	renderWorkflows(&out, []backlogadmin.WorkflowSummary{{Run: domain.WorkflowRun{ID: "run-1"}, Workflow: domain.Workflow{Name: "workflow", Project: "My Project"}}})
	if !strings.Contains(out.String(), "\"My Project\"") {
		t.Fatalf("project name splits columns: %s", out.String())
	}
}

func TestM8ShowVerification(t *testing.T) {
	var out bytes.Buffer
	detail := backlogadmin.WorkflowDetail{Tasks: []backlogadmin.TaskDetail{
		{Task: domain.Task{ID: "task-1", Name: "review"}, Attempt: &domain.Attempt{ID: "a1"}, Artifacts: []backlogadmin.Artifact{
			{Metadata: backlogadmin.ArtifactMetadata{ID: "v1", AttemptID: "a1", Kind: domain.ArtifactVerification}},
			{Metadata: backlogadmin.ArtifactMetadata{ID: "old", AttemptID: "a0", Kind: domain.ArtifactVerification}},
		}},
		{Task: domain.Task{ID: "task-2", Name: "queued"}},
	}}
	service := &fakeAdminQueryService{response: backlogadmin.Response{Kind: backlogadmin.QueryWorkflow, Workflow: &detail}}
	artifacts := &fakeArtifactService{content: backlogadmin.ArtifactContent{Content: io.NopCloser(strings.NewReader("{\"command\":\"go test ./...\",\"exitCode\":0}"))}}
	cli := backlogAdminCLI{service: service, artifacts: artifacts, stdout: &out}
	if err := cli.runBacklog(context.Background(), []string{"show", "run-1"}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"review: go test ./...: passed (exit 0)", "queued: not reported"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}
	if artifacts.id != "v1" {
		t.Fatalf("read stale attempt report %q", artifacts.id)
	}
}

func TestM8ShowVerificationInvalidReports(t *testing.T) {
	for _, raw := range []string{"{}", `{"command":"go test","exitCode":null}`, `{"command":"","exitCode":0}`, "invalid JSON"} {
		t.Run(raw, func(t *testing.T) {
			var out bytes.Buffer
			detail := backlogadmin.WorkflowDetail{Tasks: []backlogadmin.TaskDetail{{
				Task: domain.Task{ID: "task-1", Name: "review"}, Attempt: &domain.Attempt{ID: "a1"},
				Artifacts: []backlogadmin.Artifact{{Metadata: backlogadmin.ArtifactMetadata{ID: "v1", AttemptID: "a1", Kind: domain.ArtifactVerification}}},
			}}}
			cli := backlogAdminCLI{
				service:   &fakeAdminQueryService{response: backlogadmin.Response{Kind: backlogadmin.QueryWorkflow, Workflow: &detail}},
				artifacts: &fakeArtifactService{content: backlogadmin.ArtifactContent{Content: io.NopCloser(strings.NewReader(raw))}},
				stdout:    &out,
			}
			if err := cli.runBacklog(context.Background(), []string{"show", "run-1"}); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), "review: unavailable") || !strings.Contains(out.String(), "t3-steward backlog artifact get v1") || strings.Contains(out.String(), ": passed") {
				t.Fatalf("malformed report invented verification: %s", out.String())
			}
		})
	}
}

func TestM8InlineResultEscapesTerminalControls(t *testing.T) {
	f := newTaskResultFixture(t)
	f.content["final-message-attempt-task-1"] = "message \x1b[2J done"
	if err := f.run("run-1"); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(f.stdout.String(), "\x1b") || !strings.Contains(f.stdout.String(), "\\x1b") {
		t.Fatalf("terminal controls were printed raw: %q", f.stdout.String())
	}
}

func TestM8TaskResultInline(t *testing.T) {
	f := newTaskResultFixture(t)
	if err := f.run("run-1"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.stdout.String(), "the job") {
		t.Fatalf("short result missing:\n%s", f.stdout.String())
	}
	f = newTaskResultFixture(t)
	f.content["final-message-attempt-task-1"] = strings.Repeat("large result ", 1000)
	if err := f.run("run-1"); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(f.stdout.String(), "large result ") {
		t.Fatal("large result dumped inline")
	}
}
