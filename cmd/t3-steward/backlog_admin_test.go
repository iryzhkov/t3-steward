package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type fakeAdminQueryService struct {
	response backlogadmin.Response
	err      error
	queries  []backlogadmin.Query
}

func (f *fakeAdminQueryService) Query(_ context.Context, query backlogadmin.Query) (backlogadmin.Response, error) {
	f.queries = append(f.queries, query)
	return f.response, f.err
}

func TestParseBacklogAdminQuery(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		kind       backlogadmin.QueryKind
		runID      string
		taskID     string
		artifactID string
		commandID  string
		asJSON     bool
	}{
		{name: "status", args: []string{"status"}, kind: backlogadmin.QueryStatus},
		{name: "workflow list", args: []string{"list"}, kind: backlogadmin.QueryWorkflows},
		{name: "workflow", args: []string{"show", "run-1"}, kind: backlogadmin.QueryWorkflow, runID: "run-1"},
		{name: "graph", args: []string{"graph", "run-1", "--json"}, kind: backlogadmin.QueryGraph, runID: "run-1", asJSON: true},
		{name: "task", args: []string{"task", "show", "run-1/task-1"}, kind: backlogadmin.QueryTask, runID: "run-1", taskID: "task-1"},
		{name: "events", args: []string{"events", "run-1"}, kind: backlogadmin.QueryEvents, runID: "run-1"},
		{name: "explanation", args: []string{"explain", "run-1/task-1"}, kind: backlogadmin.QueryExplanation, runID: "run-1", taskID: "task-1"},
		{name: "all artifacts", args: []string{"artifacts"}, kind: backlogadmin.QueryArtifacts},
		{name: "task artifacts", args: []string{"artifacts", "task-1"}, kind: backlogadmin.QueryArtifacts, taskID: "task-1"},
		{name: "run task artifacts", args: []string{"artifacts", "run-1/task-1"}, kind: backlogadmin.QueryArtifacts, runID: "run-1", taskID: "task-1"},
		{name: "artifact", args: []string{"artifact", "show", "artifact-1"}, kind: backlogadmin.QueryArtifact, artifactID: "artifact-1"},
		{name: "commands", args: []string{"commands", "run-1/task-1"}, kind: backlogadmin.QueryCommands, runID: "run-1", taskID: "task-1"},
		{name: "command", args: []string{"command", "show", "command-1"}, kind: backlogadmin.QueryCommands, commandID: "command-1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query, asJSON, err := parseBacklogAdminQuery(test.args)
			if err != nil {
				t.Fatal(err)
			}
			if query.Kind != test.kind || query.WorkflowRunID != test.runID || query.TaskID != test.taskID ||
				query.ArtifactID != test.artifactID || query.CommandID != test.commandID {
				t.Fatalf("query = %+v", query)
			}
			if asJSON != test.asJSON {
				t.Fatalf("asJSON = %t, want %t", asJSON, test.asJSON)
			}
		})
	}
}

func TestParseWorkflowFilters(t *testing.T) {
	query, asJSON, err := parseBacklogAdminQuery([]string{
		"list", "--project", "steward", "--schedule", "nightly",
		"--progress", "active,blocked", "--class", "surplus",
		"--worker", "normandy", "--quota-pool", "codex", "--json",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !asJSON {
		t.Fatal("--json was not recognized")
	}
	wantProgress := []domain.ProgressState{domain.ProgressActive, domain.ProgressBlocked}
	if query.Filter.Project != "steward" || query.Filter.ScheduleID != "nightly" ||
		query.Filter.Class != domain.TaskClassSurplus || query.Filter.WorkerID != "normandy" ||
		query.Filter.QuotaPoolID != "codex" ||
		len(query.Filter.Progress) != 2 || query.Filter.Progress[0] != wantProgress[0] || query.Filter.Progress[1] != wantProgress[1] {
		t.Fatalf("filter = %+v", query.Filter)
	}
}

func TestParseBacklogAdminQueryRejectsInvalidArguments(t *testing.T) {
	tests := [][]string{
		{"status", "extra"},
		{"list", "--unknown", "value"},
		{"list", "--project"},
		{"list", "--progress", "not-a-state"},
		{"list", "--class", "urgent"},
		{"show"},
		{"task", "show", "task-only"},
		{"explain", "task-only"},
		{"artifact", "get", "artifact-1"},
		{"commands", "one", "two"},
		{"status", "--json", "--json"},
	}
	for _, args := range tests {
		if _, _, err := parseBacklogAdminQuery(args); err == nil {
			t.Errorf("parseBacklogAdminQuery(%q) succeeded", args)
		}
	}
}

func TestBacklogAdminCLIAddsVersionPrincipalAndRendersJSON(t *testing.T) {
	now := time.Date(2026, 9, 10, 20, 0, 0, 0, time.UTC)
	fake := &fakeAdminQueryService{response: backlogadmin.Response{
		Version:     backlogadmin.Version,
		Kind:        backlogadmin.QueryStatus,
		GeneratedAt: now,
		Status: &backlogadmin.Status{
			WorkflowRuns: map[domain.ProgressState]int{},
			Tasks:        map[domain.ProgressState]int{},
			Workers:      map[string]int{},
			QuotaPools:   map[domain.AdmissionState]int{},
		},
	}}
	var out bytes.Buffer
	cli := backlogAdminCLI{
		service:   fake,
		principal: backlogadmin.Principal{ID: "operator", Roles: []string{"local-admin"}},
		stdout:    &out,
	}
	if err := cli.runBacklog(context.Background(), []string{"status", "--json"}); err != nil {
		t.Fatal(err)
	}
	if len(fake.queries) != 1 {
		t.Fatalf("queries = %d, want 1", len(fake.queries))
	}
	query := fake.queries[0]
	if query.Version != backlogadmin.Version || query.Principal.ID != "operator" {
		t.Fatalf("query = %+v", query)
	}
	var got backlogadmin.Response
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON %q: %v", out.String(), err)
	}
	if got.Version != backlogadmin.Version || got.Kind != backlogadmin.QueryStatus || got.Status == nil {
		t.Fatalf("response = %+v", got)
	}
}

func TestBacklogAdminCLIPropagatesServiceErrorWithoutOutput(t *testing.T) {
	fake := &fakeAdminQueryService{err: errors.New("denied")}
	var out bytes.Buffer
	cli := backlogAdminCLI{service: fake, principal: backlogadmin.Principal{ID: "operator"}, stdout: &out}
	if err := cli.runBacklog(context.Background(), []string{"status"}); err == nil || !strings.Contains(err.Error(), "denied") {
		t.Fatalf("error = %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("output = %q", out.String())
	}
}

func TestSchedulesCLIListShowHistory(t *testing.T) {
	now := time.Date(2026, 9, 10, 20, 0, 0, 0, time.UTC)
	response := backlogadmin.Response{
		Version:     backlogadmin.Version,
		Kind:        backlogadmin.QuerySchedules,
		GeneratedAt: now,
		Schedules: []backlogadmin.Schedule{{
			Schedule: domain.Schedule{
				ID: "nightly", Name: "Nightly", WorkflowID: "workflow-1",
				Expression: "0 2 * * *", Timezone: "UTC", Enabled: true, Revision: 3,
			},
			Triggers: []domain.Trigger{{
				ID: "trigger-1", ScheduleID: "nightly", NominalAt: now,
				State: domain.TriggerSuppressed, Reason: "active run",
			}},
		}},
	}
	for _, test := range []struct {
		args []string
		want string
	}{
		{args: []string{"list"}, want: "nightly"},
		{args: []string{"show", "nightly"}, want: "revision: 3"},
		{args: []string{"history", "nightly"}, want: "active run"},
	} {
		t.Run(test.args[0], func(t *testing.T) {
			fake := &fakeAdminQueryService{response: response}
			var out bytes.Buffer
			cli := backlogAdminCLI{
				service: fake, principal: backlogadmin.Principal{ID: "operator", Roles: []string{"local-admin"}}, stdout: &out,
			}
			if err := cli.runSchedules(context.Background(), test.args); err != nil {
				t.Fatal(err)
			}
			if len(fake.queries) != 1 || fake.queries[0].Kind != backlogadmin.QuerySchedules {
				t.Fatalf("queries = %+v", fake.queries)
			}
			if !strings.Contains(out.String(), test.want) {
				t.Fatalf("output %q does not contain %q", out.String(), test.want)
			}
		})
	}
}

func TestSchedulesCLISelectsBeforeJSONRendering(t *testing.T) {
	fake := &fakeAdminQueryService{response: backlogadmin.Response{
		Version: backlogadmin.Version,
		Kind:    backlogadmin.QuerySchedules,
		Schedules: []backlogadmin.Schedule{
			{Schedule: domain.Schedule{ID: "one"}},
			{Schedule: domain.Schedule{ID: "two"}},
		},
	}}
	var out bytes.Buffer
	cli := backlogAdminCLI{
		service: fake, principal: backlogadmin.Principal{ID: "operator", Roles: []string{"local-admin"}}, stdout: &out,
	}
	if err := cli.runSchedules(context.Background(), []string{"show", "two", "--json"}); err != nil {
		t.Fatal(err)
	}
	var response backlogadmin.Response
	if err := json.Unmarshal(out.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Schedules) != 1 || response.Schedules[0].Schedule.ID != "two" {
		t.Fatalf("schedules = %+v", response.Schedules)
	}
}

func TestSchedulesCLINotFoundAndUsage(t *testing.T) {
	fake := &fakeAdminQueryService{response: backlogadmin.Response{
		Version: backlogadmin.Version, Kind: backlogadmin.QuerySchedules,
	}}
	var out bytes.Buffer
	cli := backlogAdminCLI{service: fake, principal: backlogadmin.Principal{ID: "operator"}, stdout: &out}
	if err := cli.runSchedules(context.Background(), []string{"show", "missing"}); !errors.Is(err, backlogadmin.ErrNotFound) {
		t.Fatalf("error = %v", err)
	}
	out.Reset()
	if err := cli.runSchedules(context.Background(), []string{"help"}); err != nil {
		t.Fatal(err)
	}
	if len(fake.queries) != 1 {
		t.Fatalf("help queried service; queries = %d", len(fake.queries))
	}
	if !strings.Contains(out.String(), "Usage: t3-steward schedules") {
		t.Fatalf("usage = %q", out.String())
	}
}

func TestHumanRenderersExposeCoordinatorDetails(t *testing.T) {
	now := time.Date(2026, 9, 10, 20, 0, 0, 0, time.UTC)
	attempt := domain.Attempt{
		ID: "attempt-1", WorkflowRunID: "run-1", TaskID: "task-1",
		Number: 2, Progress: domain.ProgressActive, Control: domain.ControlRunning, Revision: 7,
	}
	task := backlogadmin.TaskDetail{
		Task:    domain.Task{ID: "task-1", WorkflowID: "workflow-1", Name: "Implement", Class: domain.TaskClassRequired},
		Attempt: &attempt, ThreadURL: "https://normandy.example/thread/1",
	}
	artifact := backlogadmin.Artifact{
		Metadata: backlogadmin.ArtifactMetadata{
			ID: "artifact-1", Kind: domain.ArtifactOutput, Name: "result.md",
			TaskID: "task-1", Size: 42, SHA256: "abc",
		},
		Download: "artifact://artifact-1",
	}
	tests := []struct {
		name     string
		response backlogadmin.Response
		wants    []string
	}{
		{name: "status", response: backlogadmin.Response{Kind: backlogadmin.QueryStatus, Status: &backlogadmin.Status{
			WorkflowRuns: map[domain.ProgressState]int{domain.ProgressActive: 1},
			Tasks:        map[domain.ProgressState]int{domain.ProgressActive: 1},
			Workers:      map[string]int{"healthy": 2},
			QuotaPools:   map[domain.AdmissionState]int{domain.AdmissionOpen: 1},
			Reservations: 3, Locks: 1,
		}}, wants: []string{"WORKFLOW RUNS", "active: 1", "reservations: 3"}},
		{name: "workflows", response: backlogadmin.Response{Kind: backlogadmin.QueryWorkflows, Workflows: []backlogadmin.WorkflowSummary{{
			Run:      domain.WorkflowRun{ID: "run-1", Progress: domain.ProgressActive},
			Workflow: domain.Workflow{ID: "workflow-1", Name: "Build", Project: "steward", Class: domain.TaskClassRequired},
			Progress: backlogadmin.Progress{Total: 2, Succeeded: 1},
		}}}, wants: []string{"RUN", "run-1", "1/2"}},
		{name: "workflow", response: backlogadmin.Response{Kind: backlogadmin.QueryWorkflow, Workflow: &backlogadmin.WorkflowDetail{
			Summary: backlogadmin.WorkflowSummary{
				Run:      domain.WorkflowRun{ID: "run-1", Progress: domain.ProgressActive, Revision: 4},
				Workflow: domain.Workflow{ID: "workflow-1", Name: "Build", Project: "steward", Class: domain.TaskClassRequired},
			},
			Tasks: []backlogadmin.TaskDetail{task},
		}}, wants: []string{"run: run-1", "Implement", "attempt-1"}},
		{name: "graph", response: backlogadmin.Response{Kind: backlogadmin.QueryGraph, Graph: &backlogadmin.Graph{
			WorkflowRunID: "run-1",
			Nodes:         []backlogadmin.GraphNode{{TaskID: "task-1", Name: "Implement", Progress: domain.ProgressActive}},
			Edges:         []backlogadmin.GraphEdge{{FromTaskID: "inspect", ToTaskID: "task-1"}},
		}}, wants: []string{"Implement [active]", "inspect -> task-1"}},
		{name: "task", response: backlogadmin.Response{Kind: backlogadmin.QueryTask, Task: &task}, wants: []string{"Implement", "revision: 7", "https://normandy.example/thread/1"}},
		{name: "explanation", response: backlogadmin.Response{Kind: backlogadmin.QueryExplanation, Explanation: &backlogadmin.Explanation{
			WorkflowRunID: "run-1", TaskID: "task-1", Summary: "waiting", Blockers: []backlogadmin.Blocker{{Code: "quota", Detail: "pool constrained"}},
		}}, wants: []string{"waiting", "quota: pool constrained"}},
		{name: "events", response: backlogadmin.Response{Kind: backlogadmin.QueryEvents, Events: []backlogadmin.Event{{
			ID: "event-1", Kind: "assignment-created", TaskID: "task-1", At: now,
		}}}, wants: []string{"TIME", "assignment-created"}},
		{name: "artifacts", response: backlogadmin.Response{Kind: backlogadmin.QueryArtifacts, Artifacts: []backlogadmin.Artifact{artifact}}, wants: []string{"artifact-1", "result.md", "artifact://artifact-1"}},
		{name: "artifact", response: backlogadmin.Response{Kind: backlogadmin.QueryArtifact, Artifact: &artifact}, wants: []string{"artifact-1", "abc"}},
		{name: "commands", response: backlogadmin.Response{Kind: backlogadmin.QueryCommands, Commands: []backlogadmin.Command{{
			ID: "command-1", Kind: domain.AdminCommandPause, TargetType: domain.AdminTargetAttempt,
			TargetID: "attempt-1", State: domain.AdminCommandPending, RequestedBy: "operator", Reason: "maintenance",
		}}}, wants: []string{"command-1", "pending", "maintenance"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := renderAdminResponse(&out, test.response, ""); err != nil {
				t.Fatal(err)
			}
			for _, want := range test.wants {
				if !strings.Contains(out.String(), want) {
					t.Errorf("output %q does not contain %q", out.String(), want)
				}
			}
		})
	}
}

func TestLocalAdminAuthorizerFailsClosed(t *testing.T) {
	authorizer := localAdminAuthorizer{}
	if err := authorizer.Authorize(context.Background(), backlogadmin.Principal{}, backlogadmin.Action{}); err == nil {
		t.Fatal("empty principal authorized")
	}
	if err := authorizer.Authorize(context.Background(), backlogadmin.Principal{ID: "operator"}, backlogadmin.Action{}); err == nil {
		t.Fatal("principal without local-admin role authorized")
	}
	if err := authorizer.Authorize(context.Background(), backlogadmin.Principal{
		ID: "operator", Roles: []string{"local-admin"},
	}, backlogadmin.Action{}); err != nil {
		t.Fatalf("local admin denied: %v", err)
	}
}

func TestCoordinatorReadRoutingPreservesLegacyHelpers(t *testing.T) {
	for _, args := range [][]string{{"path"}, {"check", "-"}, {"receive", "id"}, {"new", "id"}, {"retry", "id"}, {"cancel", "id"}, {"list", "--all"}} {
		if isCoordinatorRead(args) {
			t.Errorf("%q unexpectedly routed to coordinator admin", args)
		}
	}
	for _, args := range [][]string{{"status"}, {"list"}, {"list", "--project", "steward"}, {"show", "run-1"}, {"task", "show", "run-1/task-1"}, {"commands"}, {"command", "show", "command-1"}} {
		if !isCoordinatorRead(args) {
			t.Errorf("%q did not route to coordinator admin", args)
		}
	}
}
