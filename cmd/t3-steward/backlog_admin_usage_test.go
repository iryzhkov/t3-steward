package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// backlog list is bounded: the text form prints the newest 50 runs and says how
// many it left out, --limit and --since narrow either form, and the query sent
// to the coordinator carries neither, because a coordinator of the previous
// release decodes a query strictly and would refuse a field it does not know.
func TestBacklogListIsBoundedClientSide(t *testing.T) {
	now := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	var runs []backlogadmin.WorkflowSummary
	for i := range 120 {
		// Newest first, one hour apart, as the coordinator answers.
		runs = append(runs, backlogadmin.WorkflowSummary{
			Run:      domain.WorkflowRun{ID: fmt.Sprintf("run-%03d", i), CreatedAt: now.Add(-time.Duration(i) * time.Hour)},
			Workflow: domain.Workflow{Name: "w", Project: "p"},
		})
	}
	answer := backlogadmin.Response{Kind: backlogadmin.QueryWorkflows, GeneratedAt: now, Workflows: runs}
	list := func(args ...string) (string, []backlogadmin.Query, error) {
		fake := &fakeAdminMutationService{queryResponse: answer}
		var out bytes.Buffer
		cli := backlogAdminCLI{service: fake, principal: backlogadmin.Principal{ID: "operator"}, stdout: &out}
		err := cli.runBacklog(context.Background(), append([]string{"list"}, args...))
		return out.String(), fake.queries, err
	}
	rows := func(text string) int { return strings.Count(text, "\nrun-") }

	text, queries, err := list()
	if err != nil {
		t.Fatal(err)
	}
	if rows(text) != 50 || !strings.Contains(text, "showing the newest 50 of 120 runs") || !strings.Contains(text, "run-000") || strings.Contains(text, "run-050") {
		t.Fatalf("default text list printed %d rows:\n%s", rows(text), text)
	}
	if len(queries) != 1 || !reflect.DeepEqual(queries[0].Filter, backlogadmin.Filter{}) {
		t.Fatalf("queries = %+v", queries)
	}
	if text, _, err = list("--limit", "0"); err != nil || rows(text) != 120 || strings.Contains(text, "showing") {
		t.Fatalf("--limit 0 printed %d rows (%v)", rows(text), err)
	}
	if text, _, err = list("--since", "5h30m", "--project", "p"); err != nil || rows(text) != 6 || strings.Contains(text, "showing") {
		t.Fatalf("--since 5h30m printed %d rows (%v):\n%s", rows(text), err, text)
	}
	// A whole number of days is accepted beside a Go duration.
	if text, _, err = list("--since", "1d"); err != nil || rows(text) != 25 {
		t.Fatalf("--since 1d printed %d rows (%v)", rows(text), err)
	}
	if text, _, err = list("--json", "--limit", "3"); err != nil {
		t.Fatal(err)
	}
	var document backlogadmin.Response
	if err := json.Unmarshal([]byte(text), &document); err != nil || len(document.Workflows) != 3 || document.Workflows[0].Run.ID != "run-000" {
		t.Fatalf("--json --limit 3 = %d runs (%v)", len(document.Workflows), err)
	}
	if text, _, err = list("--json"); err != nil || json.Unmarshal([]byte(text), &document) != nil || len(document.Workflows) != 120 {
		t.Fatalf("--json without --limit trimmed the list to %d (%v)", len(document.Workflows), err)
	}
	for _, bad := range [][]string{{"--limit", "-1"}, {"--limit"}, {"--since", "yesterday"}, {"--since", "xd"}, {"--since", "0d"}, {"--limit", "1", "--limit", "2"}} {
		if _, _, err := list(bad...); err == nil {
			t.Fatalf("%v was accepted", bad)
		}
	}
	// --all with anything beside it is neither the legacy listing nor a
	// coordinator filter, and is refused with the command meant.
	if _, _, err := list("--all", "--limit", "5"); err == nil || !strings.Contains(err.Error(), "drop --all") {
		t.Fatalf("list --all --limit 5 = %v", err)
	}
	// backlog usage keeps its own --limit, which means a page of raw samples.
	if query, _, err := parseBacklogAdminQuery([]string{"usage", "run-1", "--raw", "--limit", "5"}); err != nil || query.UsageLimit != 5 {
		t.Fatalf("usage --limit = %+v %v", query, err)
	}
}

// The usage text labels the run's progress as show does, and explains a model
// row the run was not routed to without taking it out of any total: Claude
// Code reports its own small-model calls, such as title generation, in the
// per-model usage of the task's turn.
func TestUsageTextLabelsProgressAndAuxiliaryModels(t *testing.T) {
	report := &domain.UsageReport{
		WorkflowRunID: "run-1", RunProgress: domain.ProgressReady,
		Totals: domain.UsageTotals{UncachedInputTokens: 30, Turns: 2},
		ByModel: []domain.UsageAggregate{
			{Key: "claude-haiku-4-5", Model: "claude-haiku-4-5", Totals: domain.UsageTotals{UncachedInputTokens: 20, Turns: 1}},
			{Key: "claude-sonnet-5", Model: "claude-sonnet-5", Totals: domain.UsageTotals{UncachedInputTokens: 10, Turns: 1}},
		},
	}
	var out bytes.Buffer
	if err := renderUsage(&out, report, ""); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Run: run-1 (progress: ready)", "provider's own auxiliary calls in the same session"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("usage text does not say %q:\n%s", want, out.String())
		}
	}
	report.ByModel = report.ByModel[1:]
	out.Reset()
	if err := renderUsage(&out, report, ""); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "auxiliary") {
		t.Fatalf("one model needs no note:\n%s", out.String())
	}
}

// A progress filter that names no known state is refused with the states it
// could have named. "invalid progress" alone sent an operator to the source to
// find out that the state they wanted is spelled active.
func TestProgressFilterRefusalListsTheValidValues(t *testing.T) {
	_, err := parseWorkflowFilters([]string{"--progress", "running"})
	if err == nil {
		t.Fatal("--progress running was accepted")
	}
	want := `invalid progress "running"; valid values: queued, blocked, ready, active, needs-input, waiting-external, verifying, succeeded, failed, cancelled, skipped`
	if err.Error() != want {
		t.Fatalf("error = %q\nwant    %q", err, want)
	}
}

// The verbs that only take a show form say so, and when the caller supplied
// what looks like the identifier without the show word, the refusal spells out
// the command they probably meant.
func TestShowOnlyVerbsSuggestTheShowForm(t *testing.T) {
	tests := []struct {
		args []string
		want string
	}{
		{[]string{"command", "admin-a9d88bf818bde6b032d6baaf"},
			"usage: backlog command show <command>; did you mean: backlog command show admin-a9d88bf818bde6b032d6baaf?"},
		{[]string{"command"}, "usage: backlog command show <command>"},
		{[]string{"command", "show"}, "usage: backlog command show <command>"},
		{[]string{"command", "show", "a", "b"}, "usage: backlog command show <command>"},
		{[]string{"task", "run-1/task-1"},
			"usage: backlog task show <workflow-run>/<task>; did you mean: backlog task show run-1/task-1?"},
		{[]string{"task"}, "usage: backlog task show <workflow-run>/<task>"},
		{[]string{"artifact", "artifact-1"},
			"usage: backlog artifact show <artifact>; did you mean: backlog artifact show artifact-1?"},
		{[]string{"artifact"}, "usage: backlog artifact show <artifact>"},
	}
	for _, test := range tests {
		t.Run(strings.Join(test.args, " "), func(t *testing.T) {
			_, _, err := parseBacklogAdminQuery(test.args)
			if err == nil {
				t.Fatalf("%v was accepted", test.args)
			}
			if err.Error() != test.want {
				t.Fatalf("error = %q\nwant    %q", err, test.want)
			}
		})
	}
}
