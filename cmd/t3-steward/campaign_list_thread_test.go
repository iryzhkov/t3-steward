package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// threadListFixture is a coordinator that knows nothing of a thread filter:
// it answers the ordinary workflow list and the ordinary node-wait list, and
// the thread filter is whatever this client makes of the two.
func threadListFixture(t *testing.T) (*fakeAdminQueryService, *[]backlogadmin.NodeWaitOperation, backlogAdminCLI, *bytes.Buffer) {
	t.Helper()
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	run := func(id string, progress domain.ProgressState) backlogadmin.WorkflowSummary {
		return backlogadmin.WorkflowSummary{
			Run:      domain.WorkflowRun{ID: id, Progress: progress, CreatedAt: now.Add(-time.Hour)},
			Workflow: domain.Workflow{ID: "workflow-" + id, Name: "name-" + id, Project: "steward", Class: domain.TaskClassRequired},
			Progress: backlogadmin.Progress{Total: 2, Succeeded: 1},
		}
	}
	service := &fakeAdminQueryService{response: backlogadmin.Response{
		Kind: backlogadmin.QueryWorkflows, GeneratedAt: now,
		Workflows: []backlogadmin.WorkflowSummary{
			run("run-mine-sink", domain.ProgressActive),
			run("run-other-thread", domain.ProgressActive),
			run("run-mine-task", domain.ProgressActive),
			run("run-unowned", domain.ProgressActive),
		},
	}}
	wait := func(id, thread string, target domain.NodeRef, delivery string) domain.NodeWait {
		return domain.NodeWait{Request: domain.NodeWaitRequest{ID: id, ThreadID: thread, Target: target}, Delivery: delivery}
	}
	operations := &[]backlogadmin.NodeWaitOperation{}
	var out bytes.Buffer
	cli := backlogAdminCLI{
		service:   service,
		principal: backlogadmin.Principal{ID: "operator"},
		stdout:    &out,
		nodeWaits: func(_ context.Context, operation backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
			*operations = append(*operations, operation)
			return backlogadmin.NodeWaitResponse{Waits: []domain.NodeWait{
				wait("nw-campaign-a", "thread-mine", domain.NodeRef{RunID: "run-mine-sink", TaskID: domain.SinkTaskName}, "delivered"),
				wait("nw-campaign-b", "thread-other", domain.NodeRef{RunID: "run-other-thread", TaskID: domain.SinkTaskName}, "pending"),
				wait("nw-manual", "thread-mine", domain.NodeRef{RunID: "run-mine-task", TaskID: "task-1"}, "pending"),
				// A quota wait names no run and owns nothing.
				{Request: domain.NodeWaitRequest{ID: "nw-quota", ThreadID: "thread-mine", Quota: &domain.QuotaWaitCondition{}}, Delivery: "pending"},
			}}, nil
		},
		resolveThread: func(explicit string) (string, error) {
			if explicit != "" {
				return explicit, nil
			}
			return "thread-mine", nil
		},
	}
	return service, operations, cli, &out
}

func listedRunIDs(t *testing.T, raw []byte) []string {
	t.Helper()
	var document struct {
		Workflows []backlogadmin.WorkflowSummary `json:"workflows"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("%v: %s", err, raw)
	}
	ids := []string{}
	for _, workflow := range document.Workflows {
		ids = append(ids, workflow.Run.ID)
	}
	return ids
}

func TestListThreadKeepsOnlyTheRunsWithANodeWaitForThatThread(t *testing.T) {
	for _, thread := range []string{"thread-mine", "current"} {
		service, operations, cli, out := threadListFixture(t)
		if err := cli.runBacklog(context.Background(), []string{"list", "--thread", thread, "--json"}); err != nil {
			t.Fatal(err)
		}
		if got := listedRunIDs(t, out.Bytes()); !reflect.DeepEqual(got, []string{"run-mine-sink", "run-mine-task"}) {
			t.Fatalf("--thread %s listed %v", thread, got)
		}
		if len(*operations) != 1 || (*operations)[0].Action != "list" || (*operations)[0].Host != "" || (*operations)[0].Undelivered {
			// An unnarrowed list: a delivered wait still names its run.
			t.Fatalf("node-wait operations = %+v, want one unnarrowed list", *operations)
		}
		// The coordinator is asked exactly what it is asked without --thread,
		// so a coordinator of any release answers it.
		plain, _, err := parseBacklogAdminQuery([]string{"list", "--json"})
		if err != nil {
			t.Fatal(err)
		}
		plain.Version, plain.Principal = backlogadmin.Version, cli.principal
		if len(service.queries) != 1 || !reflect.DeepEqual(service.queries[0], plain) {
			t.Fatalf("queries = %+v, want exactly %+v", service.queries, plain)
		}
	}
}

func TestListThreadComposesWithStateAndText(t *testing.T) {
	service, _, cli, out := threadListFixture(t)
	if err := cli.runBacklog(context.Background(), []string{"list", "--state", "open", "--thread", "thread-mine"}); err != nil {
		t.Fatal(err)
	}
	if len(service.queries) != 1 || len(service.queries[0].Filter.Progress) == 0 {
		t.Fatalf("--state was not sent with --thread: %+v", service.queries)
	}
	text := out.String()
	if !strings.Contains(text, "run-mine-sink") || !strings.Contains(text, "run-mine-task") {
		t.Fatalf("the thread's runs are missing:\n%s", text)
	}
	for _, absent := range []string{"run-other-thread", "run-unowned"} {
		if strings.Contains(text, absent) {
			t.Fatalf("%s is listed for a thread that does not own it:\n%s", absent, text)
		}
	}
}

func TestListThreadWithNoRunsPrintsAnEmptyList(t *testing.T) {
	_, _, cli, out := threadListFixture(t)
	if err := cli.runBacklog(context.Background(), []string{"list", "--thread", "thread-nobody", "--json"}); err != nil {
		t.Fatal(err)
	}
	if got := listedRunIDs(t, out.Bytes()); len(got) != 0 {
		t.Fatalf("listed %v for a thread with no waits", got)
	}
	if !strings.Contains(out.String(), `"workflows": []`) {
		t.Fatalf("workflows is not an empty array:\n%s", out.String())
	}
}

// An unresolved "current" is refused, never read as "every thread", and the
// refusal comes before the coordinator is asked anything.
func TestListThreadCurrentRefusesAnUnresolvedThread(t *testing.T) {
	service, operations, cli, out := threadListFixture(t)
	cli.resolveThread = func(string) (string, error) {
		return "", unresolvedThread("no T3 thread could be resolved from the caller's provider session (%s)", "none set")
	}
	err := cli.runBacklog(context.Background(), []string{"list", "--thread", "current"})
	if err == nil {
		t.Fatalf("an unresolved thread listed:\n%s", out.String())
	}
	for _, want := range []string{"could not identify the calling agent's T3 thread", "nothing was listed", "--thread <THREAD-ID>"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not contain %q", err, want)
		}
	}
	if len(service.queries) != 0 || len(*operations) != 0 {
		t.Fatalf("the coordinator was asked: queries=%d node-wait=%d", len(service.queries), len(*operations))
	}
}

func TestListThreadArgumentRefusals(t *testing.T) {
	for _, args := range [][]string{
		{"list", "--thread"},
		{"list", "--thread", ""},
		{"list", "--thread", "a", "--thread", "b"},
	} {
		service, _, cli, _ := threadListFixture(t)
		if err := cli.runBacklog(context.Background(), args); err == nil {
			t.Fatalf("%v was accepted", args)
		}
		if len(service.queries) != 0 {
			t.Fatalf("%v reached the coordinator", args)
		}
	}
}

// A failed node-wait read is reported with its transport class rather than
// printed as a list with nothing in it.
func TestListThreadReportsAFailedNodeWaitRead(t *testing.T) {
	_, _, cli, out := threadListFixture(t)
	cli.nodeWaits = func(context.Context, backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
		return backlogadmin.NodeWaitResponse{}, &backlogadmin.TransportError{Class: backlogadmin.ClassUnavailable, Operation: "node-wait", Err: errors.New("down")}
	}
	err := cli.runBacklog(context.Background(), []string{"list", "--thread", "thread-mine"})
	if exitCodeFor(err) != 5 {
		t.Fatalf("err = %v (exit %d), want the unavailable exit 5", err, exitCodeFor(err))
	}
	if out.Len() != 0 {
		t.Fatalf("a list was printed:\n%s", out.String())
	}
}

// The campaign alias forwards --thread untouched to the backlog verb, which
// owns the parsing.
func TestCampaignListForwardsThread(t *testing.T) {
	var forwarded []string
	cli := campaignCLI{admin: func(args []string) error { forwarded = args; return nil }}
	args := []string{"list", "--thread", "current", "--state", "open", "--json"}
	if err := cli.run(context.Background(), args); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(forwarded, args) {
		t.Fatalf("forwarded %v, want %v", forwarded, args)
	}
	for _, path := range []string{"campaign list", "backlog list"} {
		page, _ := helpPageFor(path)
		body := page.render()
		for _, want := range []string{"--thread", "--no-notify", "Wave C"} {
			if !strings.Contains(body, want) {
				t.Fatalf("%s help does not mention %q:\n%s", path, want, body)
			}
		}
	}
}
