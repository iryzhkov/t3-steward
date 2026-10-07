package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

var collectNow = time.Date(2026, 10, 7, 16, 0, 0, 0, time.UTC)

func collectAt(minutesAgo int) *time.Time {
	t := collectNow.Add(-time.Duration(minutesAgo) * time.Minute)
	return &t
}

// collectFixture is a coordinator that knows run collection. Thread
// thread-mine owns every run below except run-other, which belongs to
// thread-other, and run-pruned, which the coordinator no longer lists.
type collectFixture struct {
	workflows   []backlogadmin.WorkflowSummary
	waits       []domain.NodeWait
	taskWaits   []domain.TaskWait
	collections map[string][]domain.RunCollection
	// refuse answers collect-run for a run with this error.
	refuse map[string]error
	// listCollectionsErr and collectErr replace every answer to their action.
	listCollectionsErr error
	collectErr         error
	operations         []backlogadmin.NodeWaitOperation
	queries            []backlogadmin.Query
	thread             func(string) (string, error)
	stdout, stderr     bytes.Buffer
}

func newCollectFixture() *collectFixture {
	summary := func(id, name string, progress domain.ProgressState, completed *time.Time, counts backlogadmin.Progress) backlogadmin.WorkflowSummary {
		return backlogadmin.WorkflowSummary{
			Run:      domain.WorkflowRun{ID: id, WorkflowID: "wf-" + id, Progress: progress, CreatedAt: collectNow.Add(-3 * time.Hour), UpdatedAt: collectNow.Add(-time.Minute), CompletedAt: completed},
			Workflow: domain.Workflow{ID: "wf-" + id, Name: name, Project: "steward", Class: domain.TaskClassRequired},
			Progress: counts,
		}
	}
	f := &collectFixture{
		workflows: []backlogadmin.WorkflowSummary{
			summary("run-collected", "read-last-week", domain.ProgressSucceeded, collectAt(100), backlogadmin.Progress{Total: 1, Succeeded: 1}),
			summary("run-rerun", "rerun-x", domain.ProgressSucceeded, collectAt(30), backlogadmin.Progress{Total: 2, Succeeded: 2}),
			summary("run-succeeded", "findings-scan", domain.ProgressSucceeded, collectAt(60), backlogadmin.Progress{Total: 1, Succeeded: 1}),
			summary("run-failed", "impl-review", domain.ProgressFailed, collectAt(10), backlogadmin.Progress{Total: 3, Succeeded: 1, Failed: 1, Skipped: 1}),
			summary("run-ask", "implement-x", domain.ProgressWaitingExternal, nil, backlogadmin.Progress{Total: 2, WaitingExternal: 1, Queued: 1}),
			summary("run-branch", "fan-out", domain.ProgressActive, nil, backlogadmin.Progress{Total: 3, Failed: 1, Active: 1, Blocked: 1}),
			summary("run-idle", "long-build", domain.ProgressActive, nil, backlogadmin.Progress{Total: 1, Active: 1}),
			summary("run-other", "not-mine", domain.ProgressFailed, collectAt(5), backlogadmin.Progress{Total: 1, Failed: 1}),
		},
		collections: map[string][]domain.RunCollection{
			"thread-mine": {
				{RunID: "run-collected", ThreadID: "thread-mine", Progress: domain.ProgressSucceeded, CompletedAt: collectAt(100), CollectedAt: collectNow.Add(-90 * time.Minute)},
				// Collected as it finished before a rerun finished it again.
				{RunID: "run-rerun", ThreadID: "thread-mine", Progress: domain.ProgressSucceeded, CompletedAt: collectAt(200), CollectedAt: collectNow.Add(-190 * time.Minute)},
			},
		},
		refuse: map[string]error{},
	}
	wait := func(id, thread, run string, created time.Time, delivery string, delivered *time.Time) domain.NodeWait {
		return domain.NodeWait{
			Request:   domain.NodeWaitRequest{ID: id, ThreadID: thread, Target: domain.NodeRef{RunID: run, TaskID: domain.SinkTaskName}},
			CreatedAt: created, Delivery: delivery, DeliveredAt: delivered,
		}
	}
	older, newer := collectNow.Add(-3*time.Hour), collectNow.Add(-2*time.Hour)
	f.waits = []domain.NodeWait{
		wait("nw-campaign-collected", "thread-mine", "run-collected", older, "delivered", collectAt(99)),
		wait("nw-campaign-rerun", "thread-mine", "run-rerun", older, "delivered", collectAt(29)),
		wait("nw-campaign-succeeded", "thread-mine", "run-succeeded", older, "delivered", collectAt(59)),
		// Two waits of the thread on one run: the newer one names the wake.
		wait("nw-campaign-failed-old", "thread-mine", "run-failed", older, "cancelled", nil),
		wait("nw-campaign-failed-new", "thread-mine", "run-failed", newer, "delivered", collectAt(9)),
		// Another thread's newer wait on the same run does not count.
		wait("nw-campaign-failed-other", "thread-other", "run-failed", collectNow.Add(-time.Hour), "pending", nil),
		wait("nw-campaign-ask", "thread-mine", "run-ask", older, "pending", nil),
		wait("nw-campaign-branch", "thread-mine", "run-branch", older, "pending", nil),
		wait("nw-campaign-idle", "thread-mine", "run-idle", older, "pending", nil),
		wait("nw-campaign-other", "thread-other", "run-other", older, "delivered", collectAt(4)),
		wait("nw-campaign-pruned", "thread-mine", "run-pruned", older, "delivered", collectAt(500)),
	}
	registered := collectNow.Add(-50 * time.Minute)
	settled := collectNow.Add(-40 * time.Minute)
	f.taskWaits = []domain.TaskWait{
		{ID: "w-ask-12", WorkflowRunID: "run-ask", TaskID: "implement", Kind: domain.WaitKindAsk, Ask: &domain.AskRequest{Question: "Which base?", Options: []string{"main", "rc"}}, RegisteredAt: registered},
		// An answered ask on an idle run is no attention.
		{ID: "w-ask-old", WorkflowRunID: "run-idle", TaskID: "build", Kind: domain.WaitKindAsk, Ask: &domain.AskRequest{Question: "Old?"}, RegisteredAt: registered, SettledAt: &settled},
		// A shell wait is no attention either.
		{ID: "w-shell", WorkflowRunID: "run-idle", TaskID: "build", Kind: domain.WaitKindShell, RegisteredAt: registered},
		// Another thread's run: never listed here.
		{ID: "w-ask-other", WorkflowRunID: "run-other", TaskID: "t", Kind: domain.WaitKindAsk, Ask: &domain.AskRequest{Question: "Not mine?"}, RegisteredAt: registered},
	}
	f.thread = func(explicit string) (string, error) {
		if explicit != "" {
			return explicit, nil
		}
		return "thread-mine", nil
	}
	return f
}

func (f *collectFixture) cli() campaignCLI {
	return campaignCLI{
		stdout: &f.stdout, stderr: &f.stderr,
		query: func(_ context.Context, query backlogadmin.Query) (backlogadmin.Response, error) {
			f.queries = append(f.queries, query)
			return backlogadmin.Response{Kind: backlogadmin.QueryWorkflows, GeneratedAt: collectNow, Workflows: f.workflows}, nil
		},
		notify: func(_ context.Context, op backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
			f.operations = append(f.operations, op)
			switch op.Action {
			case "list":
				return backlogadmin.NodeWaitResponse{Waits: f.waits}, nil
			case "list-task":
				return backlogadmin.NodeWaitResponse{TaskWaits: f.taskWaits}, nil
			case backlogadmin.RunCollectionListAction:
				if f.listCollectionsErr != nil {
					return backlogadmin.NodeWaitResponse{}, f.listCollectionsErr
				}
				return backlogadmin.NodeWaitResponse{Collections: f.collections[op.Request.ThreadID]}, nil
			case backlogadmin.RunCollectAction:
				if f.collectErr != nil {
					return backlogadmin.NodeWaitResponse{}, f.collectErr
				}
				run := op.Request.Target.RunID
				if err := f.refuse[run]; err != nil {
					return backlogadmin.NodeWaitResponse{}, err
				}
				for _, w := range f.workflows {
					if w.Run.ID == run {
						return backlogadmin.NodeWaitResponse{Changed: true, Collections: []domain.RunCollection{{
							RunID: run, ThreadID: op.Request.ThreadID, Progress: w.Run.Progress, CompletedAt: w.Run.CompletedAt, CollectedAt: collectNow, Actor: "operator",
						}}}, nil
					}
				}
				return backlogadmin.NodeWaitResponse{}, &backlogadmin.TransportError{Class: backlogadmin.ClassRejected, Operation: "node-wait", Err: errors.New("run collection refused: unknown run " + run)}
			}
			return backlogadmin.NodeWaitResponse{}, fmt.Errorf("unexpected node-wait action %q", op.Action)
		},
		resolveThread: func(explicit string) (string, error) { return f.thread(explicit) },
	}
}

func (f *collectFixture) actions() []string {
	out := []string{}
	for _, op := range f.operations {
		out = append(out, op.Action+":"+op.Request.Target.RunID)
	}
	return out
}

// collected is the runs collect-run was sent for, in order.
func (f *collectFixture) collected() []string {
	out := []string{}
	for _, op := range f.operations {
		if op.Action == backlogadmin.RunCollectAction {
			out = append(out, op.Request.Target.RunID)
		}
	}
	return out
}

type uncollectedDocument struct {
	SchemaVersion       int    `json:"schemaVersion"`
	Kind                string `json:"kind"`
	Thread              string `json:"thread"`
	GeneratedAt         string `json:"generatedAt"`
	CollectionSupported bool   `json:"collectionSupported"`
	Note                string `json:"note"`
	Runs                []struct {
		Run        string  `json:"run"`
		Name       string  `json:"name"`
		Project    string  `json:"project"`
		State      string  `json:"state"`
		FinishedAt *string `json:"finishedAt"`
		Attention  []struct {
			Kind    string  `json:"kind"`
			Subject string  `json:"subject"`
			Task    string  `json:"task"`
			Detail  string  `json:"detail"`
			Since   *string `json:"since"`
		} `json:"attention"`
		Wake *struct {
			WaitID      string  `json:"waitId"`
			Delivery    string  `json:"delivery"`
			DeliveredAt *string `json:"deliveredAt"`
		} `json:"wake"`
		Result  string `json:"result"`
		Collect string `json:"collect"`
	} `json:"runs"`
	Omitted map[string]int `json:"omitted"`
}

func decodeUncollected(t *testing.T, raw []byte) uncollectedDocument {
	t.Helper()
	var document uncollectedDocument
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		t.Fatalf("%v:\n%s", err, raw)
	}
	return document
}

func TestCampaignUncollectedListsFinishedAndAttention(t *testing.T) {
	f := newCollectFixture()
	if err := f.cli().run(context.Background(), []string{"uncollected", "--thread", "current"}); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(f.stdout.String(), "\n"), "\n")
	if len(lines) != 8 {
		t.Fatalf("want summary, header, 5 rows and the next step; got %d lines:\n%s", len(lines), f.stdout.String())
	}
	if want := "thread thread-mine: 5 runs to collect (1 collected, 1 active without attention and 1 no longer listed by the coordinator not shown)"; lines[0] != want {
		t.Fatalf("summary = %q\nwant      %q", lines[0], want)
	}
	if got := strings.Fields(lines[1]); !reflect.DeepEqual(got, []string{"RUN", "NAME", "STATE", "FINISHED", "WAKE", "ATTENTION"}) {
		t.Fatalf("header = %q", lines[1])
	}
	wantRows := []struct {
		fields    []string
		attention string
	}{
		{[]string{"run-ask", "implement-x", "waiting-external", "-", "pending"}, `ask w-ask-12 on implement: "Which base?" (since 2026-10-07T15:10:00Z)`},
		{[]string{"run-branch", "fan-out", "active", "-", "pending"}, "task-failed: 1 tasks failed; dependents are blocked"},
		{[]string{"run-failed", "impl-review", "failed", "2026-10-07T15:50:00Z", "delivered"}, "failed"},
		{[]string{"run-rerun", "rerun-x", "succeeded", "2026-10-07T15:30:00Z", "delivered"}, "-"},
		{[]string{"run-succeeded", "findings-scan", "succeeded", "2026-10-07T15:00:00Z", "delivered"}, "-"},
	}
	for i, want := range wantRows {
		row := lines[2+i]
		fields := strings.Fields(row)
		if len(fields) < 6 || !reflect.DeepEqual(fields[:5], want.fields) || !strings.HasSuffix(row, want.attention) {
			t.Fatalf("row %d = %q\nwant %v ... %s", i, row, want.fields, want.attention)
		}
	}
	if want := `Read a result with "t3-steward task result RUN", then record it with "t3-steward campaign collect RUN... --thread current".`; lines[7] != want {
		t.Fatalf("next step = %q", lines[7])
	}
	for _, absent := range []string{"run-collected", "run-idle", "run-other", "run-pruned"} {
		if strings.Contains(f.stdout.String(), absent) {
			t.Fatalf("%s is shown:\n%s", absent, f.stdout.String())
		}
	}
	// Read-only: nothing was collected, and every read is the plain one an
	// older coordinator also answers.
	if got := f.actions(); !reflect.DeepEqual(got, []string{"list:", "list-task:", "list-collections:"}) {
		t.Fatalf("node-wait operations = %v", got)
	}
	if f.operations[0].Host != "" || f.operations[0].Undelivered || f.operations[2].Request.ThreadID != "thread-mine" {
		t.Fatalf("operations = %+v", f.operations)
	}
	if len(f.queries) != 1 || f.queries[0].Kind != backlogadmin.QueryWorkflows || f.queries[0].ProgressMirror != nil || !reflect.DeepEqual(f.queries[0].Filter, backlogadmin.Filter{}) {
		t.Fatalf("queries = %+v", f.queries)
	}

	// The same view as JSON.
	f = newCollectFixture()
	if err := f.cli().run(context.Background(), []string{"uncollected", "--json"}); err != nil {
		t.Fatal(err)
	}
	document := decodeUncollected(t, f.stdout.Bytes())
	if document.SchemaVersion != 1 || document.Kind != "t3-steward.uncollected/v1" || document.Thread != "thread-mine" ||
		document.GeneratedAt != "2026-10-07T16:00:00Z" || !document.CollectionSupported {
		t.Fatalf("document header = %+v", document)
	}
	if !reflect.DeepEqual(document.Omitted, map[string]int{"collected": 1, "active": 1, "unknown": 1}) {
		t.Fatalf("omitted = %v", document.Omitted)
	}
	got := []string{}
	for _, run := range document.Runs {
		got = append(got, run.Run)
	}
	if !reflect.DeepEqual(got, []string{"run-ask", "run-branch", "run-failed", "run-rerun", "run-succeeded"}) {
		t.Fatalf("runs = %v", got)
	}
	ask := document.Runs[0]
	if ask.Name != "implement-x" || ask.Project != "steward" || ask.State != "waiting-external" || ask.FinishedAt != nil ||
		len(ask.Attention) != 1 || ask.Attention[0].Kind != "ask" || ask.Attention[0].Subject != "w-ask-12" || ask.Attention[0].Task != "implement" ||
		ask.Attention[0].Detail != "Which base?" || ask.Attention[0].Since == nil || *ask.Attention[0].Since != "2026-10-07T15:10:00Z" ||
		ask.Wake == nil || ask.Wake.WaitID != "nw-campaign-ask" || ask.Wake.Delivery != "pending" || ask.Wake.DeliveredAt != nil ||
		ask.Result != "t3-steward task result run-ask" || ask.Collect != "t3-steward campaign collect run-ask --thread thread-mine" {
		t.Fatalf("ask run = %+v", ask)
	}
	branch := document.Runs[1]
	if len(branch.Attention) != 1 || branch.Attention[0].Kind != "task-failed" || branch.Attention[0].Detail != "1 tasks failed; dependents are blocked" {
		t.Fatalf("branch run = %+v", branch)
	}
	failed := document.Runs[2]
	if failed.FinishedAt == nil || *failed.FinishedAt != "2026-10-07T15:50:00Z" || len(failed.Attention) != 1 || failed.Attention[0].Kind != "failed" ||
		failed.Wake == nil || failed.Wake.WaitID != "nw-campaign-failed-new" || failed.Wake.Delivery != "delivered" || failed.Wake.DeliveredAt == nil {
		t.Fatalf("failed run = %+v", failed)
	}
	for _, run := range document.Runs[3:] {
		if run.Attention == nil || len(run.Attention) != 0 {
			t.Fatalf("%s attention = %+v, want an empty array", run.Run, run.Attention)
		}
	}
}

// Each attention source on its own: an attention request, a needs-input run
// with no open wait, and a cancelled run.
func TestCampaignUncollectedAttentionKinds(t *testing.T) {
	f := newCollectFixture()
	registered := collectNow.Add(-5 * time.Minute)
	f.workflows = append(f.workflows,
		backlogadmin.WorkflowSummary{Run: domain.WorkflowRun{ID: "run-attention", Progress: domain.ProgressWaitingExternal}, Workflow: domain.Workflow{Name: "asks-operator"}},
		backlogadmin.WorkflowSummary{Run: domain.WorkflowRun{ID: "run-needs", Progress: domain.ProgressNeedsInput}, Workflow: domain.Workflow{Name: "needs"}},
		backlogadmin.WorkflowSummary{Run: domain.WorkflowRun{ID: "run-cancelled", Progress: domain.ProgressCancelled, CompletedAt: collectAt(1)}, Workflow: domain.Workflow{Name: "stopped"}},
	)
	for _, run := range []string{"run-attention", "run-needs", "run-cancelled"} {
		f.waits = append(f.waits, domain.NodeWait{Request: domain.NodeWaitRequest{ID: "nw-" + run, ThreadID: "thread-mine", Target: domain.NodeRef{RunID: run}}, Delivery: "pending"})
	}
	f.taskWaits = append(f.taskWaits, domain.TaskWait{ID: "w-attn", WorkflowRunID: "run-attention", TaskID: "deploy", Kind: domain.WaitKindAttention,
		Attention: &domain.AttentionRequest{Prompt: "Approve the deploy?"}, RegisteredAt: registered})
	if err := f.cli().run(context.Background(), []string{"uncollected", "--json"}); err != nil {
		t.Fatal(err)
	}
	document := decodeUncollected(t, f.stdout.Bytes())
	kinds := map[string][]string{}
	for _, run := range document.Runs {
		for _, a := range run.Attention {
			kinds[run.Run] = append(kinds[run.Run], a.Kind+"|"+a.Subject+"|"+a.Task+"|"+a.Detail)
		}
	}
	for run, want := range map[string][]string{
		"run-attention": {"attention|w-attn|deploy|Approve the deploy?"},
		"run-needs":     {"needs-input|run-needs||the run needs input and no open ask or attention request was found"},
		"run-cancelled": {"cancelled|run-cancelled||the run was cancelled"},
	} {
		if !reflect.DeepEqual(kinds[run], want) {
			t.Fatalf("%s attention = %v, want %v", run, kinds[run], want)
		}
	}
	if document.Runs[0].Run != "run-ask" || document.Runs[len(document.Runs)-4].Run != "run-cancelled" {
		t.Fatalf("order = %+v", document.Runs)
	}
}

func TestCampaignUncollectedEmptyView(t *testing.T) {
	f := newCollectFixture()
	if err := f.cli().run(context.Background(), []string{"uncollected", "--thread", "thread-nobody"}); err != nil {
		t.Fatal(err)
	}
	if got := f.stdout.String(); got != "nothing to collect for thread thread-nobody\n" {
		t.Fatalf("empty view = %q", got)
	}
	f = newCollectFixture()
	if err := f.cli().run(context.Background(), []string{"uncollected", "--thread=thread-nobody", "--json"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.stdout.String(), `"runs": []`) {
		t.Fatalf("runs is not an empty array:\n%s", f.stdout.String())
	}
}

func TestCampaignUncollectedThreadCurrentRefusedNotWidened(t *testing.T) {
	for _, verb := range [][]string{{"uncollected"}, {"uncollected", "--thread", "current"}, {"collect", "run-failed"}} {
		f := newCollectFixture()
		f.thread = func(string) (string, error) {
			return "", unresolvedThread("no T3 thread could be resolved from the caller's provider session (%s)", "none set")
		}
		err := f.cli().run(context.Background(), verb)
		if err == nil {
			t.Fatalf("%v with an unresolved thread succeeded:\n%s", verb, f.stdout.String())
		}
		consequence := "nothing was listed"
		if verb[0] == "collect" {
			consequence = "nothing was collected"
		}
		for _, want := range []string{"could not identify the calling agent's T3 thread", consequence, "t3-steward campaign " + verb[0] + " --thread <THREAD-ID>"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("%v refusal %q does not contain %q", verb, err, want)
			}
		}
		if exitCodeFor(err) != 1 {
			t.Fatalf("%v exit = %d, want 1", verb, exitCodeFor(err))
		}
		if len(f.operations) != 0 || len(f.queries) != 0 || f.stdout.Len() != 0 {
			t.Fatalf("%v reached the coordinator: %v %v", verb, f.actions(), f.queries)
		}
	}
}

func TestCampaignCollectArgumentRefusals(t *testing.T) {
	for _, args := range [][]string{
		{"collect"},
		{"collect", "--thread", "current"},
		{"collect", "run-a", "run-b/task"},
		{"collect", "run-a", ""},
		{"collect", "run-a", "run-a"},
		{"collect", "run-a", strings.Repeat("r", 257)},
		{"collect", "run-a", "--thread"},
		{"collect", "run-a", "--thread", ""},
		{"collect", "run-a", "--thread", "a/b"},
		{"collect", "run-a", "--thread", "a", "--thread", "b"},
		{"collect", "run-a", "--force"},
		{"uncollected", "run-a"},
		{"uncollected", "--thread"},
		{"uncollected", "--limit", "3"},
	} {
		f := newCollectFixture()
		err := f.cli().run(context.Background(), args)
		if err == nil || exitCodeFor(err) != 1 {
			t.Fatalf("%q: err = %v (exit %d), want a client refusal", args, err, exitCodeFor(err))
		}
		if len(f.operations) != 0 || len(f.queries) != 0 {
			t.Fatalf("%q reached the coordinator: %v", args, f.actions())
		}
	}
}

func TestCampaignCollectRecordsInOrderAndStopsAtRefusal(t *testing.T) {
	f := newCollectFixture()
	f.refuse["run-branch"] = &backlogadmin.TransportError{Class: backlogadmin.ClassRejected, Operation: "node-wait",
		Err: errors.New("run collection refused: run run-branch is active and cannot be collected; only a finished run is collected")}
	err := f.cli().run(context.Background(), []string{"collect", "run-succeeded", "run-failed", "run-branch", "run-rerun", "--thread", "current"})
	if err == nil || exitCodeFor(err) != 8 {
		t.Fatalf("err = %v (exit %d), want the coordinator's rejection, exit 8", err, exitCodeFor(err))
	}
	if got := f.collected(); !reflect.DeepEqual(got, []string{"run-succeeded", "run-failed", "run-branch"}) {
		t.Fatalf("sent %v: it must send in argument order and stop at the refusal", got)
	}
	for _, op := range f.operations {
		if op.Action == backlogadmin.RunCollectAction && op.Request.ThreadID != "thread-mine" {
			t.Fatalf("collect-run sent for thread %q", op.Request.ThreadID)
		}
	}
	wantOut := "collected run-succeeded (succeeded, finished 2026-10-07T15:00:00Z) for thread thread-mine\n" +
		"collected run-failed (failed, finished 2026-10-07T15:50:00Z) for thread thread-mine\n"
	if f.stdout.String() != wantOut {
		t.Fatalf("stdout = %q\nwant     %q", f.stdout.String(), wantOut)
	}
	for _, want := range []string{"run-branch", "is active and cannot be collected", "run-succeeded, run-failed were collected before it", "run-rerun was not sent"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not contain %q", err, want)
		}
	}

	// All recorded: exit 0.
	f = newCollectFixture()
	if err := f.cli().run(context.Background(), []string{"collect", "run-succeeded", "run-failed"}); err != nil {
		t.Fatal(err)
	}
}

// The coordinator refuses an active run, and the refusal names the run's
// attention and the command that clears it: collecting is not how attention
// on a running run goes away.
func TestCampaignCollectRefusesActiveRunWithAttentionHint(t *testing.T) {
	f := newCollectFixture()
	f.refuse["run-ask"] = &backlogadmin.TransportError{Class: backlogadmin.ClassRejected, Operation: "node-wait",
		Err: errors.New("run collection refused: run run-ask is waiting-external and cannot be collected; only a finished run is collected")}
	err := f.cli().run(context.Background(), []string{"collect", "run-ask"})
	if err == nil || exitCodeFor(err) != 8 {
		t.Fatalf("err = %v (exit %d), want exit 8", err, exitCodeFor(err))
	}
	for _, want := range []string{
		"run run-ask is waiting-external and cannot be collected; only a finished run is collected",
		"Its attention clears when the ask is answered: t3-steward ask answer w-ask-12 --option ...",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not contain %q", err, want)
		}
	}
	if f.stdout.Len() != 0 || !reflect.DeepEqual(f.collected(), []string{"run-ask"}) {
		t.Fatalf("stdout = %q, sent %v", f.stdout.String(), f.collected())
	}
	// Without an open wait, the hint says what does clear it.
	f = newCollectFixture()
	f.refuse["run-branch"] = &backlogadmin.TransportError{Class: backlogadmin.ClassRejected, Operation: "node-wait",
		Err: errors.New("run collection refused: run run-branch is active and cannot be collected; only a finished run is collected")}
	err = f.cli().run(context.Background(), []string{"collect", "run-branch"})
	if err == nil || !strings.Contains(err.Error(), "t3-steward campaign show run-branch --wait") {
		t.Fatalf("err = %v", err)
	}
}

func TestCampaignCollectJSON(t *testing.T) {
	f := newCollectFixture()
	if err := f.cli().run(context.Background(), []string{"collect", "run-failed", "run-succeeded", "--json"}); err != nil {
		t.Fatal(err)
	}
	var document struct {
		SchemaVersion int    `json:"schemaVersion"`
		Thread        string `json:"thread"`
		Collected     []struct {
			Run         string  `json:"run"`
			Progress    string  `json:"progress"`
			CompletedAt *string `json:"completedAt"`
			CollectedAt string  `json:"collectedAt"`
			Changed     bool    `json:"changed"`
		} `json:"collected"`
	}
	decoder := json.NewDecoder(bytes.NewReader(f.stdout.Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		t.Fatalf("%v:\n%s", err, f.stdout.String())
	}
	if document.SchemaVersion != 1 || document.Thread != "thread-mine" || len(document.Collected) != 2 {
		t.Fatalf("document = %+v", document)
	}
	first := document.Collected[0]
	if first.Run != "run-failed" || first.Progress != "failed" || first.CompletedAt == nil || *first.CompletedAt != "2026-10-07T15:50:00Z" ||
		first.CollectedAt != "2026-10-07T16:00:00Z" || !first.Changed || document.Collected[1].Run != "run-succeeded" {
		t.Fatalf("collected = %+v", document.Collected)
	}
}

// olderCoordinatorAnswers are the two shapes an rc.117 coordinator's answer
// to an unknown node-wait action takes on the way to this client.
func olderCoordinatorAnswers() map[string]error {
	return map[string]error{
		"direct": &backlogadmin.TransportError{Class: backlogadmin.ClassRejected, Operation: "node-wait", Err: errors.New("unknown native wait action")},
		"ssh-wrapped": &backlogadmin.TransportError{Class: backlogadmin.ClassRejected, Operation: "node-wait",
			Err: fmt.Errorf("%w: %s", errors.New("unknown native wait action"), "Warning: Permanently added 'coordinator' to the list of known hosts.")},
	}
}

func TestCampaignCollectOlderCoordinatorExit7(t *testing.T) {
	for name, answer := range olderCoordinatorAnswers() {
		f := newCollectFixture()
		f.collectErr = answer
		err := f.cli().run(context.Background(), []string{"collect", "run-failed", "run-succeeded"})
		if exitCodeFor(err) != 7 || !strings.Contains(err.Error(), "the coordinator does not record collected runs; upgrade it") {
			t.Fatalf("%s: err = %v (exit %d)", name, err, exitCodeFor(err))
		}
		if got := f.actions(); !reflect.DeepEqual(got, []string{"collect-run:run-failed"}) || f.stdout.Len() != 0 {
			t.Fatalf("%s: sent %v, printed %q", name, got, f.stdout.String())
		}
	}
	// Any other failure keeps its own class and text.
	unrelated := &backlogadmin.TransportError{Class: backlogadmin.ClassUnavailable, Operation: "node-wait", Err: errors.New("connect to backlog-v2 coordinator: refused")}
	f := newCollectFixture()
	f.collectErr = unrelated
	err := f.cli().run(context.Background(), []string{"collect", "run-failed"})
	if exitCodeFor(err) != 5 || !errors.Is(err, unrelated) || strings.Contains(err.Error(), "upgrade") {
		t.Fatalf("unrelated: err = %v (exit %d)", err, exitCodeFor(err))
	}
}

func TestCampaignUncollectedOlderCoordinatorDegrades(t *testing.T) {
	for name, answer := range olderCoordinatorAnswers() {
		f := newCollectFixture()
		f.listCollectionsErr = answer
		if err := f.cli().run(context.Background(), []string{"uncollected"}); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		lines := strings.Split(strings.TrimRight(f.stdout.String(), "\n"), "\n")
		if !strings.HasPrefix(lines[0], "note: the coordinator does not record collected runs; upgrade it") {
			t.Fatalf("%s: first line = %q", name, lines[0])
		}
		// Nothing is collected: both runs a newer coordinator would hide as
		// collected are listed.
		if !strings.Contains(f.stdout.String(), "run-collected") || !strings.Contains(f.stdout.String(), "run-rerun") {
			t.Fatalf("%s: a run was treated as collected:\n%s", name, f.stdout.String())
		}

		f = newCollectFixture()
		f.listCollectionsErr = answer
		if err := f.cli().run(context.Background(), []string{"uncollected", "--json"}); err != nil {
			t.Fatalf("%s json: %v", name, err)
		}
		document := decodeUncollected(t, f.stdout.Bytes())
		if document.CollectionSupported || !strings.Contains(document.Note, "upgrade it") || document.Omitted["collected"] != 0 || len(document.Runs) != 6 {
			t.Fatalf("%s json = %+v", name, document)
		}
	}
	unrelated := &backlogadmin.TransportError{Class: backlogadmin.ClassTimeout, Operation: "node-wait", Err: errors.New("deadline exceeded")}
	f := newCollectFixture()
	f.listCollectionsErr = unrelated
	err := f.cli().run(context.Background(), []string{"uncollected"})
	if exitCodeFor(err) != 6 || !errors.Is(err, unrelated) || f.stdout.Len() != 0 {
		t.Fatalf("unrelated: err = %v (exit %d), printed %q", err, exitCodeFor(err), f.stdout.String())
	}
}

func TestCampaignCollectHelpPagesRender(t *testing.T) {
	for _, verb := range []string{"collect", "uncollected"} {
		// cmdCampaign answers help through admitCampaignHelp before any
		// configuration is read or any argument parsed.
		var out bytes.Buffer
		if handled, err := admitCampaignHelp(&out, []string{verb, "--help"}); !handled || err != nil {
			t.Fatalf("%s --help: handled=%v err=%v", verb, handled, err)
		}
		body := out.String()
		for _, want := range []string{"t3-steward campaign " + verb, "--thread", "upgrade", "node wait", "Only a finished run is collected", "Nothing is collected implicitly"} {
			if !strings.Contains(body, want) {
				t.Fatalf("campaign %s help does not mention %q:\n%s", verb, want, body)
			}
		}
	}
}
