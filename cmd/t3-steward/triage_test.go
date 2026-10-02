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

// triageFixture is a coordinator with one of everything triage reports and
// one of each thing it must leave out.
type triageFixture struct {
	now        time.Time
	operations []backlogadmin.SupervisionOperation
	failWaits  bool
}

func (f *triageFixture) sources() triageSources {
	now := f.now
	ago := func(d time.Duration) time.Time { return now.Add(-d) }
	ptr := func(t time.Time) *time.Time { return &t }
	run := func(id string, progress domain.ProgressState, updated time.Time, revision int64, supervised bool) backlogadmin.WorkflowSummary {
		summary := backlogadmin.WorkflowSummary{
			Run:      domain.WorkflowRun{ID: id, WorkflowID: "wf-" + id, Progress: progress, Revision: revision, UpdatedAt: updated, CreatedAt: updated.Add(-time.Hour)},
			Workflow: domain.Workflow{ID: "wf-" + id, Name: "campaign " + id},
		}
		if supervised {
			summary.Run.Supervision = &domain.SupervisionRecord{RunID: id}
		}
		return summary
	}
	query := func(_ context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
		response := backlogadmin.Response{Version: backlogadmin.Version, Kind: q.Kind, GeneratedAt: now}
		switch q.Kind {
		case backlogadmin.QueryStatus:
			response.Status = &backlogadmin.Status{Runtime: backlogadmin.RuntimeStatus{Owner: "normandy-coordinator", LastReload: ago(24 * time.Hour)}}
		case backlogadmin.QueryWorkers:
			response.Workers = []backlogadmin.Worker{
				enrolledWorker("normandy", false, ago(62*time.Minute)),
				enrolledWorker("omarchy-pc", false, ago(3*time.Minute)),
				enrolledWorker("homelab", true, now),
			}
		case backlogadmin.QueryQuota:
			response.Quotas = []backlogadmin.Quota{
				{Pool: domain.QuotaPool{ID: "claude-main"}, Admission: &domain.QuotaAdmissionRecord{QuotaPoolID: "claude-main", Admission: domain.AdmissionClosed, Reason: "five_hour at 96%", AppliedAt: ago(time.Hour)}},
				{Pool: domain.QuotaPool{ID: "codex-main"}, Admission: &domain.QuotaAdmissionRecord{QuotaPoolID: "codex-main", Admission: domain.AdmissionOpen}},
			}
		case backlogadmin.QueryWorkflows:
			response.Workflows = []backlogadmin.WorkflowSummary{
				run("run-stalled", domain.ProgressActive, ago(9*24*time.Hour), 14, false),
				run("run-fresh", domain.ProgressActive, ago(time.Hour), 3, false),
				run("run-done", domain.ProgressSucceeded, ago(20*24*time.Hour), 9, false),
				run("run-sup", domain.ProgressActive, ago(time.Hour), 21, true),
				run("run-settled-sup", domain.ProgressCancelled, ago(48*time.Hour), 30, true),
			}
		case backlogadmin.QueryQuarantine:
			response.Quarantine = []backlogadmin.QuarantinedIntake{{Key: "intake-7", Reason: "manifest invalid", QuarantinedAt: ago(time.Hour)}}
		default:
			return backlogadmin.Response{}, errors.New("unexpected query " + string(q.Kind))
		}
		return response, nil
	}
	nodeWait := func(_ context.Context, op backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
		if f.failWaits {
			return backlogadmin.NodeWaitResponse{}, &backlogadmin.TransportError{Class: backlogadmin.ClassUnavailable, Err: errors.New("connection refused")}
		}
		node := func(id, host, thread, delivery string, settled *time.Time, deadline time.Time) domain.NodeWait {
			return domain.NodeWait{Request: domain.NodeWaitRequest{ID: id, ThreadID: thread, Name: "wait " + id},
				Host: host, SettledAt: settled, Deadline: deadline, CreatedAt: deadline.Add(-24 * time.Hour), Delivery: delivery}
		}
		switch op.Action {
		case "list":
			return backlogadmin.NodeWaitResponse{Waits: []domain.NodeWait{
				node("nw-pend", "omarchy-pc", "thread-1", "pending", ptr(ago(48*time.Hour)), ago(48*time.Hour)),
				node("nw-off", "laptop", "thread-2", "offline", ptr(ago(3*time.Hour)), ago(3*time.Hour)),
				node("nw-unknown", "laptop", "thread-7", "recovery-required", ptr(ago(3*time.Hour)), ago(3*time.Hour)),
				node("nw-ok", "laptop", "thread-3", "delivered", ptr(ago(3*time.Hour)), ago(3*time.Hour)),
				node("nw-rejected", "laptop", "thread-3", "rejected", ptr(ago(3*time.Hour)), ago(3*time.Hour)),
				node("nw-fresh", "laptop", "thread-4", "pending", ptr(ago(time.Minute)), now.Add(time.Hour)),
				node("nw-live", "laptop", "thread-5", "pending", nil, now.Add(time.Hour)),
				node("nw-late", "laptop", "thread-6", "pending", nil, ago(2*time.Hour)),
			}}, nil
		case "list-task":
			return backlogadmin.NodeWaitResponse{TaskWaits: []domain.TaskWait{
				{ID: "tw-attn", WorkflowRunID: "run-fresh", TaskID: "task-a", Kind: domain.WaitKindAttention, RegisteredAt: ago(2 * time.Hour), Deadline: now.Add(time.Hour),
					Attention: &domain.AttentionRequest{Prompt: "amber or violet?"}},
				{ID: "tw-stuck", WorkflowRunID: "run-fresh", TaskID: "task-b", Kind: domain.WaitKindShell, RegisteredAt: ago(3 * time.Hour), Deadline: now.Add(time.Hour),
					SettledAt: ptr(ago(time.Hour)), Delivery: "recovery-required"},
				{ID: "tw-done", WorkflowRunID: "run-fresh", TaskID: "task-c", Kind: domain.WaitKindShell, RegisteredAt: ago(3 * time.Hour), Deadline: now.Add(time.Hour),
					SettledAt: ptr(ago(time.Hour)), Delivery: "delivered"},
			}}, nil
		}
		return backlogadmin.NodeWaitResponse{}, errors.New("unexpected wait action " + op.Action)
	}
	supervise := func(_ context.Context, request backlogadmin.SupervisionRequest) (backlogadmin.SupervisionResponse, error) {
		f.operations = append(f.operations, request.Operation)
		response := backlogadmin.SupervisionResponse{Operation: request.Operation, RunID: request.RunID}
		switch request.RunID {
		case "run-sup":
			response.State = &backlogadmin.SupervisionState{
				Record:     domain.SupervisionRecord{RunID: "run-sup", Revision: 12, ActivationsUsed: 1, BudgetGrantedActivations: 3},
				Activation: domain.Activation{ID: "act-1", State: domain.ActivationSpent, Outcome: domain.ActivationOutcomeNoDecision},
				Incidents: []backlogadmin.SupervisionIncidentView{{Incident: domain.ReviewIncident{
					ID: "inc-1", RunID: "run-sup", Revision: 3, State: domain.IncidentEscalated, Reason: "review ended without a decision"}}},
				Gates: []backlogadmin.SupervisionGateView{{Gate: domain.Gate{
					Definition: domain.GateDefinition{ID: "gate-1", Name: "design"}, State: domain.GateEscalated, Revision: 5, GraphRevision: 7, EvidenceSnapshotID: "snap-1"}}},
			}
		case "run-settled-sup":
			response.State = &backlogadmin.SupervisionState{
				Record:      domain.SupervisionRecord{RunID: "run-settled-sup", Revision: 40},
				SinkSettled: true,
				Holds:       []domain.Hold{{ID: "hold-1", RunID: "run-settled-sup", State: domain.HoldActive, Scope: domain.HoldScope{Kind: domain.HoldScopeBranch, BranchRootTaskID: "build"}}},
			}
		}
		return response, nil
	}
	return triageSources{query: query, nodeWait: nodeWait, supervise: supervise}
}

func runTriageFixture(t *testing.T, fixture *triageFixture, asJSON bool) (string, error) {
	t.Helper()
	var out bytes.Buffer
	err := runTriage(context.Background(), fixture.sources(), triageOptions{
		staleAfter: 7 * 24 * time.Hour, workerDownAfter: 10 * time.Minute, asJSON: asJSON,
	}, &out)
	return out.String(), err
}

// One command lists everything that is waiting for an operator, each with a
// command that can be run as printed: the ids and revisions are filled in.
// What needs nobody -- a connected worker, an open pool, a delivered wake, a
// run that moved an hour ago, a finished run -- is not listed.
func TestTriageListsWhatNeedsAnOperatorWithReadyCommands(t *testing.T) {
	fixture := &triageFixture{now: time.Date(2026, 10, 2, 4, 40, 0, 0, time.UTC)}
	text, err := runTriageFixture(t, fixture, false)
	if err != nil {
		t.Fatalf("triage failed: %v\n%s", err, text)
	}
	for _, want := range []string{
		// Workers.
		"worker-down normandy",
		"on normandy: systemctl --user restart t3-steward-worker",
		"worker-disconnected omarchy-pc",
		// Supervision.
		"t3-steward campaign supervision reassess run-sup --expected-revision 12 --request-id triage-reassess-run-sup-r12",
		"t3-steward campaign supervision resolve run-sup --incident inc-1 --expected-revision 3 --outcome remediated --request-id triage-resolve-inc-1-r3",
		"t3-steward campaign supervision resolve run-sup --incident inc-1 --expected-revision 3 --outcome cancelled",
		"t3-steward campaign supervision decide run-sup --gate gate-1 --accept --evidence snap-1 --expected-revision 5 --graph-revision 7",
		"supervision-hold run-settled-sup",
		// Questions, wakes and pools.
		"t3-steward wait inspect tw-attn",
		"wake-overdue nw-pend",
		"on omarchy-pc: systemctl --user status t3-steward",
		"wake-undeliverable nw-off",
		"t3-steward wait cancel nw-off",
		"wake-overdue nw-late",
		"wake-undeliverable tw-stuck",
		"quota-held claude-main",
		"intake-quarantined 1 submission: newest intake-7",
		"$ t3-steward backlog quarantine\n",
		// Stalled runs.
		"run-stalled run-stalled",
		"t3-steward campaign show run-stalled",
		"t3-steward campaign cancel run-stalled --reason",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("triage output lacks %q:\n%s", want, text)
		}
	}
	for _, absent := range []string{"homelab", "codex-main", "nw-ok", "nw-rejected", "nw-fresh", "nw-live", "tw-done", "run-done", "run-fresh "} {
		if strings.Contains(text, absent) {
			t.Fatalf("triage listed %q, which needs nobody:\n%s", absent, text)
		}
	}
	// A wake whose send outcome is unknown cannot be cancelled (the store
	// refuses it), so no cancel is offered for it.
	if !strings.Contains(text, "wake-undeliverable nw-unknown") || strings.Contains(text, "wait cancel nw-unknown") {
		t.Fatalf("the recovery-required wake:\n%s", text)
	}
	// conclude-failure is offered only for an incident whose source task
	// terminally failed; this one's did not.
	if strings.Contains(text, "--outcome conclude-failure") {
		t.Fatalf("conclude-failure was offered for an incident whose task did not fail:\n%s", text)
	}
	// The hold of a settled run cannot be released (every supervision mutation
	// is revoked once the run settles), so no release is offered for it.
	if strings.Contains(text, "supervision release run-settled-sup") {
		t.Fatalf("a release was offered on a settled run:\n%s", text)
	}
	for _, operation := range fixture.operations {
		if operation != backlogadmin.SupervisionShow {
			t.Fatalf("triage sent a %s, but it is read-only", operation)
		}
	}
}

// The JSON form is one versioned document that carries the same items and
// commands as the text.
func TestTriageJSONIsVersioned(t *testing.T) {
	fixture := &triageFixture{now: time.Date(2026, 10, 2, 4, 40, 0, 0, time.UTC)}
	text, err := runTriageFixture(t, fixture, true)
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Version         string `json:"version"`
		Coordinator     string `json:"coordinator"`
		StaleAfterDays  int    `json:"staleAfterDays"`
		WorkerDownAfter string `json:"workerDownAfter"`
		Items           []struct {
			Kind     string `json:"kind"`
			Severity string `json:"severity"`
			Subject  string `json:"subject"`
			Commands []struct {
				Run  string `json:"run"`
				Host string `json:"host"`
			} `json:"commands"`
		} `json:"items"`
		Sources []string `json:"sources"`
	}
	if err := json.Unmarshal([]byte(text), &document); err != nil {
		t.Fatalf("not one JSON document: %v\n%s", err, text)
	}
	if document.Version != "t3-steward.triage/v1" || document.Coordinator != "normandy-coordinator" ||
		document.StaleAfterDays != 7 || document.WorkerDownAfter != "10m0s" {
		t.Fatalf("header = %+v", document)
	}
	kinds := map[string]int{}
	for _, item := range document.Items {
		kinds[item.Kind]++
		if len(item.Commands) == 0 {
			t.Fatalf("item %s %s has no command", item.Kind, item.Subject)
		}
	}
	for kind, want := range map[string]int{
		"worker-down": 1, "worker-disconnected": 1, "supervision-reassess": 1, "supervision-incident": 1,
		"supervision-gate": 1, "supervision-hold": 1, "needs-input": 1, "wake-overdue": 2,
		"wake-undeliverable": 3, "quota-held": 1, "intake-quarantined": 1, "run-stalled": 1,
	} {
		if kinds[kind] != want {
			t.Fatalf("%d %s items, want %d (%v)", kinds[kind], kind, want, kinds)
		}
	}
	if document.Items[0].Severity != "action" {
		t.Fatalf("the first item is %s; items needing action come first", document.Items[0].Severity)
	}
}

// A source that cannot be read is named, the rest are still listed, and the
// exit carries the transport class: an empty section is never mistaken for
// "nothing is waiting".
func TestTriageNamesASourceItCouldNotRead(t *testing.T) {
	fixture := &triageFixture{now: time.Date(2026, 10, 2, 4, 40, 0, 0, time.UTC), failWaits: true}
	text, err := runTriageFixture(t, fixture, false)
	if err == nil {
		t.Fatalf("an unreadable source exited 0:\n%s", text)
	}
	if backlogadmin.ClassOf(err) != backlogadmin.ClassUnavailable {
		t.Fatalf("the exit class is %q, want unavailable: %v", backlogadmin.ClassOf(err), err)
	}
	if !strings.Contains(text, "NOT READ: waits:") || !strings.Contains(text, "worker-down normandy") {
		t.Fatalf("the partial answer:\n%s", text)
	}
}

// An empty triage says so, and says what it looked at.
func TestTriageWithNothingWaitingSaysSo(t *testing.T) {
	now := time.Date(2026, 10, 2, 4, 40, 0, 0, time.UTC)
	sources := triageSources{
		query: func(_ context.Context, q backlogadmin.Query) (backlogadmin.Response, error) {
			response := backlogadmin.Response{GeneratedAt: now}
			if q.Kind == backlogadmin.QueryWorkers {
				response.Workers = []backlogadmin.Worker{enrolledWorker("homelab", true, now)}
			}
			return response, nil
		},
		nodeWait: func(context.Context, backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error) {
			return backlogadmin.NodeWaitResponse{}, nil
		},
		supervise: func(context.Context, backlogadmin.SupervisionRequest) (backlogadmin.SupervisionResponse, error) {
			return backlogadmin.SupervisionResponse{}, nil
		},
	}
	var out bytes.Buffer
	if err := runTriage(context.Background(), sources, triageOptions{staleAfter: 7 * 24 * time.Hour}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Nothing needs an operator") {
		t.Fatalf("empty triage:\n%s", out.String())
	}
}

// The flags are the two thresholds and --json, and a positional argument is
// refused rather than ignored.
func TestParseTriageArgs(t *testing.T) {
	options, err := parseTriageArgs([]string{"--stale-days", "3", "--json"})
	if err != nil || options.staleAfter != 3*24*time.Hour || !options.asJSON {
		t.Fatalf("options = %+v, %v", options, err)
	}
	if options, err := parseTriageArgs(nil); err != nil || options.staleAfter != 7*24*time.Hour {
		t.Fatalf("defaults = %+v, %v", options, err)
	}
	for _, args := range [][]string{{"run-1"}, {"--stale-days", "0"}, {"--stale-days"}, {"--bogus"}} {
		if _, err := parseTriageArgs(args); err == nil {
			t.Fatalf("%q was accepted", args)
		}
	}
}
