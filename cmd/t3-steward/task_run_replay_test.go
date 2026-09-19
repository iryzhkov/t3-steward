package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// wakePromise is the line that tells the agent to end its turn. It is the only
// sentence in the record an agent acts on by doing nothing, so it is the one
// sentence that must never be printed without a wait behind it.
const wakePromise = "End this turn now"

// contractAllowsWakePromise restates contract 1's rule in the test, separately
// from the production predicate, so that what is checked is the contract and
// not the implementation agreeing with itself.
//
// A wake promise is allowed only when a wait exists that will fire for the
// calling thread: a wait was registered, its wake is deliverable to this host,
// it has not already been delivered or cancelled, it is held for this thread
// and not another, and the run it watches is still running.
func contractAllowsWakePromise(record taskRunRecord) bool {
	switch {
	case record.Notify == nil || record.Notify.WaitID == "":
		return false
	case record.Notify.Undeliverable != "":
		return false
	case record.Notify.Delivery == "delivered" || record.Notify.Delivery == "cancelled":
		return false
	case record.Notify.WaitThreadID != "" && record.Notify.WaitThreadID != record.Notify.ThreadID:
		return false
	case record.ProgressUnavailable != "":
		return false
	case record.Progress != "" && domain.ProgressState(record.Progress).Terminal():
		return false
	}
	return true
}

// Test 1 of the stage. The wake promise and the wait are the same fact.
//
// A-1 is one case of this rule and not the rule: a replay of a terminal run is
// the case that was observed, and a replay of a live run whose earlier wake was
// already delivered is the case the audit left open as U-A. Both are states in
// the sweep below, and so is every other combination of delivery state, run
// progress and thread ownership, because the defect is not "the terminal branch
// is missing" but "the promise is printed from a branch that never looks at
// whether a wait will fire".
func TestTheWakePromiseIsPrintedOnlyWhenAWaitWillFire(t *testing.T) {
	states := []string{"", "pending", "held", "sending", "recovery-required", "delivered", "cancelled"}
	progresses := []string{"", string(domain.ProgressQueued), string(domain.ProgressActive),
		string(domain.ProgressSucceeded), string(domain.ProgressFailed), string(domain.ProgressCancelled),
		string(domain.ProgressSkipped)}
	covered := 0
	for _, delivery := range states {
		for _, progress := range progresses {
			for _, undeliverable := range []string{"", "the coordinator records normandy as this wait's delivery host"} {
				for _, waitThread := range []string{"", "thread-1", "thread-other"} {
					for _, unavailable := range []string{"", "the coordinator did not answer"} {
						record := taskRunRecord{
							SchemaVersion: taskRunSchemaVersion, Run: "run-1", Tasks: []string{"task"},
							Project: "steward", Ref: "main", IdempotencyKey: "run-abc",
							Replayed: progress != "", Progress: progress, ProgressUnavailable: unavailable,
							Result: "t3-steward task result run-1",
							Notify: &campaignNotification{
								WaitID: "nw-campaign-run-abc", ThreadID: "thread-1", Target: "run-1/sink",
								Delivery: delivery, Host: "omarchy-pc", WaitThreadID: waitThread,
								Undeliverable: undeliverable,
							},
						}
						assertWakePromiseMatchesTheWait(t, record)
						covered++
					}
				}
			}
		}
	}
	if covered < 500 {
		t.Fatalf("only %d states swept; the property proves too little", covered)
	}
	// The two states with no wait at all: --no-notify, and a registration that
	// came back without an ID. Neither may promise a wake either.
	for _, record := range []taskRunRecord{
		{Run: "run-1", Result: "t3-steward task result run-1"},
		{Run: "run-1", Result: "t3-steward task result run-1", Notify: &campaignNotification{ThreadID: "thread-1"}},
	} {
		assertWakePromiseMatchesTheWait(t, record)
	}
}

func assertWakePromiseMatchesTheWait(t *testing.T, record taskRunRecord) {
	t.Helper()
	var out bytes.Buffer
	if err := renderTaskRunRecord(&out, record); err != nil {
		t.Fatal(err)
	}
	printed := strings.Contains(out.String(), wakePromise)
	want := contractAllowsWakePromise(record)
	if printed == want {
		return
	}
	if printed {
		t.Fatalf("the record told the agent to end its turn and no wait will fire for it.\n%s", out.String())
	}
	t.Fatalf("a wait will fire and the record did not say to end the turn.\n%s", out.String())
}

// Contract 1, the branch A-1 observed: a replay of a run that already ended
// states that run's outcome, names the result command, and prints neither the
// viability of the manifest it composed nor the wake instruction.
func TestAReplayOfATerminalRunReportsTheOutcomeAndNoWake(t *testing.T) {
	h := newTaskRunHarness()
	h.replay = true
	h.workflow = workflowAt(domain.ProgressSucceeded)
	if err := h.run("--model", "opus", "--", "work"); err != nil {
		t.Fatal(err)
	}
	text := h.stdout.String()
	for _, want := range []string{"replayed: true", "progress succeeded", "t3-steward task result run-1"} {
		if !strings.Contains(text, want) {
			t.Fatalf("the replay does not say %q:\n%s", want, text)
		}
	}
	for _, unwanted := range []string{wakePromise, "check "} {
		if strings.Contains(text, unwanted) {
			t.Fatalf("the replay of a finished run still printed %q:\n%s", unwanted, text)
		}
	}
	if len(h.notified) != 0 {
		t.Fatalf("a wake was registered for a run that already ended: %+v", h.notified)
	}
}

// U-A, the case the audit could not close: the replayed run is still live and
// the wait its key derives was already delivered. The earlier wake will not
// repeat, so a fresh one is registered before anything is promised.
func TestAReplayWhoseEarlierWakeWasDeliveredRegistersAFreshOne(t *testing.T) {
	h := newTaskRunHarness()
	h.replay = true
	h.workflow = workflowAt(domain.ProgressActive)
	h.deliveredWaitIDs = map[string]bool{"nw-campaign-": true}
	if err := h.run("--model", "opus", "--", "work"); err != nil {
		t.Fatal(err)
	}
	if len(h.notified) != 2 {
		t.Fatalf("registrations = %d, want the spent one and a fresh one: %+v", len(h.notified), h.notified)
	}
	text := h.stdout.String()
	if !strings.Contains(text, wakePromise) {
		t.Fatalf("a fresh wait was registered and the record did not say to end the turn:\n%s", text)
	}
	if !strings.Contains(text, "delivery=pending") {
		t.Fatalf("the record does not print the wait's delivery state:\n%s", text)
	}
}

// Degraded: the replayed run's progress cannot be read. The record says so,
// names diagnose, and promises nothing.
func TestAReplayWhoseProgressCannotBeReadPromisesNoWake(t *testing.T) {
	h := newTaskRunHarness()
	h.replay = true
	h.workflowErr = errors.New("coordinator unavailable")
	if err := h.run("--model", "opus", "--", "work"); err != nil {
		t.Fatal(err)
	}
	text := h.stdout.String()
	for _, want := range []string{"replayed: true", "progress unknown", "t3-steward diagnose run-1"} {
		if !strings.Contains(text, want) {
			t.Fatalf("the degraded replay does not say %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, wakePromise) {
		t.Fatalf("an unreadable run still produced a wake promise:\n%s", text)
	}
}

// Contract 2: --notify-thread names the thread on this verb too, which is the
// only way a plain shell, an ssh session or a script on another host can ask
// "task run" for a wake.
func TestTaskRunAcceptsNotifyThread(t *testing.T) {
	h := newTaskRunHarness()
	h.thread = "thread-named"
	if err := h.run("--model", "opus", "--notify-thread", "thread-named", "--json", "--", "work"); err != nil {
		t.Fatal(err)
	}
	if record := h.record(t); record.Notify == nil || record.Notify.ThreadID != "thread-named" {
		t.Fatalf("notify = %+v, want the named thread", record.Notify)
	}

	// It contradicts --no-notify rather than quietly winning over it.
	both := newTaskRunHarness()
	err := both.run("--model", "opus", "--notify-thread", "current", "--no-notify", "--", "work")
	if err == nil || !strings.Contains(err.Error(), "contradict") {
		t.Fatalf("error = %v", err)
	}
}

// workflowAt is the coordinator's answer about a run at one progress.
func workflowAt(progress domain.ProgressState) *backlogadmin.WorkflowDetail {
	return &backlogadmin.WorkflowDetail{
		Summary: backlogadmin.WorkflowSummary{
			Run: domain.WorkflowRun{ID: "run-1", WorkflowID: "workflow-1", Progress: progress},
		},
	}
}
