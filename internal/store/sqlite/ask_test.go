package sqlite

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/ownernotify"
)

func askRegistration(attempt domain.Attempt, requestID string, ask domain.AskRequest, deadline time.Duration) domain.TaskWaitRegistration {
	return domain.TaskWaitRegistration{
		RequestID: requestID, WorkflowRunID: attempt.WorkflowRunID, TaskID: attempt.TaskID,
		AttemptID: attempt.ID, IssuedRevision: attempt.Revision, ThreadID: attempt.ThreadID,
		Wake: domain.WakeEach, MaxDuration: deadline, Kind: domain.WaitKindAsk, Ask: &ask,
	}
}

func pickAsk() domain.AskRequest {
	return domain.AskRequest{
		Question: "M6b field test: pick one", Options: []string{"alpha", "beta"},
		OnDeadline: domain.AskDeadlineDefault, Default: []string{"alpha"},
	}
}

// An ask parks its attempt like any task-bound wait, cannot be settled by a
// check result, and once answered from the CLI resumes the same attempt with
// the ask-answer/v1 document in its wake.
func TestAskParksAndAnAnswerResumesTheTaskWithTheDocument(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	wait, err := store.RegisterTaskWait(ctx, askRegistration(attempt, "ask-1", pickAsk(), 2*time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	if parked := loadAttempt(t, store, attempt.ID); parked.Progress != domain.ProgressWaitingExternal {
		t.Fatalf("an ask did not park its attempt: %q", parked.Progress)
	}
	if wait.Ask == nil || wait.Condition != "ask: M6b field test: pick one" {
		t.Fatalf("registered ask = %+v", wait)
	}
	if _, err := store.SettleTaskWait(ctx, wait.ID, domain.TaskWaitResult{Outcome: domain.TaskWaitMet}, now); err == nil {
		t.Fatal("a check result settled an ask")
	}
	if _, err := store.AnswerAsk(ctx, domain.AskAnswer{AskID: wait.ID, Options: []string{"gamma"}, Source: domain.AskSourceCLI}, "igor", false, now); err == nil {
		t.Fatal("an answer that is not an option was accepted")
	}
	if _, err := store.AnswerAsk(ctx, domain.AskAnswer{AskID: wait.ID, Options: []string{"alpha", "beta"}, Source: domain.AskSourceCLI}, "igor", false, now); err == nil {
		t.Fatal("two options were accepted for an ask without --multi")
	}
	answered, err := store.AnswerAsk(ctx, domain.AskAnswer{AskID: wait.ID, Options: []string{"beta"}, FreeText: " because ", Source: domain.AskSourceCLI}, "igor", false, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if answered.AskAnswer == nil || answered.AskAnswer.Schema != domain.AskAnswerSchema || answered.AskAnswer.Options[0] != "beta" ||
		answered.AskAnswer.FreeText != "because" || answered.AskAnswer.AnsweredBy != "igor" || answered.AskAnswer.Question != "M6b field test: pick one" ||
		answered.Result == nil || answered.Result.Outcome != domain.TaskWaitMet {
		t.Fatalf("answered ask = %+v", answered)
	}
	// The same answer again is a safe retry; a different one is refused.
	if _, err := store.AnswerAsk(ctx, domain.AskAnswer{AskID: wait.ID, Options: []string{"beta"}, FreeText: "because", Source: domain.AskSourceCLI}, "igor", false, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("replaying the same answer: %v", err)
	}
	if _, err := store.AnswerAsk(ctx, domain.AskAnswer{AskID: wait.ID, Options: []string{"alpha"}, Source: domain.AskSourceCLI}, "igor", false, now.Add(2*time.Minute)); !errors.Is(err, domain.ErrAskAlreadyAnswered) {
		t.Fatalf("a second, different answer: %v", err)
	}
	wakes, err := store.WakeTaskWaits(ctx, now.Add(3*time.Minute))
	if err != nil || len(wakes) != 1 {
		t.Fatalf("wakes=%v err=%v", wakes, err)
	}
	prompt := wakes[0].Prompt()
	for _, want := range []string{"Your question was answered", "Answer: beta; free text: because", `"schema": "ask-answer/v1"`, "ask-answer.json"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("the wake does not say %q:\n%s", want, prompt)
		}
	}
}

// The answered wake carries the workspace the worker reported, so the steward
// on that worker can write ask-answer.json into it.
func TestAnAnsweredAskWakeCarriesTheReportedWorkspace(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	if err := store.SaveWorkerSnapshot(ctx, domain.WorkerSnapshot{
		WorkerID: "worker", WorkerEpoch: "worker-epoch-1", CoordinatorEpoch: 1, Sequence: 2,
		Connected: true, ObservedAt: now.Add(time.Second), ValidUntil: now.Add(30 * 24 * time.Hour),
		Inventory:   domain.WorkerInventory{ID: "worker", AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: now},
		Assignments: []domain.WorkerAssignmentObservation{{AssignmentID: "assign-1", WorkspacePath: "/runs/r/t/a"}},
	}); err != nil {
		t.Fatal(err)
	}
	wait, err := store.RegisterTaskWait(ctx, askRegistration(attempt, "ask-1", pickAsk(), time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AnswerAsk(ctx, domain.AskAnswer{AskID: wait.ID, Options: []string{"alpha"}, Source: domain.AskSourceCLI}, "igor", false, now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.WakeTaskWaits(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	pending, err := store.TaskWakesAwaitingDelivery(ctx, now.Add(time.Minute))
	if err != nil || len(pending) != 1 || pending[0].WorkspacePath != "/runs/r/t/a" {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
}

// An ask declared --requires approver refuses an answer given in T3 and one
// from an ordinary administrator, and accepts one verified as an approver's.
func TestAnApproverAskAcceptsOnlyTheSignedApprover(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	ask := pickAsk()
	ask.Requires = domain.AskRequiresApprover
	wait, err := store.RegisterTaskWait(ctx, askRegistration(attempt, "ask-1", ask, time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordAskRelay(ctx, wait.ID, domain.AskRelay{WorkerID: "worker", ThreadID: "relay-1", State: domain.AskRelayOpen}, now); err == nil {
		t.Fatal("a relay thread was recorded for an approver ask")
	}
	if _, err := store.AnswerAsk(ctx, domain.AskAnswer{AskID: wait.ID, Options: []string{"beta"}, Source: domain.AskSourceT3, ThreadID: "relay-1"}, "worker-steward", false, now); !errors.Is(err, domain.ErrAskApproverRequired) {
		t.Fatalf("a T3 answer to an approver ask: %v", err)
	}
	if _, err := store.AnswerAsk(ctx, domain.AskAnswer{AskID: wait.ID, Options: []string{"beta"}, Source: domain.AskSourceCLI}, "admin", false, now); !errors.Is(err, domain.ErrAskApproverRequired) {
		t.Fatalf("an administrator's answer to an approver ask: %v", err)
	}
	answered, err := store.AnswerAsk(ctx, domain.AskAnswer{AskID: wait.ID, Options: []string{"beta"}, Source: domain.AskSourceCLI}, "approver", true, now)
	if err != nil || answered.AskAnswer == nil {
		t.Fatalf("the approver's answer: %+v %v", answered, err)
	}
}

// A T3 answer is accepted only from the ask's own recorded relay thread.
func TestAT3AnswerMustComeFromTheRelayThread(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	wait, err := store.RegisterTaskWait(ctx, askRegistration(attempt, "ask-1", pickAsk(), time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	answer := domain.AskAnswer{AskID: wait.ID, Options: []string{"beta"}, Source: domain.AskSourceT3, ThreadID: "relay-1"}
	if _, err := store.AnswerAsk(ctx, answer, "steward", false, now); err == nil {
		t.Fatal("a T3 answer was accepted with no relay thread recorded")
	}
	if _, err := store.RecordAskRelay(ctx, wait.ID, domain.AskRelay{WorkerID: "worker", ThreadID: "relay-1", State: domain.AskRelayOpen}, now); err != nil {
		t.Fatal(err)
	}
	foreign := answer
	foreign.ThreadID = "thread-1"
	if _, err := store.AnswerAsk(ctx, foreign, "steward", false, now); err == nil {
		t.Fatal("a T3 answer from another thread was accepted")
	}
	answered, err := store.AnswerAsk(ctx, answer, "steward", false, now)
	if err != nil || answered.AskAnswer.ThreadID != "relay-1" || answered.AskAnswer.Source != domain.AskSourceT3 {
		t.Fatalf("answered=%+v err=%v", answered.AskAnswer, err)
	}
}

// At the deadline an ask with a default resumes with that default as its
// answer and records no contradiction; one with --on-deadline fail resumes
// told there was no answer, and the expiry is recorded.
func TestAskDeadlineAppliesTheDefaultOrFails(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	wait, err := store.RegisterTaskWait(ctx, askRegistration(attempt, "ask-1", pickAsk(), time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	expired, err := store.ExpireTaskWaits(ctx, wait.Deadline.Add(time.Second))
	if err != nil || len(expired) != 1 {
		t.Fatalf("expired=%v err=%v", expired, err)
	}
	if got := expired[0]; got.AskAnswer == nil || got.AskAnswer.Source != domain.AskSourceDeadlineDefault ||
		got.AskAnswer.Options[0] != "alpha" || got.Result.Outcome != domain.TaskWaitMet {
		t.Fatalf("defaulted ask = %+v", got)
	}
	if events, _ := store.ListTaskWaitReconciliations(ctx); len(events) != 0 {
		t.Fatalf("a default was recorded as a contradiction: %+v", events)
	}
	wakes, err := store.WakeTaskWaits(ctx, wait.Deadline.Add(time.Minute))
	if err != nil || len(wakes) != 1 || !strings.Contains(wakes[0].Prompt(), "the default you declared applies") {
		t.Fatalf("wakes=%v err=%v", wakes, err)
	}

	store, attempt, now = taskWaitFixture(t)
	failing := pickAsk()
	failing.OnDeadline, failing.Default = domain.AskDeadlineFail, nil
	wait, err = store.RegisterTaskWait(ctx, askRegistration(attempt, "ask-2", failing, time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	expired, err = store.ExpireTaskWaits(ctx, wait.Deadline.Add(time.Second))
	if err != nil || len(expired) != 1 || expired[0].AskAnswer != nil || expired[0].Result.Outcome != domain.TaskWaitTimedOut ||
		!strings.Contains(expired[0].Result.Reason, "no answer") {
		t.Fatalf("expired=%+v err=%v", expired, err)
	}
	if events, _ := store.ListTaskWaitReconciliations(ctx); len(events) != 1 || events[0].Kind != domain.TaskWaitReconciliationExpired {
		t.Fatalf("an unanswered ask was not recorded: %+v", events)
	}
	if _, err := store.AnswerAsk(ctx, domain.AskAnswer{AskID: wait.ID, Options: []string{"beta"}, Source: domain.AskSourceCLI}, "igor", false, now.Add(2*time.Hour)); !errors.Is(err, domain.ErrAskSettled) {
		t.Fatalf("a late answer: %v", err)
	}
	wakes, err = store.WakeTaskWaits(ctx, wait.Deadline.Add(time.Minute))
	if err != nil || len(wakes) != 1 || !strings.Contains(wakes[0].Prompt(), `End the task now as failed, with the reason "no answer"`) {
		t.Fatalf("wakes=%v err=%v", wakes, err)
	}
}

// Cancelling an ask releases the task, telling it the question was withdrawn.
func TestAnAskCanBeCancelled(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	wait, err := store.RegisterTaskWait(ctx, askRegistration(attempt, "ask-1", pickAsk(), time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := store.CancelTaskWait(ctx, wait.ID, now.Add(time.Minute))
	if err != nil || cancelled.Result.Outcome != domain.TaskWaitCancelled {
		t.Fatalf("cancelled=%+v err=%v", cancelled, err)
	}
	wakes, err := store.WakeTaskWaits(ctx, now.Add(2*time.Minute))
	if err != nil || len(wakes) != 1 || !strings.Contains(wakes[0].Prompt(), "withdrawn without an answer") {
		t.Fatalf("wakes=%v err=%v", wakes, err)
	}
}

// The owner hears about an ask once when it is registered and once more when
// half its deadline has passed unanswered, and never twice for either.
func TestAskNotifiesTheOwnerOnceAndAgainAtHalfTheDeadline(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	clock := now
	sink := &recordingSink{selection: ownernotify.Selection{Events: []ownernotify.Event{ownernotify.EventNeedsInput}}}
	notifier := &ownernotify.Notifier{Store: store, Sinks: []ownernotify.Sink{sink},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return clock }}
	notifier.Tick(ctx) // baseline
	wait, err := store.RegisterTaskWait(ctx, askRegistration(attempt, "ask-1", pickAsk(), 2*time.Hour), now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	clock = now.Add(time.Minute)
	notifier.Tick(ctx)
	notifier.Tick(ctx)
	if got := sink.delivered(); len(got) != 1 || got[0].WaitID != wait.ID || got[0].Prompt != "M6b field test: pick one" ||
		len(got[0].AskOptions) != 2 || got[0].Reminder || got[0].Commands()["answer"] == "" {
		t.Fatalf("first notice = %+v", got)
	}
	clock = now.Add(59 * time.Minute)
	notifier.Tick(ctx)
	if got := sink.delivered(); len(got) != 1 {
		t.Fatalf("a reminder before half the deadline: %+v", got)
	}
	clock = now.Add(61 * time.Minute)
	notifier.Tick(ctx)
	notifier.Tick(ctx)
	if got := sink.delivered(); len(got) != 2 || !got[1].Reminder || got[1].WaitID != wait.ID {
		t.Fatalf("reminder = %+v", got)
	}
}
