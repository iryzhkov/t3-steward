package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Review finding 1: an approver ask is never default-settled, even when its
// stored record carries a default from before registration refused one.
func TestAnApproverAskWithAStoredDefaultTimesOutUnanswered(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	wait, err := store.RegisterTaskWait(ctx, askRegistration(attempt, "ask-1", pickAsk(), time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	// Rewrite the record as an approver ask with a default, the shape an
	// earlier build could have stored.
	wait.Ask.Requires = domain.AskRequiresApprover
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := saveTaskWaitTx(ctx, tx, wait); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	expired, err := store.ExpireTaskWaits(ctx, wait.Deadline.Add(time.Second))
	if err != nil || len(expired) != 1 {
		t.Fatalf("expired=%v err=%v", expired, err)
	}
	if expired[0].AskAnswer != nil || expired[0].Result.Outcome != domain.TaskWaitTimedOut {
		t.Fatalf("an approver ask was default-settled: %+v %+v", expired[0].AskAnswer, expired[0].Result)
	}
}

// Review finding 2: an answer is accepted strictly before the deadline; at or
// after it, the expiry policy is applied atomically and the answer refused,
// even when the expiry pass has not run yet.
func TestAnAnswerAroundTheDeadline(t *testing.T) {
	ctx := context.Background()
	answer := func(id string) domain.AskAnswer {
		return domain.AskAnswer{AskID: id, Options: []string{"beta"}, Source: domain.AskSourceCLI}
	}

	store, attempt, now := taskWaitFixture(t)
	wait, err := store.RegisterTaskWait(ctx, askRegistration(attempt, "ask-1", pickAsk(), time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.AnswerAsk(ctx, answer(wait.ID), "igor", false, wait.Deadline.Add(-time.Nanosecond))
	if err != nil || before.AskAnswer == nil || before.AskAnswer.Source != domain.AskSourceCLI {
		t.Fatalf("an answer just before the deadline: %+v %v", before.AskAnswer, err)
	}

	store, attempt, now = taskWaitFixture(t)
	wait, err = store.RegisterTaskWait(ctx, askRegistration(attempt, "ask-1", pickAsk(), time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.AnswerAsk(ctx, answer(wait.ID), "igor", false, wait.Deadline)
	if !errors.Is(err, domain.ErrAskSettled) {
		t.Fatalf("an answer at the deadline: %v", err)
	}
	waits, err := store.ListTaskWaits(ctx)
	if err != nil || len(waits) != 1 || waits[0].AskAnswer == nil || waits[0].AskAnswer.Source != domain.AskSourceDeadlineDefault ||
		waits[0].AskAnswer.Options[0] != "alpha" {
		t.Fatalf("the late answer did not leave the default in force: %+v %v", waits, err)
	}
	// The expiry pass that runs afterwards finds nothing left to do.
	if expired, err := store.ExpireTaskWaits(ctx, wait.Deadline.Add(time.Minute)); err != nil || len(expired) != 0 {
		t.Fatalf("expired twice: %v %v", expired, err)
	}
}

// Review finding 4: a replay that differs only by a nil option list or by
// surrounding spaces is the same answer, and authority is checked before a
// replay succeeds.
func TestAskAnswerReplayIsNormalisedAndAuthorityComesFirst(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	wait, err := store.RegisterTaskWait(ctx, askRegistration(attempt, "ask-1", pickAsk(), time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AnswerAsk(ctx, domain.AskAnswer{AskID: wait.ID, FreeText: "  neither  ", Source: domain.AskSourceCLI}, "igor", false, now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AnswerAsk(ctx, domain.AskAnswer{AskID: wait.ID, Options: []string{}, FreeText: "neither", Source: domain.AskSourceCLI}, "igor", false, now); err != nil {
		t.Fatalf("a normalised replay was refused: %v", err)
	}

	store, attempt, now = taskWaitFixture(t)
	approver := pickAsk()
	approver.Requires, approver.OnDeadline, approver.Default = domain.AskRequiresApprover, domain.AskDeadlineFail, nil
	wait, err = store.RegisterTaskWait(ctx, askRegistration(attempt, "ask-2", approver, time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	given := domain.AskAnswer{AskID: wait.ID, Options: []string{"beta"}, Source: domain.AskSourceCLI}
	if _, err := store.AnswerAsk(ctx, given, "approver", true, now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AnswerAsk(ctx, given, "admin", false, now); !errors.Is(err, domain.ErrAskApproverRequired) {
		t.Fatalf("a non-approver replaying the approver's answer: %v", err)
	}
}

// Small finding: a relay record never moves an open relay back to opening.
func TestAskRelayStateDoesNotRegress(t *testing.T) {
	ctx := context.Background()
	store, attempt, now := taskWaitFixture(t)
	wait, err := store.RegisterTaskWait(ctx, askRegistration(attempt, "ask-1", pickAsk(), time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []domain.AskRelayState{domain.AskRelayOpening, domain.AskRelayOpen, domain.AskRelayOpening} {
		if _, err := store.RecordAskRelay(ctx, wait.ID, domain.AskRelay{WorkerID: "worker", ThreadID: "relay", State: state}, now); err != nil {
			t.Fatal(err)
		}
	}
	waits, _ := store.ListTaskWaits(ctx)
	if waits[0].Ask.Relay.State != domain.AskRelayOpen {
		t.Fatalf("relay regressed to %q", waits[0].Ask.Relay.State)
	}
}
