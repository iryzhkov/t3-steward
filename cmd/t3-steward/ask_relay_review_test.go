package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

func relayCard(question string, labels ...string) domain.UserInputQuestion {
	card := domain.UserInputQuestion{ID: question, Question: question}
	for _, label := range labels {
		card.Options = append(card.Options, domain.UserInputOption{Label: label})
	}
	return card
}

// Review finding 7: a card that is not the ask's question is withdrawn as soon
// as it is requested, before anyone can answer it.
func TestAWrongRelayCardIsWithdrawnBeforeItIsAnswered(t *testing.T) {
	ctx := context.Background()
	control, runner, list := relayFixture(t)
	runner.Tick(ctx, nil, nil)
	thread := control.started[0].ThreadID
	control.events[thread] = []domain.UserInputEvent{{Kind: domain.UserInputRequested, RequestID: "r",
		Questions: []domain.UserInputQuestion{relayCard("Pick alpha or beta?", "alpha", "beta")}}}
	runner.Tick(ctx, nil, nil)
	if len(control.archived) != 1 || list()[0].Ask.Relay.State != domain.AskRelayFailed {
		t.Fatalf("a pending wrong card stayed up: archived=%v relay=%+v", control.archived, list()[0].Ask.Relay)
	}
}

// Review finding 8: a relay is recorded failed only after its thread is
// archived; until then it stays in the relay's work and is retried.
func TestARelayIsRecordedFailedOnlyAfterItsThreadIsArchived(t *testing.T) {
	ctx := context.Background()
	control, runner, list := relayFixture(t)
	runner.Tick(ctx, nil, nil)
	thread := control.started[0].ThreadID
	control.events[thread] = []domain.UserInputEvent{
		{Kind: domain.UserInputRequested, RequestID: "r", Questions: []domain.UserInputQuestion{relayCard("M6b field test: pick one", "alpha", "beta")}},
		{Kind: domain.UserInputResolved, RequestID: "r", Answers: map[string]any{}},
	}
	control.archiveErr = errors.New("T3 unavailable")
	runner.Tick(ctx, nil, nil)
	if state := list()[0].Ask.Relay.State; state != domain.AskRelayOpen {
		t.Fatalf("recorded %q while its card could still be visible", state)
	}
	control.archiveErr = nil
	runner.Tick(ctx, nil, nil)
	if state := list()[0].Ask.Relay.State; state != domain.AskRelayFailed || len(control.archived) != 1 {
		t.Fatalf("after the archive: %q archived=%v", state, control.archived)
	}
}

// Review finding 9: a relay that cannot be opened is given up after a bounded
// number of attempts, with the reason recorded, and the ask stays open.
func TestARelayThatCannotBeOpenedIsGivenUp(t *testing.T) {
	ctx := context.Background()
	control, runner, list := relayFixture(t)
	control.createErr = errors.New("T3 refused the command")
	for range 15 {
		runner.Tick(ctx, nil, nil)
	}
	ask := list()[0]
	if control.creates != 10 || ask.Ask.Relay.State != domain.AskRelayFailed || !strings.Contains(ask.Ask.Relay.Reason, "could not be opened") || ask.Settled() {
		t.Fatalf("creates=%d relay=%+v settled=%v", control.creates, ask.Ask.Relay, ask.Settled())
	}
}

// Review finding 10: a thread created without its turn, also across a steward
// restart, gets its turn started and is never recorded open before that.
func TestARelayCreatedWithoutItsTurnGetsTheTurnStarted(t *testing.T) {
	ctx := context.Background()
	control, runner, list, store := relayFixtureWithStore(t)
	control.turnErr = errors.New("connection reset")
	runner.Tick(ctx, nil, nil)
	if control.creates != 1 || list()[0].Ask.Relay.State != domain.AskRelayOpening {
		t.Fatalf("after a failed turn start: creates=%d relay=%+v", control.creates, list()[0].Ask.Relay)
	}
	// A new steward process, as after a restart, with the turn now possible.
	control.turnErr = nil
	restarted := wait.New(store, control, nil)
	restarted.DisableQuotaChecks = true
	restarted.AskRelay = runner.AskRelay
	restarted.SetClock(func() time.Time { return time.Date(2026, 9, 14, 12, 2, 0, 0, time.UTC) })
	restarted.Tick(ctx, nil, nil)
	if control.creates != 1 || control.turns != 2 || list()[0].Ask.Relay.State != domain.AskRelayOpen {
		t.Fatalf("after the restart: creates=%d turns=%d relay=%+v", control.creates, control.turns, list()[0].Ask.Relay)
	}
}

// flakyAnswerStore fails the first answers it is given with a transport error.
type flakyAnswerStore struct {
	wait.AskRelayStore
	failures int
}

func (s *flakyAnswerStore) AnswerAsk(ctx context.Context, answer domain.AskAnswer, principal string, approver bool, now time.Time) (domain.TaskWait, error) {
	if s.failures > 0 {
		s.failures--
		return domain.TaskWait{}, errors.New("coordinator-exchange: no coordinator answered")
	}
	return s.AskRelayStore.AnswerAsk(ctx, answer, principal, approver, now)
}

// Review finding 11: a transient failure to record the answer is retried from
// the card on the next tick; it does not fail the relay.
func TestATransientAnswerFailureIsRetried(t *testing.T) {
	ctx := context.Background()
	control, runner, list, store := relayFixtureWithStore(t)
	runner.AskStore = &flakyAnswerStore{AskRelayStore: store, failures: 1}
	runner.Tick(ctx, nil, nil)
	thread := control.started[0].ThreadID
	control.events[thread] = []domain.UserInputEvent{
		{Kind: domain.UserInputRequested, RequestID: "r", Questions: []domain.UserInputQuestion{relayCard("M6b field test: pick one", "alpha", "beta")}},
		{Kind: domain.UserInputResolved, RequestID: "r", Answers: map[string]any{"M6b field test: pick one": "beta"}},
	}
	runner.Tick(ctx, nil, nil)
	if ask := list()[0]; ask.AskAnswer != nil || ask.Ask.Relay.State != domain.AskRelayOpen {
		t.Fatalf("after a transient failure: answer=%+v relay=%+v", ask.AskAnswer, ask.Ask.Relay)
	}
	runner.Tick(ctx, nil, nil)
	if ask := list()[0]; ask.AskAnswer == nil || ask.AskAnswer.Options[0] != "beta" {
		t.Fatalf("the retried answer was not recorded: %+v", ask.AskAnswer)
	}
}
