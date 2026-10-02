package main

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// relayTestControl is a T3 with the task's thread, the relay threads the
// steward opens, and a scripted question card per relay thread.
type relayTestControl struct {
	waitTestControl
	started    []domain.AskRelayStart
	creates    int
	turns      int
	createErr  error
	turnErr    error
	archiveErr error
	events     map[string][]domain.UserInputEvent
	archived   []string
}

func (c *relayTestControl) CreateAskRelayThread(_ context.Context, relay domain.AskRelayStart) error {
	c.creates++
	if c.createErr != nil {
		return c.createErr
	}
	if c.threads[relay.ThreadID] == nil {
		c.threads[relay.ThreadID] = &domain.Thread{ID: relay.ThreadID, ProjectID: relay.ProjectID}
	}
	return nil
}

func (c *relayTestControl) StartAskRelayTurn(_ context.Context, relay domain.AskRelayStart) error {
	c.turns++
	if c.turnErr != nil {
		return c.turnErr
	}
	c.started = append(c.started, relay)
	thread := c.threads[relay.ThreadID]
	thread.TurnID, thread.Running = "turn-1", true
	if relay.ProjectID == "" {
		relay.ProjectID = thread.ProjectID
		c.started[len(c.started)-1] = relay
	}
	return nil
}

func (c *relayTestControl) UserInputEvents(_ context.Context, threadID string) ([]domain.UserInputEvent, error) {
	return c.events[threadID], nil
}

func (c *relayTestControl) ArchiveThread(_ context.Context, threadID string) error {
	if c.archiveErr != nil {
		return c.archiveErr
	}
	c.archived = append(c.archived, threadID)
	archived := time.Now()
	c.threads[threadID].ArchivedAt = &archived
	return nil
}

func relayFixture(t *testing.T, extra ...string) (*relayTestControl, *wait.Runner, func() []domain.TaskWait) {
	t.Helper()
	control, runner, list, _ := relayFixtureWithStore(t, extra...)
	return control, runner, list
}

func relayFixtureWithStore(t *testing.T, extra ...string) (*relayTestControl, *wait.Runner, func() []domain.TaskWait, *sqlite.Store) {
	t.Helper()
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	identity, err := resolveTaskIdentity(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"M6b field test: pick one", "--option", "alpha", "--option", "beta"}
	if len(extra) == 0 {
		args = append(args, "--deadline", "2h", "--default", "alpha")
	}
	spec, err := parseAskArgs(append(args, extra...))
	if err != nil {
		t.Fatal(err)
	}
	if err := runAsk(ctx, cfg, spec, identity, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	control := &relayTestControl{
		waitTestControl: waitTestControl{threads: map[string]*domain.Thread{
			"thread-1": {ID: "thread-1", ProjectID: "project-1", ProviderInstanceID: "codex"},
		}},
		events: map[string][]domain.UserInputEvent{},
	}
	runner := wait.New(store, control, nil)
	runner.DisableQuotaChecks = true
	runner.AskRelay = &wait.AskRelayRoute{Instance: "claudeAgent", Model: "claude-haiku-4-5"}
	now := time.Date(2026, 9, 14, 12, 1, 0, 0, time.UTC)
	runner.SetClock(func() time.Time { return now })
	list := func() []domain.TaskWait {
		waits, err := store.ListTaskWaits(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return waits
	}
	return control, runner, list, store
}

// A new ask gets one relay thread, on a Claude route, in the task thread's
// project, asking the exact question; an answer on its question card is
// recorded as a T3 answer, resumes the task, and the relay is archived.
func TestAskRelayOpensAThreadAndRecordsTheAnswerGivenOnIt(t *testing.T) {
	ctx := context.Background()
	control, runner, list := relayFixture(t)
	runner.Tick(ctx, nil, nil)
	runner.Tick(ctx, nil, nil)
	if len(control.started) != 1 {
		t.Fatalf("relay threads opened: %d, want 1", len(control.started))
	}
	start := control.started[0]
	ask := list()[0]
	if start.ThreadID != domain.AskRelayThreadID(ask.ID) || start.ProjectID != "project-1" || start.Instance != "claudeAgent" ||
		!strings.Contains(start.Title, "run-1/task-1") || !strings.Contains(start.Prompt, `question: "M6b field test: pick one"`) ||
		!strings.Contains(start.Prompt, "AskUserQuestion") {
		t.Fatalf("relay start = %+v", start)
	}
	if ask.Ask.Relay == nil || ask.Ask.Relay.State != domain.AskRelayOpen || ask.Ask.Relay.ThreadID != start.ThreadID {
		t.Fatalf("relay record = %+v", ask.Ask.Relay)
	}

	question := domain.UserInputQuestion{ID: "M6b field test: pick one", Header: domain.AskRelayHeader, Question: "M6b field test: pick one",
		Options: []domain.UserInputOption{{Label: "alpha"}, {Label: "beta"}}}
	control.events[start.ThreadID] = []domain.UserInputEvent{
		{Kind: domain.UserInputRequested, RequestID: "req-1", Questions: []domain.UserInputQuestion{question}},
		{Kind: domain.UserInputResolved, RequestID: "req-1", Answers: map[string]any{"M6b field test: pick one": "beta"}},
	}
	runner.Tick(ctx, nil, nil)
	ask = list()[0]
	if ask.AskAnswer == nil || ask.AskAnswer.Source != domain.AskSourceT3 || ask.AskAnswer.ThreadID != start.ThreadID ||
		len(ask.AskAnswer.Options) != 1 || ask.AskAnswer.Options[0] != "beta" {
		t.Fatalf("answer = %+v", ask.AskAnswer)
	}
	runner.Tick(ctx, nil, nil)
	if len(control.sends) != 1 {
		t.Fatalf("the task was woken %d times, want once", len(control.sends))
	}
	ask = list()[0]
	if len(control.archived) != 1 || ask.Ask.Relay.State != domain.AskRelayArchived {
		t.Fatalf("relay after the answer: archived=%v record=%+v", control.archived, ask.Ask.Relay)
	}
	runner.Tick(ctx, nil, nil)
	if len(control.started) != 1 || len(control.archived) != 1 {
		t.Fatalf("a settled ask's relay was touched again: started=%d archived=%v", len(control.started), control.archived)
	}
}

// A card that asks something else, or is dismissed, is not an answer: the
// relay fails and is archived, and the ask stays open for the CLI.
func TestAskRelayRefusesAnAnswerToADifferentQuestion(t *testing.T) {
	ctx := context.Background()
	for name, events := range map[string]func(string) []domain.UserInputEvent{
		"rephrased": func(string) []domain.UserInputEvent {
			q := domain.UserInputQuestion{ID: "Pick alpha or beta?", Question: "Pick alpha or beta?",
				Options: []domain.UserInputOption{{Label: "alpha"}, {Label: "beta"}}}
			return []domain.UserInputEvent{
				{Kind: domain.UserInputRequested, RequestID: "r", Questions: []domain.UserInputQuestion{q}},
				{Kind: domain.UserInputResolved, RequestID: "r", Answers: map[string]any{q.ID: "alpha"}},
			}
		},
		"dismissed": func(string) []domain.UserInputEvent {
			q := domain.UserInputQuestion{ID: "M6b field test: pick one", Question: "M6b field test: pick one",
				Options: []domain.UserInputOption{{Label: "alpha"}, {Label: "beta"}}}
			return []domain.UserInputEvent{
				{Kind: domain.UserInputRequested, RequestID: "r", Questions: []domain.UserInputQuestion{q}},
				{Kind: domain.UserInputResolved, RequestID: "r", Answers: map[string]any{}},
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			control, runner, list := relayFixture(t)
			runner.Tick(ctx, nil, nil)
			thread := control.started[0].ThreadID
			control.events[thread] = events(thread)
			runner.Tick(ctx, nil, nil)
			ask := list()[0]
			if ask.AskAnswer != nil || ask.Settled() || ask.Ask.Relay.State != domain.AskRelayFailed || ask.Ask.Relay.Reason == "" {
				t.Fatalf("ask = %+v relay = %+v", ask.AskAnswer, ask.Ask.Relay)
			}
			if len(control.archived) != 1 {
				t.Fatalf("failed relay archived: %v", control.archived)
			}
			runner.Tick(ctx, nil, nil)
			if len(control.started) != 1 {
				t.Fatal("a failed relay was reopened")
			}
		})
	}
}

// An approver ask opens no relay: its answer must come through the signed
// approver path, which a T3 card is not.
func TestAskRelayOpensNothingForAnApproverAsk(t *testing.T) {
	control, runner, list := relayFixture(t, "--requires", "approver")
	runner.Tick(context.Background(), nil, nil)
	if len(control.started) != 0 || list()[0].Ask.Relay != nil {
		t.Fatalf("an approver ask got a relay: %+v", control.started)
	}
}

// Over the transport a relay record must name its worker, which the
// coordinator checks against the asking attempt's assignment.
func TestAskRelayRecordOverTheTransportNamesItsWorker(t *testing.T) {
	ctx := context.Background()
	cfg, _ := taskWaitCLIFixture(t)
	identity, err := resolveTaskIdentity(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := parseAskArgs([]string{"q?", "--option", "a", "--option", "b"})
	if err != nil {
		t.Fatal(err)
	}
	if err := runAsk(ctx, cfg, spec, identity, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	remote := remoteTaskWaitStore{cfg: cfg}
	work, err := remote.AskRelayWork(ctx, "worker")
	if err != nil || len(work) != 1 {
		t.Fatalf("work=%v err=%v", work, err)
	}
	if other, err := remote.AskRelayWork(ctx, "other-worker"); err != nil || len(other) != 0 {
		t.Fatalf("another worker was given this ask: %v %v", other, err)
	}
	transport, err := newCoordinatorTransport(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transport.client.NodeWait(ctx, backlogadmin.NodeWaitOperation{Action: backlogadmin.AskRelayRecordAction, ID: work[0].ID,
		Relay: &domain.AskRelay{ThreadID: "relay", State: domain.AskRelayOpening}}); err == nil {
		t.Fatal("a relay record naming no worker was accepted")
	}
	if _, err := remote.RecordAskRelay(ctx, work[0].ID, domain.AskRelay{WorkerID: "other-worker", ThreadID: "relay", State: domain.AskRelayOpening}, time.Time{}); err == nil {
		t.Fatal("a worker that does not run the task recorded its relay")
	}
	if recorded, err := remote.RecordAskRelay(ctx, work[0].ID, domain.AskRelay{WorkerID: "worker", ThreadID: "relay", State: domain.AskRelayOpening}, time.Time{}); err != nil ||
		recorded.Ask.Relay.State != domain.AskRelayOpening {
		t.Fatalf("recorded=%+v err=%v", recorded.Ask, err)
	}
}
