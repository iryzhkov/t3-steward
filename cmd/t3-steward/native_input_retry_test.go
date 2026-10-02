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

// flakyNativeStore fails the next reports it is given with a transport error.
type flakyNativeStore struct {
	wait.NativeInputStore
	failures int
}

func (s *flakyNativeStore) RecordNativeUserInput(ctx context.Context, workerID, attemptID, threadID string, events []domain.UserInputEvent, now time.Time) (int, error) {
	if s.failures > 0 {
		s.failures--
		return 0, errors.New("coordinator-exchange: no coordinator answered")
	}
	return s.NativeInputStore.RecordNativeUserInput(ctx, workerID, attemptID, threadID, events, now)
}

// Review finding 14: an answer whose report failed is reported again on a
// later tick, even though the thread no longer shows pending input and the
// steward would otherwise stop reading it.
func TestANativeAnswerWhoseReportFailedIsReportedLater(t *testing.T) {
	ctx := context.Background()
	_, store := taskWaitCLIFixture(t)
	control := &relayTestControl{
		waitTestControl: waitTestControl{threads: map[string]*domain.Thread{
			"thread-1": {ID: "thread-1", HasPendingUserInput: true, Running: true},
		}},
		events: map[string][]domain.UserInputEvent{},
	}
	question := domain.UserInputQuestion{ID: "Which database?", Question: "Which database?",
		Options: []domain.UserInputOption{{Label: "sqlite"}, {Label: "postgres"}}}
	asked := time.Date(2026, 9, 14, 12, 5, 0, 0, time.UTC)
	control.events["thread-1"] = []domain.UserInputEvent{
		{Kind: domain.UserInputRequested, ActivityID: "act-1", RequestID: "req-1", Questions: []domain.UserInputQuestion{question}, At: asked},
	}
	flaky := &flakyNativeStore{NativeInputStore: store}
	runner := wait.New(store, control, nil)
	runner.DisableQuotaChecks = true
	runner.TaskWorkerID = "worker"
	runner.NativeStore = flaky
	runner.TaskThreads = func(context.Context) (map[string]string, error) {
		return map[string]string{"thread-1": "attempt-1"}, nil
	}
	runner.SetClock(func() time.Time { return asked.Add(time.Minute) })
	runner.Tick(ctx, nil, nil)

	control.threads["thread-1"].HasPendingUserInput = false
	control.events["thread-1"] = append(control.events["thread-1"], domain.UserInputEvent{
		Kind: domain.UserInputResolved, ActivityID: "act-2", RequestID: "req-1",
		Answers: map[string]any{"Which database?": "postgres"}, At: asked.Add(2 * time.Minute),
	})
	flaky.failures = 1
	runner.Tick(ctx, nil, nil)
	runner.Tick(ctx, nil, nil)

	events, err := store.LoadAuditEvents(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	answered := false
	for _, event := range events {
		if event.Kind == "native-answer" && strings.Contains(event.Reason, "postgres") {
			answered = true
		}
	}
	if !answered {
		t.Fatal("the answer whose report failed was never reported again")
	}
}
