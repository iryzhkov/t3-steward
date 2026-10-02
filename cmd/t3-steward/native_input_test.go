package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// A task that asks with AskUserQuestion in its own thread is answered there;
// the steward of its worker records the question and the answer in the run's
// events, once each.
func TestANativeQuestionInATaskThreadIsRecordedInTheRunsEvents(t *testing.T) {
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
	runner := wait.New(store, control, nil)
	runner.DisableQuotaChecks = true
	runner.TaskWorkerID = "worker"
	runner.TaskThreads = func(context.Context) (map[string]string, error) {
		return map[string]string{"thread-1": "attempt-1"}, nil
	}
	runner.SetClock(func() time.Time { return asked.Add(time.Minute) })
	runner.Tick(ctx, nil, nil)
	// The owner answers; T3 no longer reports pending input, and the steward
	// still reads the thread once more because it saw the open request.
	control.threads["thread-1"].HasPendingUserInput = false
	control.events["thread-1"] = append(control.events["thread-1"], domain.UserInputEvent{
		Kind: domain.UserInputResolved, ActivityID: "act-2", RequestID: "req-1",
		Answers: map[string]any{"Which database?": "postgres"}, At: asked.Add(2 * time.Minute),
	})
	runner.Tick(ctx, nil, nil)
	runner.Tick(ctx, nil, nil)

	events, err := store.LoadAuditEvents(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	var answer string
	for _, event := range events {
		if strings.HasPrefix(event.Kind, "native-") {
			kinds = append(kinds, event.Kind)
			if event.Kind == "native-answer" {
				answer = event.Reason
			}
			if event.AttemptID != "attempt-1" || event.TaskID != "task-1" {
				t.Fatalf("event bound to the wrong node: %+v", event)
			}
		}
	}
	if strings.Join(kinds, ",") != "native-question,native-answer" || answer != "Which database?: postgres" {
		t.Fatalf("native events = %v, answer %q", kinds, answer)
	}
	if _, err := store.RecordNativeUserInput(ctx, "other-worker", "attempt-1", "thread-1", control.events["thread-1"], asked); err == nil {
		t.Fatal("a worker that does not run the attempt recorded its questions")
	}
}
