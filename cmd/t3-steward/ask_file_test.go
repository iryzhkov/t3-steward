package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

func reportWorkspace(t *testing.T, store *sqlite.Store, workspace string, sequence int64) {
	t.Helper()
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	if err := store.SaveWorkerSnapshot(context.Background(), domain.WorkerSnapshot{
		WorkerID: "worker", WorkerEpoch: "worker-epoch-1", CoordinatorEpoch: 1, Sequence: sequence,
		Connected: true, ObservedAt: now.Add(time.Duration(sequence) * time.Second), ValidUntil: now.Add(30 * 24 * time.Hour),
		Inventory:   domain.WorkerInventory{ID: "worker", AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: now},
		Assignments: []domain.WorkerAssignmentObservation{{AssignmentID: "assign-1", WorkspacePath: workspace}},
	}); err != nil {
		t.Fatal(err)
	}
}

// Review finding 5: each wake leaves ask-answer.json saying exactly what the
// wake says. A second ask that ends unanswered removes the first ask's file,
// so the task never reads a stale answer.
func TestASecondUnansweredAskRemovesTheFirstAnswerFile(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	workspace := t.TempDir()
	reportWorkspace(t, store, workspace, 2)
	identity, err := resolveTaskIdentity(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	control := &waitTestControl{threads: map[string]*domain.Thread{"thread-1": {ID: "thread-1", ProviderInstanceID: "claudeAgent"}}}
	clock := time.Date(2026, 9, 14, 12, 1, 0, 0, time.UTC)
	runner := wait.New(store, control, nil)
	runner.DisableQuotaChecks = true
	runner.SetClock(func() time.Time { return clock })

	first, err := parseAskArgs([]string{"First?", "--option", "alpha", "--option", "beta"})
	if err != nil {
		t.Fatal(err)
	}
	if err := runAsk(ctx, cfg, first, identity, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	waits, _ := store.ListTaskWaits(ctx)
	if err := runAskAnswer(ctx, cfg, askAnswerSpec{ID: waits[0].ID, Options: []string{"beta"}}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	runner.Tick(ctx, nil, nil)
	file := filepath.Join(workspace, domain.AskAnswerFile)
	if _, err := os.Stat(file); err != nil {
		t.Fatalf("the first answer was not written: %v", err)
	}

	second, err := parseAskArgs([]string{"Second?", "--option", "alpha", "--option", "beta", "--deadline", "1m", "--on-deadline", "fail"})
	if err != nil {
		t.Fatal(err)
	}
	if err := runAsk(ctx, cfg, second, identity, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(time.Hour)
	runner.Tick(ctx, nil, nil)
	runner.Tick(ctx, nil, nil)
	if len(control.sends) != 2 {
		t.Fatalf("wakes sent: %d, want 2", len(control.sends))
	}
	if _, err := os.Lstat(file); !os.IsNotExist(err) {
		t.Fatalf("the first ask's answer file survived the second, unanswered ask: %v", err)
	}
}

// Review finding 5: when the file cannot be prepared, the wake says so and
// tells the task to use only the document in the message.
func TestAnUnwritableAnswerFileIsNamedInTheWake(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	workspace := t.TempDir()
	if err := os.Mkdir(filepath.Join(workspace, domain.AskAnswerFile), 0o700); err != nil {
		t.Fatal(err)
	}
	reportWorkspace(t, store, workspace, 2)
	identity, err := resolveTaskIdentity(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := parseAskArgs([]string{"Pick?", "--option", "alpha", "--option", "beta"})
	if err != nil {
		t.Fatal(err)
	}
	if err := runAsk(ctx, cfg, spec, identity, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	waits, _ := store.ListTaskWaits(ctx)
	if err := runAskAnswer(ctx, cfg, askAnswerSpec{ID: waits[0].ID, Options: []string{"alpha"}}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	messages := map[string]string{}
	control := &messageRecordingControl{waitTestControl: &waitTestControl{threads: map[string]*domain.Thread{
		"thread-1": {ID: "thread-1", ProviderInstanceID: "claudeAgent"},
	}}, messages: messages}
	runner := wait.New(store, control, nil)
	runner.DisableQuotaChecks = true
	runner.SetClock(func() time.Time { return time.Date(2026, 9, 14, 12, 1, 0, 0, time.UTC) })
	runner.Tick(ctx, nil, nil)
	if len(messages) != 1 {
		t.Fatalf("wakes: %d", len(messages))
	}
	for _, message := range messages {
		if !strings.Contains(message, "could not be prepared") || !strings.Contains(message, "use only the document in this message") {
			t.Fatalf("the wake does not warn about the file:\n%s", message)
		}
	}
}
