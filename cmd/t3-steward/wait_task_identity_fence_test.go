package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

type countedTaskRuntime struct{ calls int }

func (s *countedTaskRuntime) SettleTaskWait(context.Context, string, domain.TaskWaitResult, time.Time) (domain.TaskWait, error) {
	s.calls++
	return domain.TaskWait{}, nil
}
func (s *countedTaskRuntime) ExpireTaskWaits(context.Context, time.Time) ([]domain.TaskWait, error) {
	s.calls++
	return nil, nil
}
func (s *countedTaskRuntime) WakeTaskWaits(context.Context, time.Time) ([]domain.TaskWaitWakeContext, error) {
	s.calls++
	return nil, nil
}
func (s *countedTaskRuntime) TaskWakesAwaitingDelivery(context.Context, time.Time) ([]domain.TaskWaitWakeContext, error) {
	s.calls++
	return nil, nil
}
func (s *countedTaskRuntime) TransitionTaskWake(context.Context, string, string, string, time.Time) (bool, error) {
	s.calls++
	return false, nil
}

func TestTaskWaitInvalidIdentityFencesEveryRuntimeCall(t *testing.T) {
	ctx := context.Background()
	home := t.TempDir()
	previous := taskWaitWorkerHome
	taskWaitWorkerHome = func() (string, error) { return home, nil }
	t.Cleanup(func() { taskWaitWorkerHome = previous })
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settled := time.Now()
	if err := store.SaveWait(ctx, wait.Wait{ID: "w-task", ThreadID: "thread", TaskWaitID: "tw-task", Status: wait.StatusMet, CreatedAt: settled, SettledAt: &settled}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveWait(ctx, wait.Wait{ID: "w-interactive", ThreadID: "interactive", Status: wait.StatusWaiting, CreatedAt: settled.Add(-time.Minute), Every: time.Second, Timeout: time.Hour}); err != nil {
		t.Fatal(err)
	}
	runner := wait.New(store, &waitTestControl{threads: map[string]*domain.Thread{}}, nil)
	cfg := config.Default()
	cfg.BacklogV2.CoordinatorClient = config.V2CoordinatorClient{CoordinatorID: "test", Address: "unused.invalid", Connection: "ssh", Credential: "secretref:f03-admin/test"}
	if err := configureTaskWaitTransport(runner, cfg); err == nil {
		t.Fatal("missing worker identity accepted")
	}
	calls := &countedTaskRuntime{}
	runner.TaskStore = calls
	checks := 0
	runner.Exec = func(context.Context, wait.Wait) (string, int, error) { checks++; return "pending", 1, nil }
	runner.Tick(ctx, nil, nil)
	if calls.calls != 0 {
		t.Fatalf("invalid identity made %d coordinator task-runtime calls", calls.calls)
	}
	if checks != 1 {
		t.Fatalf("interactive checks stopped: got %d", checks)
	}
	path := filepath.Join(home, ".config/t3-steward/worker-bootstrap.json")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"schema_version":1,"worker_id":"worker","coordinator_id":"wrong-coordinator","transport":"ssh","capabilities":["git","huyang"],"provider_routes":["codex"],"credential_ref":"secretref:f02-protocol/worker"}`)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := configureTaskWaitTransport(runner, cfg); err == nil {
		t.Fatal("mismatched coordinator accepted")
	}
	runner.TaskStore = calls
	runner.Tick(ctx, nil, nil)
	if calls.calls != 0 {
		t.Fatalf("mismatched identity made %d coordinator calls", calls.calls)
	}
}
