package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

func writeTaskWaitBootstrap(t *testing.T, workerID, coordinatorID string) string {
	t.Helper()
	home := t.TempDir()
	old := taskWaitWorkerHome
	taskWaitWorkerHome = func() (string, error) { return home, nil }
	t.Cleanup(func() { taskWaitWorkerHome = old })
	if workerID == "" {
		return home
	}
	path := filepath.Join(home, ".config/t3-steward/worker-bootstrap.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	raw := `{"schema_version":1,"worker_id":"` + workerID + `","coordinator_id":"` + coordinatorID +
		`","transport":"ssh","capabilities":["git","huyang"],"provider_routes":["codex"],"credential_ref":"secretref:f02-protocol/` + workerID + `"}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	return home
}

// rc.100 field proof on normandy: the coordinator host runs a worker whose
// identity is only in its worker bootstrap, and its configuration names no
// coordinator client and no local worker. Its steward must still deliver the
// wakes of tasks running there and open their ask relays, from its own store.
func TestTheCoordinatorHostDeliversTaskWakesAndOpensRelays(t *testing.T) {
	ctx := context.Background()
	cfg, store := taskWaitCLIFixture(t)
	cfg.BacklogV2.Coordinator.ID = "test-coordinator"
	writeTaskWaitBootstrap(t, "worker", "test-coordinator")

	control := &relayTestControl{
		waitTestControl: waitTestControl{threads: map[string]*domain.Thread{
			"thread-1": {ID: "thread-1", ProjectID: "project-1", ProviderInstanceID: "codex"},
		}},
		events: map[string][]domain.UserInputEvent{},
	}
	runner := wait.New(store, control, nil)
	runner.DisableQuotaChecks = true
	runner.AskRelay = &wait.AskRelayRoute{Instance: "claudeAgent", Model: "claude-haiku-4-5"}
	runner.SetClock(func() time.Time { return time.Date(2026, 9, 14, 12, 1, 0, 0, time.UTC) })
	if err := configureTaskWaitTransport(runner, cfg); err != nil {
		t.Fatal(err)
	}
	if runner.TaskWorkerID != "worker" || runner.DisableTaskWaitRuntime || runner.TaskStore != nil {
		t.Fatalf("coordinator host: worker=%q disabled=%v remote=%v", runner.TaskWorkerID, runner.DisableTaskWaitRuntime, runner.TaskStore != nil)
	}

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
	runner.Tick(ctx, nil, nil)
	if len(control.started) != 1 {
		t.Fatalf("relay threads opened on the coordinator host: %d, want 1", len(control.started))
	}
	waits, _ := store.ListTaskWaits(ctx)
	if err := runAskAnswer(ctx, cfg, askAnswerSpec{ID: waits[0].ID, Options: []string{"beta"}}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	runner.Tick(ctx, nil, nil)
	if len(control.sends) != 1 {
		t.Fatalf("task wakes delivered on the coordinator host: %d, want 1", len(control.sends))
	}
}

// Without any worker identity the coordinator host still runs the
// coordinator's own wait duties, and says plainly that tasks on this host
// cannot be woken; check reports it.
func TestAMissingWorkerIdentityOnTheCoordinatorHostIsReported(t *testing.T) {
	cfg := config.Default()
	cfg.BacklogV2.Coordinator.ID = "test-coordinator"
	writeTaskWaitBootstrap(t, "", "")
	runner := wait.New(nil, nil, nil)
	err := configureTaskWaitTransport(runner, cfg)
	if !errors.Is(err, errNoTaskWorkerIdentity) || runner.DisableTaskWaitRuntime || runner.TaskWorkerID != "" {
		t.Fatalf("err=%v disabled=%v worker=%q", err, runner.DisableTaskWaitRuntime, runner.TaskWorkerID)
	}
	level, text := checkTaskWaitIdentity(cfg)
	if level != "FAIL" || !strings.Contains(text, "worker-bootstrap.json") {
		t.Fatalf("check = %s %q", level, text)
	}

	// A host that is neither a coordinator nor a worker has nothing to wake.
	plain := config.Default()
	if level, text := checkTaskWaitIdentity(plain); level != "ok" {
		t.Fatalf("plain host check = %s %q", level, text)
	}

	// A bootstrap for another coordinator is refused on the coordinator host.
	writeTaskWaitBootstrap(t, "normandy", "other-coordinator")
	if err := configureTaskWaitTransport(runner, cfg); err == nil || errors.Is(err, errNoTaskWorkerIdentity) || runner.TaskWorkerID != "" {
		t.Fatalf("a bootstrap for another coordinator: err=%v worker=%q", err, runner.TaskWorkerID)
	}
}
