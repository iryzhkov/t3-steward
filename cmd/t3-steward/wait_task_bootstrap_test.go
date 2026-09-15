package main

import (
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/wait"
	"os"
	"path/filepath"
	"testing"
)

func TestTaskWaitOwnershipFromPrivateWorkerBootstrap(t *testing.T) {
	home := t.TempDir()
	old := taskWaitWorkerHome
	taskWaitWorkerHome = func() (string, error) { return home, nil }
	t.Cleanup(func() { taskWaitWorkerHome = old })
	path := filepath.Join(home, ".config/t3-steward/worker-bootstrap.json")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"schema_version":1,"worker_id":"omarchy-pc","coordinator_id":"normandy-coordinator","transport":"ssh","capabilities":["git","huyang"],"provider_routes":["codex"],"credential_ref":"secretref:f02-protocol/omarchy-pc"}`)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.BacklogV2.CoordinatorClient = config.V2CoordinatorClient{CoordinatorID: "normandy-coordinator", Address: "test.invalid", Connection: "ssh", Credential: "secretref:f03-admin/test"}
	runner := wait.New(nil, nil, nil)
	if err := configureTaskWaitTransport(runner, cfg); err != nil {
		t.Fatal(err)
	}
	if runner.TaskWorkerID != "omarchy-pc" || !runner.AssignedTaskWakesOnly || runner.TaskStore == nil {
		t.Fatalf("bootstrap-only daemon ownership not configured: %+v", runner)
	}
	cfg.BacklogV2.LocalWorker.ID = "other"
	if err := configureTaskWaitTransport(runner, cfg); err == nil || runner.TaskWorkerID != "" {
		t.Fatal("contradictory worker identity accepted")
	}
	cfg.BacklogV2.LocalWorker.ID = ""
	cfg.BacklogV2.CoordinatorClient.CoordinatorID = "other"
	if err := configureTaskWaitTransport(runner, cfg); err == nil || runner.TaskWorkerID != "" {
		t.Fatal("contradictory coordinator identity accepted")
	}
	cfg.BacklogV2.CoordinatorClient.CoordinatorID = "normandy-coordinator"
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if err := configureTaskWaitTransport(runner, cfg); err == nil || runner.TaskWorkerID != "" {
		t.Fatal("nonprivate bootstrap accepted")
	}
}
