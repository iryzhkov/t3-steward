package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/testtiming"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

// busyEnrollmentWorker runs a worker daemon's catalog host on a Unix socket,
// lets the coordinator publish its catalog once, and then leaves a running
// attempt in the worker's journal, as agent-a had when its sizes changed.
func busyEnrollmentWorker(t *testing.T) (*config.Config, *sqlite.Store, backlog.CoordinatorArtifactStore) {
	t.Helper()
	root, err := os.MkdirTemp("", "t3-busy-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	ctx, cancel := context.WithCancel(context.Background())
	cfg := config.Default()
	setCoordinatorTestRoots(t, &cfg)
	cfg.BacklogV2.Coordinator.ID = "coordinator"
	cfg.BacklogV2.MessageLimits.MaxArtifactBytes = 1 << 20
	cfg.BacklogV2.SetupProfiles = map[string]config.V2SetupProfile{"test": {Commands: []string{"true"}, Timeout: config.Duration(time.Minute)}}
	project := cfg.BacklogV2.Projects["steward"]
	project.SetupProfile = "test"
	project.Repository = "https://example.invalid/steward.git"
	cfg.BacklogV2.Projects["steward"] = project
	worker := cfg.BacklogV2.Workers["normandy"]
	worker.Epoch = "worker-1"
	worker.Connection = "unix"
	worker.Address = filepath.Join(root, "worker.sock")
	worker.Capabilities = []string{"git", "huyang"}
	worker.Credential = "secretref:f02-protocol/normandy"
	worker.AcceptBacklog = true
	cfg.BacklogV2.Workers["normandy"] = worker
	host := &workerruntime.CatalogHost{Home: t.TempDir(), Bootstrap: workerruntime.WorkerBootstrap{SchemaVersion: 1, WorkerID: "normandy", CoordinatorID: "coordinator", Transport: "ssh", Capabilities: worker.Capabilities, ProviderRoutes: []string{"codex"}, CredentialRef: worker.Credential}, Options: workerruntime.WorkerServiceOptions{ProtocolCredentials: persistentTestCredentials{}, DryRun: true}}
	listener, err := net.Listen("unix", worker.Address)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- workerruntime.ServeWorkerListener(ctx, listener, 24<<20, testtiming.Bound(time.Minute), 4, host.HandleFrame)
	}()
	t.Cleanup(func() { cancel(); <-done })
	store, err := sqlitetest.OpenMigrated(filepath.Join(root, "coordinator.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	artifacts := backlog.CoordinatorArtifactStore{Root: filepath.Join(root, "artifacts"), Catalog: store}
	session, err := newCoordinatorWorkerSession(ctx, cfg.BacklogV2, store, "normandy", 1, "first", persistentTestCredentials{}, time.Now(), nil, artifacts)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := session.Client.Snapshot(ctx, workerproto.SnapshotRequest{ParkedReported: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	session.Close()

	projection, err := workerruntime.BuildCatalogProjection(cfg.BacklogV2, "normandy")
	if err != nil {
		t.Fatal(err)
	}
	settings, err := projection.Settings(host.Bootstrap, host.Home)
	if err != nil {
		t.Fatal(err)
	}
	_, _, journalRoot := workerruntime.WorkerRoots(settings, "normandy")
	path := filepath.Join(journalRoot, "journal.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var journal map[string]json.RawMessage
	if err := json.Unmarshal(raw, &journal); err != nil {
		t.Fatal(err)
	}
	record := workerruntime.AttemptRecord{Phase: workerruntime.PhaseRunning, UpdatedAt: time.Now().UTC()}
	record.Assignment.ID = "running"
	record.Package.Package.Identity.AssignmentID = "running"
	attempts, err := json.Marshal(map[string]workerruntime.AttemptRecord{"running": record})
	if err != nil {
		t.Fatal(err)
	}
	journal["attempts"] = attempts
	if raw, err = json.Marshal(journal); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return &cfg, store, artifacts
}

// The executor capacity of a busy worker changes without draining it: the
// catalog exchange that ended in a bare EOF on agent-a is accepted.
func TestBusyWorkerCatalogSessionAcceptsCapacityChange(t *testing.T) {
	cfg, store, artifacts := busyEnrollmentWorker(t)
	worker := cfg.BacklogV2.Workers["normandy"]
	worker.Executors.CPUUnits = 9
	worker.Executors.MemoryMB = 30000
	cfg.BacklogV2.Workers["normandy"] = worker
	session, err := newCoordinatorWorkerSession(context.Background(), cfg.BacklogV2, store, "normandy", 1, "capacity", persistentTestCredentials{}, time.Now(), nil, artifacts)
	if err != nil {
		t.Fatalf("capacity-only catalog change on a busy worker: %v", err)
	}
	session.Close()
}

// A catalog change a busy worker cannot take is refused with its reason, the
// enrollment returns that reason, and "worker enroll" prints it.
func TestWorkerEnrollReportsABusyWorkerCatalogRefusal(t *testing.T) {
	cfg, store, artifacts := busyEnrollmentWorker(t)
	project := cfg.BacklogV2.Projects["steward"]
	project.DefaultRef = "release"
	cfg.BacklogV2.Projects["steward"] = project
	binding, err := workerruntime.BuildWorkerBinding(cfg.BacklogV2, "normandy", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	handler := coordinatorEnrollmentHandlerWith(cfg.BacklogV2, store, 1, artifacts, persistentTestCredentials{})
	_, err = handler(context.Background(), backlogadmin.Principal{ID: "operator"}, domain.WorkerEnrollmentRequest{
		ID: "enroll-busy", WorkerID: "normandy", CatalogRevision: binding.CatalogRevision, Reason: "projects changed", ExpectedRevision: 0,
	})
	if err == nil {
		t.Fatal("busy worker accepted an incompatible catalog change")
	}
	for _, want := range []string{`worker "normandy" has 1 active or parked assignment`, "catalog change touches projects", "retry when idle"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("enrollment refusal %q does not say %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "EOF") {
		t.Fatalf("enrollment refusal is still a bare stream end: %v", err)
	}

	statePath, configPath := enrollmentTestConfig(t)
	serveFakeCoordinator(t, statePath, func(request map[string]any) any {
		if request["operation"] == "query" {
			return workersAnswer(staleWorker("normandy", 1))
		}
		return map[string]any{"version": backlogadmin.LocalTransportVersion, "error": err.Error()}
	})
	var out bytes.Buffer
	printed := runWorkerEnroll(globalFlags{configPath: configPath}, []string{"normandy", "--current-catalog", "--reason", "projects changed"}, &out)
	if printed == nil || !strings.Contains(printed.Error(), "retry when idle") || !strings.Contains(printed.Error(), "catalog change touches projects") {
		t.Fatalf("worker enroll reported %v", printed)
	}
}
