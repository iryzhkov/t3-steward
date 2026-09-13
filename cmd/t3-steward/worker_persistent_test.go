package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

type persistentTestCredentials struct{}

func (persistentTestCredentials) ResolveProtocol(context.Context, string) (workerruntime.ProtocolCredentials, error) {
	return workerruntime.ProtocolCredentials{CoordinatorPrincipal: "ssh:coordinator", CoordinatorKeyID: "coordinator-key", CoordinatorSecret: []byte("coordinator-secret"), WorkerPrincipal: "ssh:normandy", WorkerKeyID: "worker-key", WorkerSecret: []byte("worker-secret-material")}, nil
}

func TestPersistentCoordinatorCatalogAndExecutionSessionsShareWorker(t *testing.T) {
	root, err := os.MkdirTemp("", "t3-s4-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
	cfg.BacklogV2.Workers["normandy"] = worker
	host := &workerruntime.CatalogHost{Home: t.TempDir(), Bootstrap: workerruntime.WorkerBootstrap{SchemaVersion: 1, WorkerID: "normandy", CoordinatorID: "coordinator", Transport: "ssh", Capabilities: worker.Capabilities, ProviderRoutes: []string{"codex"}, CredentialRef: worker.Credential}, Options: workerruntime.WorkerServiceOptions{ProtocolCredentials: persistentTestCredentials{}, DryRun: true}}
	listener, err := net.Listen("unix", worker.Address)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- workerruntime.ServeWorkerListener(ctx, listener, 24<<20, time.Minute, 4, host.HandleFrame)
	}()
	defer func() {
		cancel()
		if err := <-done; err != nil && ctx.Err() == nil {
			t.Error(err)
		}
	}()
	store, err := sqlite.OpenMigrated(filepath.Join(root, "coordinator.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	artifacts := backlog.CoordinatorArtifactStore{Root: filepath.Join(root, "artifacts"), Catalog: store}
	session, err := newCoordinatorWorkerSession(ctx, cfg.BacklogV2, store, "normandy", 1, "first", persistentTestCredentials{}, time.Now(), nil, artifacts)
	if err != nil {
		t.Fatal(err)
	}
	first, err := session.Client.Snapshot(ctx)
	if err != nil {
		t.Fatal("execution sequence after catalog", err)
	}
	second, err := session.Client.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.Inventory.CatalogRevision != session.Binding.CatalogRevision || second.Sequence <= first.Sequence {
		t.Fatal("persistent runtime or catalog identity lost")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	reconnected, err := newCoordinatorWorkerSession(ctx, cfg.BacklogV2, store, "normandy", 1, "reconnected", persistentTestCredentials{}, time.Now(), nil, artifacts)
	if err != nil {
		t.Fatal(err)
	}
	defer reconnected.Close()
	third, err := reconnected.Client.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if third.Sequence <= second.Sequence || third.WorkerEpoch != first.WorkerEpoch || third.Inventory.CatalogRevision != first.Inventory.CatalogRevision {
		t.Fatal("SSH reconnect replaced worker authority")
	}
}
