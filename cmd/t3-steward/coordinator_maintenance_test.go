package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestCoordinatorWorkerNeverOpensCoordinatorDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.AcquireCoordinator(context.Background(), "normandy"); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{StatePath: path}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	// Both reads used to hold this DB open even while the worker was drained.
	// A separate worker-mode configuration may also point at the same DB.
	for _, mode := range []string{"coordinator", "worker"} {
		cfg.BacklogV2.Mode = mode
		if guard := hostQuotaGuard(cfg, logger, nil); guard != nil {
			t.Fatalf("%s worker opened coordinator quota store: %T", mode, guard)
		}
		if usage, err := workerExchangeUsageStore(path); err != nil || usage != nil {
			if usage != nil {
				usage.Close()
			}
			t.Fatalf("one-shot worker opened coordinator usage store: %v", err)
		}
		if usage := hostUsageStore(cfg, logger); usage != nil {
			usage.Close()
			t.Fatalf("%s worker opened coordinator usage store", mode)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(path + suffix); !os.IsNotExist(err) {
			t.Fatalf("worker retained coordinator DB sidecar %s: %v", suffix, err)
		}
	}
}

func readyHealthWorkers(epoch int64) []backlogadmin.Worker {
	return []backlogadmin.Worker{
		{Snapshot: domain.WorkerSnapshot{WorkerID: "worker", Connected: true, CoordinatorEpoch: epoch}},
		{Requirement: &domain.WorkerRequirement{WorkerID: "retired", Connection: "removed"}},
	}
}

func TestCoordinatorHealthWaitsForWorkersAndHealthyStatus(t *testing.T) {
	var output bytes.Buffer
	calls := 0
	start := time.Now()
	err := runCoordinatorHealth(context.Background(), &output, true, true, time.Second, 5*time.Millisecond, "coordinator",
		func(context.Context) (backlogadmin.Status, []backlogadmin.Worker, error) {
			calls++
			status := backlogadmin.Status{Runtime: backlogadmin.RuntimeStatus{Epoch: 2, Release: "test-release", Health: "healthy"}}
			switch calls {
			case 1:
				return status, nil, &backlogadmin.TransportError{Class: backlogadmin.ClassUnavailable, Err: errors.New("restarting")}
			case 2:
				// Healthy status alone is insufficient when a configured worker
				// has never produced a snapshot.
				return status, []backlogadmin.Worker{{Requirement: &domain.WorkerRequirement{WorkerID: "worker"}}}, nil
			case 3:
				// A connected old-epoch snapshot is insufficient as well.
				return status, readyHealthWorkers(1), nil
			case 4:
				status.Runtime.Health = "degraded"
			}
			return status, readyHealthWorkers(2), nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 5 || time.Since(start) < 20*time.Millisecond {
		t.Fatalf("wait returned early: calls=%d elapsed=%s", calls, time.Since(start))
	}
	var doc coordinatorHealthDocument
	if err := json.Unmarshal(output.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	if !doc.Ready || doc.Epoch != 2 || doc.Release != "test-release" || len(doc.ConnectedWorkers) != 1 || len(doc.ExpectedWorkers) != 1 || doc.Error != "" {
		t.Fatalf("document: %+v", doc)
	}
}

func TestCoordinatorHealthTimeoutPrintsLastDocument(t *testing.T) {
	var output bytes.Buffer
	err := runCoordinatorHealth(context.Background(), &output, true, true, 20*time.Millisecond, time.Millisecond, "coordinator",
		func(context.Context) (backlogadmin.Status, []backlogadmin.Worker, error) {
			return backlogadmin.Status{Runtime: backlogadmin.RuntimeStatus{Epoch: 4, Health: "healthy"}}, readyHealthWorkers(3), nil
		})
	if backlogadmin.ClassOf(err) != backlogadmin.ClassTimeout {
		t.Fatalf("timeout: %v", err)
	}
	var printed documentPrinted
	if !errors.As(err, &printed) {
		t.Fatal("timeout would print second JSON document")
	}
	var doc coordinatorHealthDocument
	if err := json.Unmarshal(output.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Ready || doc.Epoch != 4 || len(doc.MissingWorkers) != 1 {
		t.Fatalf("document: %+v", doc)
	}
}

func TestCoordinatorHealthPermanentFailureIsNotRetried(t *testing.T) {
	calls := 0
	err := runCoordinatorHealth(context.Background(), io.Discard, true, true, time.Second, time.Millisecond, "coordinator",
		func(context.Context) (backlogadmin.Status, []backlogadmin.Worker, error) {
			calls++
			return backlogadmin.Status{}, nil, &backlogadmin.TransportError{Class: backlogadmin.ClassAuthentication, Err: errors.New("credential rejected")}
		})
	if calls != 1 || backlogadmin.ClassOf(err) != backlogadmin.ClassAuthentication {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestCoordinatorMaintenancePaths(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	for input, want := range map[string]string{"~": home, "~/snapshots/a": filepath.Join(home, "snapshots/a"), "relative/a": ""} {
		got, err := maintenancePath(input)
		if err != nil || !filepath.IsAbs(got) || (want != "" && got != want) {
			t.Fatalf("%q: %q %v", input, got, err)
		}
	}
	if _, err := maintenancePath("~someone/snapshot"); err == nil || !strings.Contains(err.Error(), "absolute path") {
		t.Fatalf("unsupported tilde: %v", err)
	}
	if err := cmdCoordinatorBackup(globalFlags{}, []string{"--out", "snapshot"}); err == nil || !strings.Contains(err.Error(), "operator-controlled") {
		t.Fatalf("backup authority: %v", err)
	}
	cfg := config.Config{}
	cfg.BacklogV2.Mode = "worker"
	if err := runCoordinatorBackup(context.Background(), cfg, io.Discard, "snapshot"); err == nil {
		t.Fatal("worker could create coordinator backup")
	}
}

func TestCoordinatorBackupDrillIgnoresProductionConfiguration(t *testing.T) {
	// Argument admission reaches snapshot verification despite a nonexistent
	// production configuration. Successful restoration is covered by the
	// package's online backup/drill integration test.
	err := cmdCoordinatorBackup(globalFlags{configPath: filepath.Join(t.TempDir(), "absent-config")}, []string{"verify", filepath.Join(t.TempDir(), "absent-snapshot"), "--restore-drill"})
	if err == nil || strings.Contains(err.Error(), "absent-config") {
		t.Fatalf("drill loaded production configuration: %v", err)
	}
}
