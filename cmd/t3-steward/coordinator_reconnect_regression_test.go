package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

// This test changes TMPDIR before parallel tests start; its cleanup restores it.
func TestShortTempDirBoundsSocketPathWithLongTMPDIR(t *testing.T) {
	longRoot := filepath.Join(t.TempDir(), strings.Repeat("long-temp-root-", 10))
	if err := os.MkdirAll(longRoot, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", longRoot)
	root := shortTempDir(t)
	socket := filepath.Join(root, "admin.sock")
	if len(socket) >= 104 {
		t.Fatalf("socket path is %d bytes: %s", len(socket), socket)
	}
	if filepath.Dir(root) != "/tmp" {
		t.Fatalf("long TMPDIR did not fall back to /tmp: %s", root)
	}
	entries, err := os.ReadDir(longRoot)
	if err != nil || len(entries) != 0 {
		t.Fatalf("discarded long directory was not removed: %v, %v", entries, err)
	}
	listener, err := backlogadmin.ListenLocal(socket)
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}

type alwaysOfflineWorker struct {
	calls   int
	reports []backlog.QuotaBridgeReport
}

func (w *alwaysOfflineWorker) Tick(_ context.Context, quota backlog.QuotaBridgeReport) coordinatorWorkerTickReport {
	w.calls++
	w.reports = append(w.reports, quota)
	return coordinatorWorkerTickReport{Results: []coordinatorWorkerTickResult{{WorkerID: "omarchy-pc", Err: errors.New("dial tcp: connect: no route to host")}}}
}

type reconnectQuotaTicker struct {
	calls *int
	fail  bool
}

func (q reconnectQuotaTicker) Tick(context.Context) (backlog.QuotaBridgeReport, error) {
	*q.calls++
	if q.fail {
		return backlog.QuotaBridgeReport{}, errors.New("quota unavailable")
	}
	return backlog.QuotaBridgeReport{ChecksDisabled: true}, nil
}

// Review reproduction: an absent worker must neither accelerate an hour-long
// scheduling interval nor keep dialing every five seconds indefinitely.
func TestReproOfflineWorkerRunsFullCycleEvery5s(t *testing.T) {
	for _, failQuota := range []bool{false, true} {
		name := "healthy-quota"
		if failQuota {
			name = "failed-quota"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := shortTempDir(t)
			store, err := sqlitetest.OpenMigrated(filepath.Join(dir, "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			service, err := backlogadmin.New(store, localAdminAuthorizer{})
			if err != nil {
				t.Fatal(err)
			}
			listener, err := backlogadmin.ListenLocal(filepath.Join(dir, "admin.sock"))
			if err != nil {
				t.Fatal(err)
			}
			var quotaCalls, scheduleCalls, planningCalls, adminCalls int
			worker := &alwaysOfflineWorker{}
			cycle := coordinatorBoundaryCycle{
				quota:     reconnectQuotaTicker{calls: &quotaCalls, fail: failQuota},
				schedules: recordingCoordinatorScheduleTicker{calls: &scheduleCalls},
				planning:  recordingCoordinatorPlanningTicker{calls: &planningCalls},
				admin:     recordingCoordinatorAdminExecutor{calls: &adminCalls},
				workers:   worker, logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			}
			server := &backlogadmin.LocalServer{Listener: listener, Service: coordinatorLocalService{admin: service},
				AllowedUID: uint32(os.Getuid()), MaxRequestBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
				RequestTimeout: time.Second, MaxConcurrent: 4}
			ctx, cancel := context.WithTimeout(context.Background(), 23*time.Second)
			defer cancel()
			if err := serveCoordinatorBoundaries(ctx, server, cycle, time.Hour); err != nil {
				t.Fatal(err)
			}
			t.Logf("quota=%d schedules=%d planning=%d admin=%d exchanges=%d", quotaCalls, scheduleCalls, planningCalls, adminCalls, worker.calls)
			if quotaCalls != 1 || scheduleCalls != 1 {
				t.Fatal("retry ran a full scheduling boundary")
			}
			wantPlanning := 1
			if failQuota {
				wantPlanning = 0
			}
			if planningCalls != wantPlanning || adminCalls != wantPlanning {
				t.Fatal("retry ran planning/admin or bypassed quota failure")
			}
			if worker.calls != 4 {
				t.Fatalf("bounded retry sequence made %d exchanges, want 4", worker.calls)
			}
			for _, report := range worker.reports {
				if report.ChecksDisabled == failQuota {
					t.Fatal("retry did not reuse last boundary quota report")
				}
			}
		})
	}
}

func TestWorkerExchangeUsageStoreGuardAndLocalDatabase(t *testing.T) {
	t.Parallel()
	for _, marker := range []string{"file", "symlink"} {
		t.Run(marker, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			var err error
			if marker == "file" {
				err = os.WriteFile(path+".coordinator.lock", nil, 0600)
			} else {
				err = os.Symlink("missing", path+".coordinator.lock")
			}
			if err != nil {
				t.Fatal(err)
			}
			store, err := workerExchangeUsageStore(path)
			if store != nil {
				store.Close()
			}
			if err != nil || store != nil {
				t.Fatalf("marked path opened: %v", err)
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("marked database created: %v", err)
			}
		})
	}
	path := filepath.Join(t.TempDir(), "worker.db")
	store, err := workerExchangeUsageStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if store == nil {
		t.Fatal("unmarked worker database unavailable")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	// ENOTDIR must fail closed rather than opening a database after a failed check.
	invalid := filepath.Join(path, "child.db")
	if store, err := workerExchangeUsageStore(invalid); err == nil || store != nil {
		t.Fatalf("marker inspection failure did not fail closed: %v", err)
	}
}
