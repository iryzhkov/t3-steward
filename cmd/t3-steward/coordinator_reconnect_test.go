package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type reconnectWorkerTicker struct {
	exchange  func(context.Context) (domain.WorkerSnapshot, error)
	snapshots chan domain.WorkerSnapshot
	failOnce  bool
	calls     int
}

func (w *reconnectWorkerTicker) Tick(ctx context.Context, _ backlog.QuotaBridgeReport) coordinatorWorkerTickReport {
	w.calls++
	if w.failOnce {
		w.failOnce = false
		return coordinatorWorkerTickReport{Results: []coordinatorWorkerTickResult{{WorkerID: "normandy", Err: errors.New("connection interrupted")}}}
	}
	snapshot, err := w.exchange(ctx)
	if err == nil {
		w.snapshots <- snapshot
	}
	return coordinatorWorkerTickReport{Results: []coordinatorWorkerTickResult{{WorkerID: "normandy", Err: err}}}
}

func TestCoordinatorReconnectBackoffIsBoundedAndResetsOnSuccess(t *testing.T) {
	worker := &reconnectWorkerTicker{failOnce: true, snapshots: make(chan domain.WorkerSnapshot, 1),
		exchange: func(context.Context) (domain.WorkerSnapshot, error) { return domain.WorkerSnapshot{}, nil }}
	retry := coordinatorReconnect{workers: worker, pending: true}
	for _, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second} {
		if got := retry.nextDelay(); got != want {
			t.Fatalf("retry delay = %s, want %s", got, want)
		}
	}
	retry.Tick(context.Background(), backlog.QuotaBridgeReport{})
	if !retry.pending {
		t.Fatal("failed exchange did not retain retry")
	}
	retry.Tick(context.Background(), backlog.QuotaBridgeReport{})
	if retry.pending {
		t.Fatal("successful exchange did not clear retry")
	}
}

// Run the coordinator boundary server twice while the worker daemon stays up.
// The scheduling interval is deliberately minutes; restart recovery must reach
// the existing worker within seconds, including one interrupted connection.
func TestCoordinatorRestartReconnectsWorkerWithNewEpochPromptly(t *testing.T) {
	cfg, store, artifacts := busyEnrollmentWorker(t)
	service, err := backlogadmin.New(store, localAdminAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	snapshots := make(chan domain.WorkerSnapshot, 1)
	var previous domain.WorkerSnapshot
	for _, epoch := range []int64{1, 2} {
		listener, err := backlogadmin.ListenLocal(filepath.Join(filepath.Dir(cfg.BacklogV2.Workers["normandy"].Address), "admin.sock"))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		worker := &reconnectWorkerTicker{snapshots: snapshots, failOnce: epoch == 2}
		worker.exchange = func(ctx context.Context) (domain.WorkerSnapshot, error) {
			id, err := newCoordinatorWorkerSessionID("coordinator", "normandy")
			if err != nil {
				return domain.WorkerSnapshot{}, err
			}
			session, err := newCoordinatorWorkerSession(ctx, cfg.BacklogV2, store, "normandy", epoch, id, persistentTestCredentials{}, time.Now(), nil, artifacts)
			if err != nil {
				return domain.WorkerSnapshot{}, err
			}
			defer session.Close()
			return session.Client.Snapshot(ctx, workerproto.SnapshotRequest{ParkedReported: true})
		}
		var quotaCalls, scheduleCalls, planningCalls, adminCalls int
		cycle := coordinatorBoundaryCycle{
			quota:     failingCoordinatorQuotaTicker{calls: &quotaCalls},
			schedules: recordingCoordinatorScheduleTicker{calls: &scheduleCalls},
			planning:  recordingCoordinatorPlanningTicker{calls: &planningCalls},
			admin:     recordingCoordinatorAdminExecutor{calls: &adminCalls},
			workers:   worker, logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		server := &backlogadmin.LocalServer{Listener: listener, Service: coordinatorLocalService{admin: service},
			AllowedUID: uint32(os.Getuid()), MaxRequestBytes: 1 << 20, MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
			RequestTimeout: time.Second, MaxConcurrent: 4}
		started := make(chan struct{})
		ctx = withCoordinatorStarted(ctx, func() { close(started) })
		done := make(chan error, 1)
		go func() { done <- serveCoordinatorBoundaries(ctx, server, cycle, 3*time.Minute) }()
		select {
		case <-started:
		case err := <-done:
			cancel()
			t.Fatalf("coordinator startup failed: %v", err)
		case <-ctx.Done():
			cancel()
			<-done
			t.Fatal("coordinator startup timed out")
		}
		var snapshot domain.WorkerSnapshot
		select {
		case snapshot = <-snapshots:
		case <-ctx.Done():
			cancel()
			<-done
			t.Fatalf("epoch %d did not reconnect within seconds", epoch)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if snapshot.CoordinatorEpoch != epoch {
			t.Fatalf("coordinator epoch = %d, want %d", snapshot.CoordinatorEpoch, epoch)
		}
		if epoch == 2 && (snapshot.WorkerEpoch != previous.WorkerEpoch || snapshot.Inventory.CatalogRevision != previous.Inventory.CatalogRevision || snapshot.Sequence <= previous.Sequence) {
			t.Fatal("coordinator restart replaced worker identity or reset sequence")
		}
		if worker.calls != int(epoch) {
			t.Fatalf("exchange attempts = %d, want %d", worker.calls, epoch)
		}
		previous = snapshot
	}
}
