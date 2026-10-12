package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/testtiming"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

type reconnectWorkerTicker struct {
	exchange  func(context.Context) (domain.WorkerSnapshot, error)
	snapshots chan domain.WorkerSnapshot
	failOnce  bool
	mu        sync.Mutex
	state     reconnectExchangeState
}

type reconnectExchangeState struct {
	attempts, completed, firstFailure, lastSuccess int
	lastError                                      error
}

func (s reconnectExchangeState) String() string {
	return fmt.Sprintf("attempts=%d completed=%d first_failure=%d last_success=%d last_error=%v",
		s.attempts, s.completed, s.firstFailure, s.lastSuccess, s.lastError)
}

func (w *reconnectWorkerTicker) observed() reconnectExchangeState {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.state
}

func (w *reconnectWorkerTicker) Tick(ctx context.Context, _ backlog.QuotaBridgeReport) coordinatorWorkerTickReport {
	w.mu.Lock()
	w.state.attempts++
	attempt := w.state.attempts
	w.mu.Unlock()
	var snapshot domain.WorkerSnapshot
	var err error
	if w.failOnce {
		w.failOnce = false
		err = errors.New("connection interrupted")
	} else {
		snapshot, err = w.exchange(ctx)
	}
	w.mu.Lock()
	w.state.completed++
	if err != nil {
		w.state.lastError = err
		if w.state.firstFailure == 0 {
			w.state.firstFailure = attempt
		}
	} else {
		w.state.lastSuccess = attempt
	}
	w.mu.Unlock()
	if err == nil {
		select {
		case w.snapshots <- snapshot:
		case <-ctx.Done():
		}
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
	if retry.pending || retry.delay != 0 {
		t.Fatal("successful exchange did not clear retry and reset backoff")
	}
	if got := retry.nextDelay(); got != time.Second {
		t.Fatalf("fresh sequence starts at %s", got)
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
	const scheduleInterval = 3 * time.Minute
	watchdog := testtiming.Bound(8 * time.Second)
	if watchdog >= scheduleInterval {
		t.Fatal("reconnect watchdog must expire before an ordinary scheduling pass")
	}
	snapshots := make(chan domain.WorkerSnapshot, 1)
	var previous domain.WorkerSnapshot
	for _, epoch := range []int64{1, 2} {
		listener, err := backlogadmin.ListenLocal(filepath.Join(filepath.Dir(cfg.BacklogV2.Workers["normandy"].Address), "admin.sock"))
		if err != nil {
			t.Fatal(err)
		}
		epochStarted := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), watchdog)
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
		go func() { done <- serveCoordinatorBoundaries(ctx, server, cycle, scheduleInterval) }()
		select {
		case <-started:
		case err := <-done:
			cancel()
			t.Fatalf("epoch %d coordinator exited before readiness: %v; %s", epoch, err, worker.observed())
		case <-ctx.Done():
			state := worker.observed()
			cancel()
			<-done
			t.Fatalf("epoch %d watchdog expired: ready=false; %s", epoch, state)
		}
		var snapshot domain.WorkerSnapshot
		select {
		case snapshot = <-snapshots:
		case err := <-done:
			cancel()
			t.Fatalf("epoch %d coordinator exited before snapshot: %v; ready=true; %s", epoch, err, worker.observed())
		case <-ctx.Done():
			state := worker.observed()
			cancel()
			<-done
			t.Fatalf("epoch %d watchdog expired: ready=true; %s", epoch, state)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if snapshot.WorkerID != "normandy" {
			t.Fatalf("worker identity = %q, want normandy", snapshot.WorkerID)
		}
		if snapshot.CoordinatorEpoch != epoch {
			t.Fatalf("coordinator epoch = %d, want %d", snapshot.CoordinatorEpoch, epoch)
		}
		if epoch == 2 && (snapshot.WorkerEpoch != previous.WorkerEpoch || snapshot.Inventory.CatalogRevision != previous.Inventory.CatalogRevision || snapshot.Sequence <= previous.Sequence) {
			t.Fatal("coordinator restart replaced worker identity or reset sequence")
		}
		state := worker.observed()
		if state.lastSuccess == 0 || (epoch == 2 && (state.firstFailure == 0 || state.lastSuccess <= state.firstFailure)) {
			t.Fatalf("epoch %d snapshot did not follow the expected exchange ordering: %s", epoch, state)
		}
		t.Logf("epoch %d recovered in %s: %s", epoch, time.Since(epochStarted), state)
		previous = snapshot
	}
}

func TestReconnectWorkerTickerObservesExchangeErrorsBeforeSnapshot(t *testing.T) {
	interrupted := errors.New("exchange transport failed")
	worker := &reconnectWorkerTicker{snapshots: make(chan domain.WorkerSnapshot, 1)}
	before := worker.observed()
	worker.exchange = func(context.Context) (domain.WorkerSnapshot, error) {
		state := worker.observed() // Also checks the exchange does not hold the observer lock.
		if state.attempts == before.attempts+1 {
			return domain.WorkerSnapshot{}, interrupted
		}
		return domain.WorkerSnapshot{CoordinatorEpoch: 2}, nil
	}
	worker.Tick(context.Background(), backlog.QuotaBridgeReport{})
	state := worker.observed()
	if state.attempts-before.attempts != 1 || state.completed-before.completed != 1 || state.firstFailure != state.attempts || state.lastSuccess != 0 || !errors.Is(state.lastError, interrupted) {
		t.Fatalf("failed exchange observation: %s", state)
	}
	select {
	case <-worker.snapshots:
		t.Fatal("failed exchange published a snapshot")
	default:
	}
	failed := state
	worker.Tick(context.Background(), backlog.QuotaBridgeReport{})
	state = worker.observed()
	if state.completed-failed.completed != 1 || state.lastSuccess <= state.firstFailure || !errors.Is(state.lastError, interrupted) {
		t.Fatalf("successful retry lost failure ordering or diagnostics: %s", state)
	}
	if snapshot := <-worker.snapshots; snapshot.CoordinatorEpoch != 2 {
		t.Fatalf("successful snapshot = %+v", snapshot)
	}
}
