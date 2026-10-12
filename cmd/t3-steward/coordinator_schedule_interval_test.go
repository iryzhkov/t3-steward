package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"github.com/iryzhkov/t3-steward/internal/testtiming"
)

type scheduledPassTiming struct {
	start time.Time
	end   time.Time
}

type timedCoordinatorSchedules struct {
	delay  time.Duration
	passes []scheduledPassTiming
	onPass func()
}

func (s *timedCoordinatorSchedules) Tick(context.Context) (backlog.ScheduleTickReport, error) {
	pass := scheduledPassTiming{start: time.Now()}
	time.Sleep(s.delay)
	pass.end = time.Now()
	s.passes = append(s.passes, pass)
	if s.onPass != nil {
		s.onPass()
	}
	return backlog.ScheduleTickReport{}, nil
}

func assertScheduledPassGaps(t *testing.T, passes []scheduledPassTiming, interval time.Duration) {
	t.Helper()
	for i := 1; i < len(passes); i++ {
		if gap := passes[i].start.Sub(passes[i-1].end); gap < interval {
			t.Errorf("pass %d gap = %s, want at least %s", i, gap, interval)
		}
	}
}

func serveTimedCoordinator(t *testing.T, ctx context.Context, schedules *timedCoordinatorSchedules, workers coordinatorWorkerTicker, interval time.Duration) {
	t.Helper()
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
	defer listener.Close()
	server := &backlogadmin.LocalServer{
		Listener: listener, Service: coordinatorLocalService{admin: service},
		AllowedUID: uint32(os.Getuid()), MaxRequestBytes: 1 << 20,
		MaxArtifactBytes: 1 << 20, MaxSubmissionBytes: 1 << 20,
		RequestTimeout: time.Second, MaxConcurrent: 4,
	}
	var quota, planning, admin int
	cycle := coordinatorBoundaryCycle{
		quota: reconnectQuotaTicker{calls: &quota}, schedules: schedules,
		planning: recordingCoordinatorPlanningTicker{calls: &planning},
		admin:    recordingCoordinatorAdminExecutor{calls: &admin},
		workers:  workers, logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if err := serveCoordinatorBoundaries(ctx, server, cycle, interval); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorScheduledSlowPassDoesNotCoalesce(t *testing.T) {
	t.Parallel()
	const interval = 50 * time.Millisecond
	schedules := &timedCoordinatorSchedules{delay: 3 * interval}
	ctx, cancel := context.WithTimeout(context.Background(), 20*interval)
	defer cancel()
	serveTimedCoordinator(t, ctx, schedules, nil, interval)
	if n := len(schedules.passes); n < 2 || n > 6 {
		t.Fatalf("slow passes = %d, want 2 through 6 over %s", n, 20*interval)
	}
	assertScheduledPassGaps(t, schedules.passes, interval)
}

func TestCoordinatorScheduledFastPassKeepsRunning(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		startupDelay time.Duration
	}{
		{name: "normal"},
		// Startup alone exceeds the old 500 ms throughput window.
		{name: "delayed_startup", startupDelay: 600 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const interval = 50 * time.Millisecond
			const scheduledTarget = 4
			// This is only a watchdog for missing progress, not a throughput bound.
			ctx, cancel := context.WithTimeout(context.Background(), testtiming.Bound(10*time.Second))
			defer cancel()
			schedules := &timedCoordinatorSchedules{delay: tc.startupDelay}
			var ready bool
			var startupPasses int
			ctx = withCoordinatorStarted(ctx, func() {
				startupPasses = len(schedules.passes)
				ready = true
				schedules.delay = 0
			})
			// The callback, readiness baseline and recordings all run on the
			// coordinator goroutine. Inspect them only after shutdown returns.
			schedules.onPass = func() {
				if ready && len(schedules.passes)-startupPasses >= scheduledTarget {
					cancel()
				}
			}
			serveTimedCoordinator(t, ctx, schedules, nil, interval)
			if !ready {
				t.Fatal("coordinator did not report readiness")
			}
			if n := len(schedules.passes) - startupPasses; n < scheduledTarget {
				t.Fatalf("scheduled passes after readiness = %d, want at least %d (watchdog: %v)", n, scheduledTarget, ctx.Err())
			}
			assertScheduledPassGaps(t, schedules.passes, interval)
		})
	}
}

type timedCoordinatorRetry struct {
	scheduledPassTiming
	ids []string
}

type timedCoordinatorWorkers struct {
	full    int
	retries []timedCoordinatorRetry
}

func (w *timedCoordinatorWorkers) Tick(context.Context, backlog.QuotaBridgeReport) coordinatorWorkerTickReport {
	w.full++
	return coordinatorWorkerTickReport{Results: []coordinatorWorkerTickResult{
		{WorkerID: "A", Err: errors.New("transport")}, {WorkerID: "B"},
	}}
}

func (w *timedCoordinatorWorkers) TickWorkers(_ context.Context, _ backlog.QuotaBridgeReport, ids []string) coordinatorWorkerTickReport {
	retry := timedCoordinatorRetry{
		scheduledPassTiming: scheduledPassTiming{start: time.Now()},
		ids:                 append([]string(nil), ids...),
	}
	report := coordinatorWorkerTickReport{Results: []coordinatorWorkerTickResult{
		{WorkerID: "A", Err: errors.New("transport")},
	}}
	retry.end = time.Now()
	w.retries = append(w.retries, retry)
	return report
}

func TestCoordinatorScheduledSlowPassPreservesRetryBackoff(t *testing.T) {
	t.Parallel()
	const interval = 100 * time.Millisecond
	schedules := &timedCoordinatorSchedules{delay: 3 * interval}
	workers := &timedCoordinatorWorkers{}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	var ready time.Time
	var stopAfterReady *time.Timer
	ctx = withCoordinatorStarted(ctx, func() {
		ready = time.Now()
		stopAfterReady = time.AfterFunc(4500*time.Millisecond, cancel)
	})
	serveTimedCoordinator(t, ctx, schedules, workers, interval)
	if stopAfterReady != nil {
		stopAfterReady.Stop()
	}
	if ready.IsZero() {
		t.Fatal("coordinator did not report readiness")
	}
	if len(workers.retries) != 2 {
		t.Fatalf("subset retries = %d, want exactly 2", len(workers.retries))
	}
	for i, retry := range workers.retries {
		if !reflect.DeepEqual(retry.ids, []string{"A"}) {
			t.Errorf("retry %d subset = %v, want [A]", i, retry.ids)
		}
	}
	if gap := workers.retries[0].start.Sub(ready); gap < time.Second {
		t.Errorf("first retry after readiness = %s, want at least 1s", gap)
	}
	if gap := workers.retries[1].start.Sub(workers.retries[0].end); gap < 2*time.Second {
		t.Errorf("second retry gap = %s, want at least 2s", gap)
	}
	if workers.full < 5 || workers.full != len(schedules.passes)-1 {
		t.Errorf("full worker passes = %d, scheduled passes including startup = %d", workers.full, len(schedules.passes))
	}
	assertScheduledPassGaps(t, schedules.passes, interval)
	// Scheduled passes must also continue after the second retry.
	if schedules.passes[len(schedules.passes)-1].start.Before(workers.retries[1].end) {
		t.Error("scheduled passes stopped after the second retry")
	}
}
