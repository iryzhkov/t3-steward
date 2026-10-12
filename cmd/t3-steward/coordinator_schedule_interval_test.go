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
)

type scheduledPassTiming struct {
	start time.Time
	end   time.Time
}

type timedCoordinatorSchedules struct {
	delay  time.Duration
	passes []scheduledPassTiming
}

func (s *timedCoordinatorSchedules) Tick(context.Context) (backlog.ScheduleTickReport, error) {
	pass := scheduledPassTiming{start: time.Now()}
	time.Sleep(s.delay)
	pass.end = time.Now()
	s.passes = append(s.passes, pass)
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
	const interval = 50 * time.Millisecond
	schedules := &timedCoordinatorSchedules{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*interval)
	defer cancel()
	serveTimedCoordinator(t, ctx, schedules, nil, interval)
	if n := len(schedules.passes); n < 5 {
		t.Fatalf("fast passes = %d, want at least 5 over %s", n, 10*interval)
	}
	assertScheduledPassGaps(t, schedules.passes, interval)
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
