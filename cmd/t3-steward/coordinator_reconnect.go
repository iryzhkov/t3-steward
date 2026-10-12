package main

import (
	"context"
	"errors"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

const coordinatorReconnectMaxDelay = 5 * time.Second

// coordinatorReconnect tracks failed exchanges independently of the scheduling
// interval. A new authority must reach workers promptly even when normal
// scheduling runs only every few minutes.
type coordinatorReconnect struct {
	workers   coordinatorWorkerTicker
	pending   bool
	workerIDs []string
	delay     time.Duration
}

func (r *coordinatorReconnect) Tick(ctx context.Context, quota backlog.QuotaBridgeReport) coordinatorWorkerTickReport {
	report := r.workers.Tick(ctx, quota)
	r.record(report)
	return report
}

// Retry uses nil workerIDs for the initial exchange of all configured workers.
func (r *coordinatorReconnect) Retry(ctx context.Context, quota backlog.QuotaBridgeReport) coordinatorWorkerTickReport {
	var report coordinatorWorkerTickReport
	if subset, ok := r.workers.(coordinatorWorkerSubsetTicker); ok && r.workerIDs != nil {
		report = subset.TickWorkers(ctx, quota, r.workerIDs)
	} else {
		report = r.workers.Tick(ctx, quota)
	}
	r.record(report)
	return report
}

func (r *coordinatorReconnect) record(report coordinatorWorkerTickReport) {
	r.workerIDs = []string{}
	for _, result := range report.Results {
		if coordinatorWorkerRetryable(result) {
			r.workerIDs = append(r.workerIDs, result.WorkerID)
		}
	}
	r.pending = len(r.workerIDs) > 0
	if !r.pending {
		r.delay = 0
	}
}

type coordinatorWorkerSubsetTicker interface {
	TickWorkers(context.Context, backlog.QuotaBridgeReport, []string) coordinatorWorkerTickReport
}

// coordinatorWorkerRetryable leaves freshly observed non-accepting workers to
// the next scheduled pass, which still collects usage, snapshots and artifacts.
func coordinatorWorkerRetryable(result coordinatorWorkerTickResult) bool {
	if result.Err == nil {
		return false
	}
	snapshot := result.Report.Snapshot
	if snapshot.WorkerID == "" || snapshot.WorkerID != result.WorkerID ||
		(snapshot.Inventory.AcceptBacklog && snapshot.Inventory.Health == domain.WorkerHealthReady) {
		return true
	}
	errs := []error{result.Err}
	if joined, ok := result.Err.(interface{ Unwrap() []error }); ok {
		errs = joined.Unwrap()
	}
	for _, err := range errs {
		if err != nil && !errors.Is(err, sqlite.ErrWorkerUnavailable) {
			return true
		}
	}
	return false
}

func (r *coordinatorReconnect) nextDelay() time.Duration {
	if r.delay == 0 {
		r.delay = time.Second
	} else {
		r.delay *= 2
		if r.delay > coordinatorReconnectMaxDelay {
			r.delay = coordinatorReconnectMaxDelay
		}
	}
	return r.delay
}
