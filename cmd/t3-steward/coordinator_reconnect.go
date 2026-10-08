package main

import (
	"context"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
)

const coordinatorReconnectMaxDelay = 5 * time.Second

// coordinatorReconnect tracks failed exchanges independently of the scheduling
// interval. A new authority must reach workers promptly even when normal
// scheduling runs only every few minutes.
type coordinatorReconnect struct {
	workers coordinatorWorkerTicker
	pending bool
	delay   time.Duration
}

func (r *coordinatorReconnect) Tick(ctx context.Context, quota backlog.QuotaBridgeReport) coordinatorWorkerTickReport {
	report := r.workers.Tick(ctx, quota)
	r.pending = false
	for _, result := range report.Results {
		if result.Err != nil {
			r.pending = true
		}
	}
	return report
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
