package main

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/jocasta"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/testtiming"
)

type unusedLedgerStates struct{}

func (unusedLedgerStates) LoadLedgerStates(context.Context) ([]domain.LedgerState, error) {
	return nil, nil
}

func (unusedLedgerStates) SaveLedgerState(context.Context, domain.LedgerState) error { return nil }

// TestCoordinatorLedgerNeverBlocksTheBoundary proves a slow Jocasta cannot hold
// a coordinator boundary: the pass runs beside the cycle, one at a time, and
// shutdown waits for it.
func TestCoordinatorLedgerNeverBlocksTheBoundary(t *testing.T) {
	release := make(chan struct{})
	var passes atomic.Int32
	ledger := &coordinatorLedger{
		reconciler: &backlog.LedgerReconciler{
			Records: func(context.Context) (sqlite.CoordinatorRecords, error) {
				passes.Add(1)
				<-release
				return sqlite.CoordinatorRecords{}, nil
			},
			States: unusedLedgerStates{},
			Client: jocasta.CLI{Binary: "/nonexistent/jocasta"},
		},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	started := time.Now()
	ledger.Tick(context.Background())
	ledger.Tick(context.Background())
	if elapsed := time.Since(started); elapsed > testtiming.Bound(time.Second) {
		t.Fatalf("ledger ticks blocked the boundary for %s", elapsed)
	}
	deadline := time.Now().Add(testtiming.Bound(5 * time.Second))
	for passes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if passes.Load() != 1 {
		t.Fatalf("passes = %d, want exactly one while the first is running", passes.Load())
	}
	close(release)
	ledger.wait()
	ledger.Tick(context.Background())
	ledger.wait()
	if passes.Load() != 2 {
		t.Fatalf("passes = %d after the first finished, want 2", passes.Load())
	}

	// Shutdown cancels a pass that is still waiting on Jocasta.
	blocked := &coordinatorLedger{
		reconciler: &backlog.LedgerReconciler{
			Records: func(ctx context.Context) (sqlite.CoordinatorRecords, error) {
				<-ctx.Done()
				return sqlite.CoordinatorRecords{}, ctx.Err()
			},
			States: unusedLedgerStates{},
			Client: jocasta.CLI{Binary: "/nonexistent/jocasta"},
		},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	blocked.Tick(context.Background())
	stopped := make(chan struct{})
	go func() { blocked.stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(testtiming.Bound(5 * time.Second)):
		t.Fatal("stop did not cancel the running pass")
	}

	var unconfigured *coordinatorLedger
	unconfigured.Tick(context.Background())
	unconfigured.wait()
	unconfigured.stop()
}
