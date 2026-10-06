package main

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/jocasta"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// coordinatorLedger runs the Jocasta milestone ledger beside the boundary
// cycle rather than inside it. Every ledger write is a jocasta CLI call that
// can take as long as its timeout, and a boundary that waited on Jocasta would
// delay collection, planning and worker exchange for a document nobody's work
// depends on. A Tick therefore starts a pass in the background unless one is
// already running, and returns at once.
type coordinatorLedger struct {
	reconciler *backlog.LedgerReconciler
	logger     *slog.Logger
	running    atomic.Bool
	passes     sync.WaitGroup
	mu         sync.Mutex
	cancel     context.CancelFunc
}

// newCoordinatorLedger writes ledgers through the jocasta CLI configured on
// this host, with its content files under the configured workspaces root.
func newCoordinatorLedger(cfg config.Config, store *sqlite.Store, artifacts backlog.CoordinatorArtifactStore, logger *slog.Logger) *coordinatorLedger {
	return &coordinatorLedger{
		reconciler: &backlog.LedgerReconciler{
			Records:      store.LoadCoordinatorRecords,
			ReviewRounds: store.ListReviewRoundsForRun,
			States:       store,
			Client:       jocasta.CLI{TempDir: filepath.Join(cfg.BacklogV2.Storage.Workspaces, "jocasta-ledger")},
			Open: func(ctx context.Context, id string) (domain.Artifact, io.ReadCloser, error) {
				artifact, content, err := artifacts.Open(ctx, id)
				return artifact, content, err
			},
			Usage: store.AttributedUsage,
			Waits: store.ListTaskWaits,
		},
		logger: logger,
	}
}

// Tick starts one ledger pass unless one is still running.
func (l *coordinatorLedger) Tick(ctx context.Context) {
	if l == nil || l.reconciler == nil || !l.running.CompareAndSwap(false, true) {
		return
	}
	l.passes.Add(1)
	ctx, cancel := context.WithCancel(ctx)
	l.mu.Lock()
	l.cancel = cancel
	l.mu.Unlock()
	go func() {
		defer l.passes.Done()
		defer l.running.Store(false)
		defer cancel()
		report := l.reconciler.Tick(ctx)
		for _, err := range report.Errors {
			logTickFailure(ctx, l.logger, "milestone ledger reconciliation failed", err)
		}
		for _, run := range report.Behind {
			l.logger.Warn("milestone ledger is behind; the run continues and the ledger is retried with backoff",
				"run", run, "inspect", "coordinator_ledgers in the state database")
		}
		if len(report.Applied) != 0 {
			l.logger.Info("milestone ledger records written", "records", len(report.Applied))
		}
	}()
}

// wait returns once no pass is running.
func (l *coordinatorLedger) wait() {
	if l == nil {
		return
	}
	l.passes.Wait()
}

// stop cancels a running pass and waits for it. The coordinator calls it before
// it closes the store the pass may still be using; a pass cut short leaves its
// unwritten boundaries pending, and the next coordinator writes them.
func (l *coordinatorLedger) stop() {
	if l == nil {
		return
	}
	l.mu.Lock()
	cancel := l.cancel
	l.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	l.passes.Wait()
}
