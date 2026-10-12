package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
)

type recordingAuditPruner struct {
	cutoffs []time.Time
	err     error
}

func (p *recordingAuditPruner) PruneAuditEvents(_ context.Context, cutoff time.Time) (int64, error) {
	p.cutoffs = append(p.cutoffs, cutoff)
	return 1000, p.err
}

func TestCoordinatorAuditRetentionRunsOneBatchPerBoundary(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			var quotaCalls, scheduleCalls, planningCalls, adminCalls int
			var logs bytes.Buffer
			pruner := &recordingAuditPruner{}
			if fail {
				pruner.err = errors.New("prune failed")
			}
			retention := 48 * time.Hour
			cycle := coordinatorBoundaryCycle{
				auditPruner: pruner, auditRetention: retention,
				quota:     failingCoordinatorQuotaTicker{calls: &quotaCalls},
				schedules: recordingCoordinatorScheduleTicker{calls: &scheduleCalls},
				planning:  recordingCoordinatorPlanningTicker{calls: &planningCalls},
				admin:     recordingCoordinatorAdminExecutor{calls: &adminCalls},
				logger:    slog.New(slog.NewTextHandler(&logs, nil)),
			}
			start := time.Now()
			cycle.Tick(context.Background())
			cycle.TickWithWorkers(context.Background())
			end := time.Now()
			if len(pruner.cutoffs) != 2 {
				t.Fatalf("batches = %d, want one per boundary", len(pruner.cutoffs))
			}
			for _, cutoff := range pruner.cutoffs {
				if cutoff.Before(start.Add(-retention)) || cutoff.After(end.Add(-retention)) {
					t.Fatalf("cutoff %s outside configured retention", cutoff)
				}
			}
			cycle.tickWorkersOnly(context.Background(), backlog.QuotaBridgeReport{})
			if len(pruner.cutoffs) != 2 {
				t.Fatal("worker retry pruned extra batch")
			}
			if quotaCalls != 2 || scheduleCalls != 2 {
				t.Fatal("pruning prevented normal boundary work")
			}
			if fail && !strings.Contains(logs.String(), "coordinator audit retention failed") {
				t.Fatalf("prune error was not logged: %s", logs.String())
			}
		})
	}
}
