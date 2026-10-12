package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestPendingLedgerWorkNanosecondOrdering(t *testing.T) {
	store, err := OpenMigrated(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "workflow-1", Ledger: &domain.WorkflowLedger{JocastaProject: "steward"}}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1"}},
	}); err != nil {
		t.Fatal(err)
	}
	for _, nanos := range []int{0, 123456789, 999999999} {
		now := time.Date(2026, 10, 12, 12, 0, 0, nanos, time.UTC)
		for _, zone := range []*time.Location{time.UTC, time.FixedZone("east", 2*60*60), time.FixedZone("west", -5*60*60)} {
			for _, offset := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
				next := now.Add(offset).In(zone)
				if err := store.SaveLedgerState(ctx, domain.LedgerState{RunID: "run-1", NextAttemptAt: &next}); err != nil {
					t.Fatal(err)
				}
				got, err := store.PendingLedgerWork(ctx, now)
				if want := offset <= 0; err != nil || got != want {
					t.Fatalf("now %s, retry %s: pending = %v, %v; want %v", now.Format(time.RFC3339Nano), next.Format(time.RFC3339Nano), got, err, want)
				}
			}
		}
	}
}

func TestPendingLedgerWork(t *testing.T) {
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	past, future := now.Add(-time.Minute), now.Add(time.Minute)
	for _, tc := range []struct {
		name   string
		ledger bool
		runs   int
		states []domain.LedgerState
		want   bool
	}{
		{name: "no workflows"},
		{name: "no runs", ledger: true},
		{name: "missing state", ledger: true, runs: 1, want: true},
		{name: "closed", ledger: true, runs: 1, states: []domain.LedgerState{{RunID: "run-1", Closed: true}}},
		{name: "future", ledger: true, runs: 1, states: []domain.LedgerState{{RunID: "run-1", NextAttemptAt: &future}}},
		{name: "past", ledger: true, runs: 1, states: []domain.LedgerState{{RunID: "run-1", NextAttemptAt: &past}}, want: true},
		{name: "now", ledger: true, runs: 1, states: []domain.LedgerState{{RunID: "run-1", NextAttemptAt: &now}}, want: true},
		{name: "open without backoff", ledger: true, runs: 1, states: []domain.LedgerState{{RunID: "run-1"}}, want: true},
		{name: "non ledgered", runs: 1},
		{name: "closed and open", ledger: true, runs: 2, states: []domain.LedgerState{{RunID: "run-1", Closed: true}, {RunID: "run-2"}}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := OpenMigrated(filepath.Join(t.TempDir(), "ledger.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			ctx := context.Background()
			records := CoordinatorRecords{}
			if tc.name != "no workflows" {
				workflow := domain.Workflow{ID: "workflow-1", CreatedAt: now}
				if tc.ledger {
					workflow.Ledger = &domain.WorkflowLedger{JocastaProject: "steward"}
				}
				records.Workflows = []domain.Workflow{workflow}
			}
			for i := 0; i < tc.runs; i++ {
				id := []string{"run-1", "run-2"}[i]
				records.WorkflowRuns = append(records.WorkflowRuns, domain.WorkflowRun{ID: id, WorkflowID: "workflow-1", CreatedAt: now})
			}
			if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
				t.Fatal(err)
			}
			for _, state := range tc.states {
				if err := store.SaveLedgerState(ctx, state); err != nil {
					t.Fatal(err)
				}
			}
			got, err := store.PendingLedgerWork(ctx, now)
			if err != nil || got != tc.want {
				t.Fatalf("PendingLedgerWork = %v, %v; want %v", got, err, tc.want)
			}
		})
	}
}
