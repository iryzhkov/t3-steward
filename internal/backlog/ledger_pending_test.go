package backlog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type countingLedgerStates struct {
	*memoryLedgerStates
	loads int
}

func (s *countingLedgerStates) LoadLedgerStates(ctx context.Context) ([]domain.LedgerState, error) {
	s.loads++
	return s.memoryLedgerStates.LoadLedgerStates(ctx)
}

func TestLedgerPendingGate(t *testing.T) {
	gateErr := errors.New("eligibility unavailable")
	for _, tc := range []struct {
		name      string
		gate      bool
		pending   bool
		err       error
		wantLoads int
	}{
		{name: "false", gate: true},
		{name: "true", gate: true, pending: true, wantLoads: 1},
		{name: "error", gate: true, err: gateErr},
		{name: "nil", wantLoads: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := newLedgerWorld(true)
			reconciler := world.reconciler()
			states := &countingLedgerStates{memoryLedgerStates: world.states}
			reconciler.States = states
			recordsCalls, pendingCalls := 0, 0
			reconciler.Records = func(context.Context) (sqlite.CoordinatorRecords, error) {
				recordsCalls++
				return world.records, nil
			}
			if tc.gate {
				reconciler.Pending = func(_ context.Context, now time.Time) (bool, error) {
					pendingCalls++
					if !now.Equal(world.now) {
						t.Fatalf("gate time = %v, want %v", now, world.now)
					}
					return tc.pending, tc.err
				}
			}
			report := reconciler.Tick(context.Background())
			if recordsCalls != tc.wantLoads || states.loads != tc.wantLoads {
				t.Fatalf("records loads = %d, state loads = %d; want %d each", recordsCalls, states.loads, tc.wantLoads)
			}
			if tc.gate && pendingCalls != 1 {
				t.Fatalf("pending calls = %d, want 1", pendingCalls)
			}
			if tc.err != nil {
				if len(report.Errors) != 1 || !errors.Is(report.Errors[0], gateErr) {
					t.Fatalf("errors = %v, want gate error", report.Errors)
				}
			} else if len(report.Errors) != 0 {
				t.Fatalf("unexpected errors: %v", report.Errors)
			}
			if tc.wantLoads == 0 {
				if len(report.Applied) != 0 || len(report.Behind) != 0 || report.Busy || world.jocasta.callCount() != 0 || states.saves != 0 {
					t.Fatalf("gate performed reconciliation: %#v", report)
				}
			} else if len(report.Applied) != 1 || report.Applied[0] != "run-1 open" {
				t.Fatalf("enabled reconciliation = %#v", report)
			}
		})
	}
}
