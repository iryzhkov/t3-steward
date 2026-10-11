package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestCoordinatorBoundaryPrunesOneBatchPerTickDespiteQuotaFailure(t *testing.T) {
	t.Parallel()
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "prune succeeds", true: "prune fails"}[fail], func(t *testing.T) {
			var quotaCalls, scheduleCalls, pruneCalls int
			var logs bytes.Buffer
			retention := 48 * time.Hour
			cycle := coordinatorBoundaryCycle{
				quota:          failingCoordinatorQuotaTicker{calls: &quotaCalls},
				schedules:      recordingCoordinatorScheduleTicker{calls: &scheduleCalls},
				logger:         slog.New(slog.NewTextHandler(&logs, nil)),
				auditRetention: retention,
				pruneAudit: func(ctx context.Context, cutoff time.Time) (int, error) {
					pruneCalls++
					now := time.Now().UTC()
					if age := now.Sub(cutoff); age < retention || age > retention+time.Second {
						t.Fatalf("cutoff age %v", age)
					}
					if fail {
						return 0, errors.New("injected prune failure")
					}
					return 1000, nil
				},
			}
			cycle.Tick(context.Background())
			if pruneCalls != 1 || quotaCalls != 1 || scheduleCalls != 1 {
				t.Fatalf("first tick prune=%d quota=%d schedules=%d", pruneCalls, quotaCalls, scheduleCalls)
			}
			cycle.Tick(context.Background())
			if pruneCalls != 2 || quotaCalls != 2 || scheduleCalls != 2 {
				t.Fatalf("second tick prune=%d quota=%d schedules=%d", pruneCalls, quotaCalls, scheduleCalls)
			}
			if fail && !strings.Contains(logs.String(), "coordinator audit retention failed") {
				t.Fatalf("missing prune failure: %s", logs.String())
			}
		})
	}
}
