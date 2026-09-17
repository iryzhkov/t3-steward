package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
)

// cancelledCoordinatorQuotaTicker fails the way every tick does once the
// coordinator's context is cancelled: with the context's own error.
type cancelledCoordinatorQuotaTicker struct{}

func (cancelledCoordinatorQuotaTicker) Tick(ctx context.Context) (backlog.QuotaBridgeReport, error) {
	return backlog.QuotaBridgeReport{}, ctx.Err()
}

// wrappedCancelCoordinatorScheduleTicker fails with the cancellation wrapped
// the way a store call wraps it, which is what the real ticks return.
type wrappedCancelCoordinatorScheduleTicker struct{}

func (wrappedCancelCoordinatorScheduleTicker) Tick(ctx context.Context) (backlog.ScheduleTickReport, error) {
	return backlog.ScheduleTickReport{}, errors.New("load coordinator epoch: " + ctx.Err().Error())
}

type cancelledCoordinatorLegacyTicker struct{}

func (cancelledCoordinatorLegacyTicker) Tick(ctx context.Context) backlog.LegacySubmissionReport {
	return backlog.LegacySubmissionReport{Errors: []error{ctx.Err()}}
}

// A tick that fails because the coordinator is shutting down is not an
// operational error. Restarting an idle coordinator used to log a burst of
// ERROR lines, every one of them "context canceled", which is noise that hides
// the errors an operator has to read.
func TestCoordinatorBoundaryCycleLogsCancelledTicksAsShutdown(t *testing.T) {
	var logs bytes.Buffer
	var planningCalls, adminCalls, workerCalls int
	var workerReports []backlog.QuotaBridgeReport
	cycle := coordinatorBoundaryCycle{
		quota:     cancelledCoordinatorQuotaTicker{},
		schedules: wrappedCancelCoordinatorScheduleTicker{},
		planning:  recordingCoordinatorPlanningTicker{calls: &planningCalls},
		admin:     recordingCoordinatorAdminExecutor{calls: &adminCalls},
		legacy:    cancelledCoordinatorLegacyTicker{},
		workers:   recordingCoordinatorWorkerTicker{calls: &workerCalls, reports: &workerReports},
		logger:    slog.New(slog.NewTextHandler(&logs, nil)),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cycle.TickWithWorkers(ctx)

	if strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("a cancelled tick was logged as an error:\n%s", logs.String())
	}
	for _, want := range []string{
		"shutting down",
		"quota reconciliation",
		"schedule reconciliation",
		"legacy backlog-v2 submission",
	} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("logs do not mention %q:\n%s", want, logs.String())
		}
	}
	infoLines := 0
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if strings.Contains(line, "shutting down") && !strings.Contains(line, "level=INFO") {
			t.Fatalf("shutdown line is not INFO: %s", line)
		}
		if strings.Contains(line, "shutting down") {
			infoLines++
		}
	}
	if infoLines < 3 {
		t.Fatalf("expected quota, schedule and legacy shutdown lines, got %d:\n%s", infoLines, logs.String())
	}
}

// The same failures with a live context are still errors: only a cancellation
// is read as shutdown, so a real failure keeps its severity.
func TestCoordinatorBoundaryCycleKeepsRealTickFailuresAsErrors(t *testing.T) {
	var logs bytes.Buffer
	var quotaCalls, scheduleCalls, planningCalls, adminCalls, legacyCalls int
	cycle := coordinatorBoundaryCycle{
		quota:     failingCoordinatorQuotaTicker{calls: &quotaCalls},
		schedules: recordingCoordinatorScheduleTicker{calls: &scheduleCalls},
		planning:  recordingCoordinatorPlanningTicker{calls: &planningCalls},
		admin:     recordingCoordinatorAdminExecutor{calls: &adminCalls},
		legacy:    recordingCoordinatorLegacyTicker{calls: &legacyCalls},
		logger:    slog.New(slog.NewTextHandler(&logs, nil)),
	}
	cycle.Tick(context.Background())
	if !strings.Contains(logs.String(), "level=ERROR") || !strings.Contains(logs.String(), "quota unavailable") {
		t.Fatalf("a real failure was not logged as an error:\n%s", logs.String())
	}
	if strings.Contains(logs.String(), "shutting down") {
		t.Fatalf("a real failure was read as shutdown:\n%s", logs.String())
	}
}
