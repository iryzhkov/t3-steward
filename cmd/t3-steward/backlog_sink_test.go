package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestSinkSettlesWhileQuotaReconciliationFails(t *testing.T) {
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC()
	run, err := domain.BindRunSink(domain.WorkflowRun{ID: "empty", WorkflowID: "w", Progress: domain.ProgressBlocked, Revision: 1, CreatedAt: now, UpdatedAt: now}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}}); err != nil {
		t.Fatal(err)
	}
	quota, schedules, planning, admin, legacy := 0, 0, 0, 0, 0
	cycle := coordinatorBoundaryCycle{
		projection: store, quota: failingCoordinatorQuotaTicker{calls: &quota},
		schedules: recordingCoordinatorScheduleTicker{calls: &schedules},
		planning:  recordingCoordinatorPlanningTicker{calls: &planning},
		admin:     recordingCoordinatorAdminExecutor{calls: &admin},
		legacy:    recordingCoordinatorLegacyTicker{calls: &legacy},
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	cycle.Tick(context.Background())
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if records.WorkflowRuns[0].Sink.Progress != domain.ProgressSucceeded || planning != 0 || admin != 0 || quota != 1 || len(records.Attempts) != 0 {
		t.Fatalf("records=%+v planning=%d admin=%d quota=%d", records, planning, admin, quota)
	}
}

func TestSinkCountFlagAndHumanResult(t *testing.T) {
	for _, args := range [][]string{{"status", "--include-sink"}, {"list", "--include-sink", "--json"}, {"show", "r", "--include-sink"}} {
		query, _, err := parseBacklogAdminQuery(args)
		if err != nil || !query.IncludeSink {
			t.Fatalf("args=%v query=%+v err=%v", args, query, err)
		}
	}
	for _, args := range [][]string{{"status", "--include-sink", "--include-sink"}, {"graph", "r", "--include-sink"}} {
		if _, _, err := parseBacklogAdminQuery(args); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
	var out bytes.Buffer
	renderTask(&out, &backlogadmin.TaskDetail{Task: domain.Task{ID: "sink:r", Name: domain.SinkTaskName}, Sink: &domain.SinkTask{GraphRevision: 1, Progress: domain.ProgressFailed, Result: &domain.SinkResult{FailedTaskIDs: []string{"a", "b"}}}})
	if !strings.Contains(out.String(), "failed task IDs: a, b") || strings.Contains(out.String(), "attempt:") {
		t.Fatalf("output=%s", out.String())
	}
}
