package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestNativeWaitRoutingNeverOpensShellCheck(t *testing.T) {
	for _, args := range [][]string{{"add", "--task", "r/t"}, {"add", "--run=r"}, {"list", "--native"}, {"cancel", "nw-123"}, {"run-now", "nw-123"}} {
		if !nativeWaitArgs(args) {
			t.Fatalf("native command routed to shell/store: %v", args)
		}
	}
	if nativeWaitArgs([]string{"add", "--name", "test", "--", "true"}) {
		t.Fatal("shell command routed as native")
	}
}
func TestCoordinatorSettlesNodeWaitDespiteQuotaFailure(t *testing.T) {
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC()
	run, err := domain.BindRunSink(domain.WorkflowRun{ID: "r", WorkflowID: "w", Progress: domain.ProgressBlocked, Revision: 1, CreatedAt: now, UpdatedAt: now}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RegisterNodeWait(context.Background(), domain.NodeWaitRequest{ID: "nw-test", ThreadID: "thread", Target: domain.NodeRef{RunID: "r", TaskID: domain.SinkTaskName}, Timeout: time.Hour}, "test", "host", now); err != nil {
		t.Fatal(err)
	}
	quota, schedules, planning, admin, legacy := 0, 0, 0, 0, 0
	cycle := coordinatorBoundaryCycle{projection: store, quota: failingCoordinatorQuotaTicker{calls: &quota}, schedules: recordingCoordinatorScheduleTicker{calls: &schedules}, planning: recordingCoordinatorPlanningTicker{calls: &planning}, admin: recordingCoordinatorAdminExecutor{calls: &admin}, legacy: recordingCoordinatorLegacyTicker{calls: &legacy}, logger: newLogger("error")}
	cycle.Tick(context.Background())
	waits, err := store.ListNodeWaits(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if waits[0].Observation == nil || waits[0].Observation.ExitCode != 0 || planning != 0 {
		t.Fatalf("waits=%+v planning=%d", waits, planning)
	}
}
