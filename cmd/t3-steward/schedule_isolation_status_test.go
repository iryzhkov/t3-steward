package main

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/config"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// TestRunningCoordinatorReportsAScheduleThatCannotFire drives the real
// reconciliation loop rather than the service setter underneath it.
//
// The signal only exists because the boundary cycle is handed the admin service
// to report into. A unit test that calls SetScheduleIssues itself would pass
// with that wiring deleted, which is the same shape of defect as the one this
// work was written to fix: the line nobody tests is the line that quietly stops
// working. This test fails if it is removed.
func TestRunningCoordinatorReportsAScheduleThatCannotFire(t *testing.T) {
	cfg := config.Default()
	setCoordinatorTestRoots(t, &cfg)
	cfg.BacklogV2.Mode = "coordinator"
	cfg.BacklogV2.Coordinator.ID = "normandy"
	cfg.BacklogV2.StartupAdmission = "closed"
	cfg.BacklogV2.Scheduling.Interval = config.Duration(10 * time.Millisecond)

	// A workflow that declares a task whose definition is not stored cannot be
	// turned into a run, so every occurrence of a schedule over it fails.
	created := time.Now().UTC().Truncate(time.Minute).Add(-time.Minute)
	seed, err := sqlite.OpenMigrated(cfg.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.SaveCoordinatorRecords(context.Background(), sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: "workflow-broken", Version: backlog.ManifestVersion, Name: "broken",
			Class: domain.TaskClassRequired, CreatedAt: created,
			TaskIDs: []string{"task-that-was-removed"},
		}},
		Schedules: []domain.Schedule{{
			ID: "schedule-broken", Name: "broken", Version: 1, WorkflowID: "workflow-broken",
			Expression: "* * * * *", Timezone: "UTC", Overlap: domain.ScheduleOverlapForbid,
			Misfire: domain.ScheduleMisfireSkip, AfterFailure: domain.ScheduleFailureNextCycle,
			Enabled: true, Revision: 1, CreatedAt: created, UpdatedAt: created,
		}},
		ScheduleTemplates: []domain.ScheduleTemplate{{
			ScheduleID: "schedule-broken", Version: 1, WorkflowID: "workflow-broken",
			Expression: "* * * * *", Timezone: "UTC", Overlap: domain.ScheduleOverlapForbid,
			Misfire: domain.ScheduleMisfireSkip, AfterFailure: domain.ScheduleFailureNextCycle,
			CreatedAt: created,
		}},
	}); err != nil {
		seed.Close()
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, runErr := runBacklogV2(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
		done <- runErr
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case runErr := <-done:
			if runErr != nil {
				t.Errorf("coordinator stopped with %v", runErr)
			}
		case <-time.After(10 * time.Second):
			t.Error("coordinator did not stop")
		}
	})
	socketPath, err := resolveBacklogV2AdminSocketPath(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client := backlogadmin.LocalClient{
		Path: socketPath, MaxResponseBytes: int64(cfg.BacklogV2.MessageLimits.MaxBytes),
		MaxArtifactBytes: int64(cfg.BacklogV2.MessageLimits.MaxArtifactBytes),
		RequestTimeout:   cfg.BacklogV2.Transport.RequestTimeout.D(),
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		response, queryErr := client.Query(context.Background(), backlogadmin.Query{
			Version: backlogadmin.Version, Kind: backlogadmin.QueryStatus,
		})
		if queryErr == nil && response.Status != nil {
			for _, issue := range response.Status.Runtime.ReconciliationIssues {
				if strings.Contains(issue, "schedule:schedule-broken:unschedulable") {
					if response.Status.Runtime.Health != "degraded" {
						t.Fatalf("health = %q while a schedule cannot fire", response.Status.Runtime.Health)
					}
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("a running coordinator never reported the schedule it could not fire; " +
				"the reconciliation loop is not wired to the status view")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestAScheduleThatCannotFireIsReportedInStatus is the second half of schedule
// isolation. Making a failing schedule fail alone means nothing else breaks to
// make an operator look, so the coordinator has to say so itself: the schedule
// is named in the reconciliation issues and the runtime reports degraded, the
// same treatment a project the catalog cannot hold already gets.
func TestAScheduleThatCannotFireIsReportedInStatus(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service, err := backlogadmin.New(store, localAdminAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return probeNow })
	service.SetRuntimeInfo(backlogadmin.RuntimeInfo{
		Epoch: 1, Mode: "coordinator", Owner: "coordinator", MaxWorkerSnapshotAge: time.Minute,
	})
	principal := backlogadmin.Principal{ID: "local:1000", Roles: []string{backlogadmin.LocalAdminRole}}
	status := func() *backlogadmin.Status {
		t.Helper()
		response, queryErr := service.Query(ctx, backlogadmin.Query{
			Version: backlogadmin.Version, Kind: backlogadmin.QueryStatus, Principal: principal,
		})
		if queryErr != nil {
			t.Fatal(queryErr)
		}
		if response.Status == nil {
			t.Fatal("no status")
		}
		return response.Status
	}

	// The quiet case first, so the surface cannot become noise an operator
	// learns to ignore.
	if healthy := status(); len(healthy.Runtime.ReconciliationIssues) != 0 || healthy.Runtime.Health == "degraded" {
		t.Fatalf("a coordinator with no failing schedule reported %+v", healthy.Runtime)
	}

	service.SetScheduleIssues([]string{
		`schedule:nightly:unschedulable: occurrence 2026-09-10T02:00:00Z: seed scheduled run for "nightly": scheduled workflow "workflow-1" declares task "gone", which has no stored definition`,
	})
	degraded := status()
	found := false
	for _, issue := range degraded.Runtime.ReconciliationIssues {
		if strings.Contains(issue, "schedule:nightly:unschedulable") && strings.Contains(issue, "no stored definition") {
			found = true
		}
	}
	if !found {
		t.Fatalf("reconciliation issues = %v", degraded.Runtime.ReconciliationIssues)
	}
	if degraded.Runtime.Health != "degraded" {
		t.Fatalf("health = %q, want degraded while a schedule cannot fire", degraded.Runtime.Health)
	}

	// A later tick that finds nothing wrong clears the report rather than
	// leaving the coordinator degraded forever.
	service.SetScheduleIssues(nil)
	if recovered := status(); len(recovered.Runtime.ReconciliationIssues) != 0 || recovered.Runtime.Health == "degraded" {
		t.Fatalf("a repaired schedule left the coordinator degraded: %+v", recovered.Runtime)
	}
}
