package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

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
