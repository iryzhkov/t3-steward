package backlogadmin

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
)

// automaticRetryFixture is a run of two tasks, build and ship (which needs
// build), whose first build attempt failed with reason a minute ago.
func automaticRetryFixture(t *testing.T, reason string) (*Service, *sqlite.Store) {
	t.Helper()
	store, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := adminTestNow
	failedAt := now.Add(-time.Minute)
	records := sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{ID: "workflow-1", Version: 2, Name: "workflow", Class: domain.TaskClassRequired, TaskIDs: []string{"task-build", "task-ship"}}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "workflow-1", Progress: domain.ProgressActive, Revision: 1,
			CreatedAt: now.Add(-time.Hour), UpdatedAt: failedAt}},
		Tasks: []domain.Task{
			{ID: "task-build", WorkflowID: "workflow-1", Name: "build", Class: domain.TaskClassRequired, MaxTurns: 1},
			{ID: "task-ship", WorkflowID: "workflow-1", Name: "ship", Class: domain.TaskClassRequired, MaxTurns: 1, Needs: []string{"build"}},
		},
		Attempts: []domain.Attempt{
			{ID: "attempt-build-1", WorkflowRunID: "run-1", TaskID: "task-build", Number: 1, Progress: domain.ProgressFailed,
				Control: domain.ControlStopped, Revision: 5, Failure: reason, UpdatedAt: failedAt, CompletedAt: &failedAt},
			{ID: "attempt-ship-1", WorkflowRunID: "run-1", TaskID: "task-ship", Number: 1, Progress: domain.ProgressBlocked,
				Control: domain.ControlUnassigned, Revision: 1, UpdatedAt: failedAt},
		},
	}
	if err := store.SaveCoordinatorRecords(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now })
	return service, store
}

func loadAttempt(t *testing.T, records sqlite.CoordinatorRecords, id string) domain.Attempt {
	t.Helper()
	for _, attempt := range records.Attempts {
		if attempt.ID == id {
			return attempt
		}
	}
	t.Fatalf("attempt %s not found", id)
	return domain.Attempt{}
}

func TestAutomaticRetryOfInfrastructureFailureIsAuditedDelayedAndHoldsTheRun(t *testing.T) {
	ctx := context.Background()
	service, store := automaticRetryFixture(t, "provider turn did not complete successfully: overloaded")

	// Before the pass runs, the projection already holds the run: settling it
	// now would skip ship and fail the run before the retry exists.
	if _, err := backlog.ProjectWorkflowRunsWithRetries(ctx, store, adminTestNow, 3); err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ship := loadAttempt(t, records, "attempt-ship-1"); ship.Progress != domain.ProgressBlocked {
		t.Fatalf("ship = %s before the retry", ship.Progress)
	}

	report, err := service.SubmitAutomaticRetries(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if report.Classified != 1 || len(report.Submitted) != 1 || report.Submitted[0].Command.State != domain.AdminCommandPending ||
		report.Submitted[0].Command.RequestedBy != domain.AutomaticRetryRequestedBy {
		t.Fatalf("report = %+v", report)
	}
	records, _ = store.LoadCoordinatorRecords(ctx)
	failed := loadAttempt(t, records, "attempt-build-1")
	if failed.FailureClass != domain.FailureInfrastructure || failed.FailureReason != domain.ReasonProviderTurnFailed || failed.Revision != 5 {
		t.Fatalf("classification not recorded, or it moved the revision: %+v", failed)
	}
	if event, found, err := store.LoadAuditEvent(ctx, report.Submitted[0].Event.ID); err != nil || !found ||
		event.Actor != domain.AutomaticRetryRequestedBy || event.AttemptID != "attempt-build-1" {
		t.Fatalf("submission receipt = %+v, %t, %v", event, found, err)
	}
	// A second pass submits nothing new: the command ID is stable.
	if again, err := service.SubmitAutomaticRetries(ctx, 3); err != nil || len(again.Submitted) != 0 || again.Classified != 0 {
		t.Fatalf("second pass = %+v, %v", again, err)
	}

	execution, err := service.ExecutePendingCommands(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(execution.Decisions) != 1 || execution.Decisions[0].Command.State != domain.AdminCommandApplied {
		t.Fatalf("execution = %+v", execution)
	}
	records, _ = store.LoadCoordinatorRecords(ctx)
	var retry domain.Attempt
	for _, attempt := range records.Attempts {
		if attempt.TaskID == "task-build" && attempt.Number == 2 {
			retry = attempt
		}
	}
	wantNotBefore := adminTestNow.Add(-time.Minute + domain.DefaultRetryBackoff)
	if retry.ID == "" || retry.Progress != domain.ProgressReady || retry.Control != domain.ControlUnassigned ||
		retry.AutomaticRetry == nil || retry.AutomaticRetry.SourceAttemptID != "attempt-build-1" ||
		retry.AutomaticRetry.Ordinal != 1 || retry.AutomaticRetry.Budget != 2 ||
		retry.AdminNotBefore == nil || !retry.AdminNotBefore.Equal(wantNotBefore) {
		t.Fatalf("retry attempt = %+v", retry)
	}

	// With the retry in place the run projects normally and stays open.
	if _, err := backlog.ProjectWorkflowRunsWithRetries(ctx, store, adminTestNow, 3); err != nil {
		t.Fatal(err)
	}
	records, _ = store.LoadCoordinatorRecords(ctx)
	if ship := loadAttempt(t, records, "attempt-ship-1"); ship.Progress != domain.ProgressBlocked {
		t.Fatalf("ship = %s after the retry was created", ship.Progress)
	}
	if run := records.WorkflowRuns[0]; run.Progress.Terminal() || run.Sink != nil && run.Sink.Progress.Terminal() {
		t.Fatalf("run settled with a retry queued: %+v", run)
	}

	// The retry fails on code: no further retry, and the run settles.
	failedAt := adminTestNow.Add(time.Hour)
	retry.Progress, retry.Control, retry.Failure = domain.ProgressFailed, domain.ControlStopped, "verification command failed (2): make test"
	retry.Revision++
	retry.CompletedAt = &failedAt
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{retry}}); err != nil {
		t.Fatal(err)
	}
	if after, err := service.SubmitAutomaticRetries(ctx, 3); err != nil || len(after.Submitted) != 0 || after.Classified != 1 {
		t.Fatalf("code failure pass = %+v, %v", after, err)
	}
	if _, err := backlog.ProjectWorkflowRunsWithRetries(ctx, store, failedAt, 3); err != nil {
		t.Fatal(err)
	}
	records, _ = store.LoadCoordinatorRecords(ctx)
	if got := loadAttempt(t, records, retry.ID); got.FailureClass != domain.FailureCode {
		t.Fatalf("code failure classified as %s", got.FailureClass)
	}
	if ship := loadAttempt(t, records, "attempt-ship-1"); ship.Progress != domain.ProgressSkipped {
		t.Fatalf("ship = %s after a code failure", ship.Progress)
	}
	if run := records.WorkflowRuns[0]; run.Progress != domain.ProgressFailed {
		t.Fatalf("run = %s after a code failure", run.Progress)
	}
}

func TestNoAutomaticRetryForPolicyOrCodeFailures(t *testing.T) {
	for _, reason := range []string{
		"verification command failed (1): go test ./...",
		"permanent collection secret failure: result secret scan refused results/x",
	} {
		t.Run(reason, func(t *testing.T) {
			ctx := context.Background()
			service, store := automaticRetryFixture(t, reason)
			report, err := service.SubmitAutomaticRetries(ctx, 3)
			if err != nil || len(report.Submitted) != 0 || report.Classified != 1 {
				t.Fatalf("report = %+v, %v", report, err)
			}
			if _, err := backlog.ProjectWorkflowRunsWithRetries(ctx, store, adminTestNow, 3); err != nil {
				t.Fatal(err)
			}
			records, _ := store.LoadCoordinatorRecords(ctx)
			if len(records.Attempts) != 2 || records.WorkflowRuns[0].Progress != domain.ProgressFailed {
				t.Fatalf("run = %s with %d attempts", records.WorkflowRuns[0].Progress, len(records.Attempts))
			}
		})
	}
}

func TestRejectedAutomaticRetryReleasesTheRun(t *testing.T) {
	ctx := context.Background()
	service, store := automaticRetryFixture(t, "T3 thread creation failed: dial unix: connection refused")
	report, err := service.SubmitAutomaticRetries(ctx, 3)
	if err != nil || len(report.Submitted) != 1 {
		t.Fatalf("report = %+v, %v", report, err)
	}
	// The attempt moves on before the command applies, so the command's
	// revision fence rejects it.
	records, _ := store.LoadCoordinatorRecords(ctx)
	failed := loadAttempt(t, records, "attempt-build-1")
	failed.Revision++
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{failed}}); err != nil {
		t.Fatal(err)
	}
	execution, err := service.ExecutePendingCommands(ctx)
	if err != nil || len(execution.Decisions) != 1 || execution.Decisions[0].Command.State != domain.AdminCommandRejected {
		t.Fatalf("execution = %+v, %v", execution, err)
	}
	if again, err := service.SubmitAutomaticRetries(ctx, 3); err != nil || len(again.Submitted) != 0 {
		t.Fatalf("resubmitted after rejection = %+v, %v", again, err)
	}
	if _, err := backlog.ProjectWorkflowRunsWithRetries(ctx, store, adminTestNow, 3); err != nil {
		t.Fatal(err)
	}
	records, _ = store.LoadCoordinatorRecords(ctx)
	if records.WorkflowRuns[0].Progress != domain.ProgressFailed {
		t.Fatalf("a rejected retry kept the run open: %s", records.WorkflowRuns[0].Progress)
	}
}
