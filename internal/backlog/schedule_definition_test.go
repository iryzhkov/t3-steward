package backlog

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestScheduleDefinitionAdministrationRevisionReplayAndTimer(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 9, 10, 16, 0, 0, 0, time.UTC)
	workflow := domain.Workflow{
		ID: "workflow-scheduled", Version: ManifestVersion, Name: "scheduled",
		Class: domain.TaskClassRequired, CreatedAt: now,
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{workflow},
	}); err != nil {
		t.Fatal(err)
	}
	service := &ScheduleDefinitionService{Store: store, Now: func() time.Time { return now }}
	request := ScheduleDefinitionRequest{
		RequestID: "schedule-put-1", ID: "schedule-1", Name: "hourly",
		WorkflowID: workflow.ID, Expression: "0 * * * *", Timezone: "UTC",
		Enabled: true, ExpectedRevision: 0, Actor: "operator-1", Reason: "create schedule",
	}
	created, err := service.Put(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if created.Replay || created.Schedule.Revision != 1 || created.Schedule.Version != 1 ||
		created.Schedule.Misfire != domain.ScheduleMisfireSkip ||
		created.Schedule.Overlap != domain.ScheduleOverlapForbid ||
		created.Schedule.AfterFailure != domain.ScheduleFailureNextCycle {
		t.Fatalf("created schedule = %#v", created)
	}
	replayed, err := service.Put(ctx, request)
	if err != nil || !replayed.Replay || replayed.Schedule.Revision != 1 {
		t.Fatalf("replay = %#v, err %v", replayed, err)
	}
	changedReplay := request
	changedReplay.Expression = "30 * * * *"
	if _, err := service.Put(ctx, changedReplay); err == nil || !strings.Contains(err.Error(), "different content") {
		t.Fatalf("changed replay error = %v", err)
	}

	now = now.Add(time.Minute)
	update := request
	update.RequestID = "schedule-put-2"
	update.Expression = "30 * * * *"
	update.ExpectedRevision = 1
	update.Reason = "move to half hour"
	updated, err := service.Put(ctx, update)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Schedule.Version != 2 || updated.Schedule.Revision != 2 ||
		updated.Schedule.CreatedAt != created.Schedule.CreatedAt {
		t.Fatalf("updated schedule = %#v", updated.Schedule)
	}

	timerNow := time.Date(2026, 9, 10, 16, 30, 0, 0, time.UTC)
	timer := ScheduleTimer{Store: store, CatchUpMax: 4, Now: func() time.Time { return timerNow }}
	report, err := timer.Tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Results) != 1 || report.Results[0].Trigger.State != domain.TriggerAccepted ||
		report.Results[0].Trigger.ScheduleVersion != 2 {
		t.Fatalf("timer after definition update = %#v", report)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.ScheduleTemplates) != 2 || len(records.AuditEvents) != 3 {
		t.Fatalf("schedule history = %d templates, %d events", len(records.ScheduleTemplates), len(records.AuditEvents))
	}
}

func TestScheduleDefinitionAdministrationRejectsInvalidAndStaleRequests(t *testing.T) {
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 9, 10, 17, 0, 0, 0, time.UTC)
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: "workflow-1", Version: ManifestVersion, Name: "workflow",
			Class: domain.TaskClassRequired, CreatedAt: now,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	service := &ScheduleDefinitionService{Store: store, Now: func() time.Time { return now }}
	valid := ScheduleDefinitionRequest{
		RequestID: "put-1", ID: "schedule-1", Name: "daily",
		WorkflowID: "workflow-1", Expression: "0 9 * * *", Timezone: "America/Los_Angeles",
		Enabled: true, Actor: "operator", Reason: "daily run",
	}
	if _, err := service.Put(ctx, valid); err != nil {
		t.Fatal(err)
	}
	stale := valid
	stale.RequestID = "put-stale"
	stale.ExpectedRevision = 9
	if _, err := service.Put(ctx, stale); err == nil || !strings.Contains(err.Error(), "revision is 1") {
		t.Fatalf("stale error = %v", err)
	}
	invalid := valid
	invalid.RequestID = "put-invalid"
	invalid.ID = "schedule-invalid"
	invalid.Expression = "0 25 * * *"
	if _, err := service.Put(ctx, invalid); err == nil || !strings.Contains(err.Error(), "hour field") {
		t.Fatalf("invalid expression error = %v", err)
	}
	missing := valid
	missing.RequestID = "put-missing"
	missing.ID = "schedule-missing"
	missing.WorkflowID = "missing"
	if _, err := service.Put(ctx, missing); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("missing workflow error = %v", err)
	}
}
