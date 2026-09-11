package backlog

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestParseScheduleExpressionSyntaxAndMatching(t *testing.T) {
	expression, err := ParseScheduleExpression("*/15 9-17 * 1,6 1-5")
	if err != nil {
		t.Fatal(err)
	}
	location := time.FixedZone("test", -8*60*60)
	after := time.Date(2026, 1, 5, 16, 59, 0, 0, time.UTC)
	got := expression.occurrences(after, after.Add(30*time.Minute), location)
	if len(got) != 2 ||
		got[0] != time.Date(2026, 1, 5, 17, 0, 0, 0, time.UTC) ||
		got[1] != time.Date(2026, 1, 5, 17, 15, 0, 0, time.UTC) {
		t.Fatalf("occurrences = %v", got)
	}

	for _, invalid := range []string{
		"", "* * * *", "60 * * * *", "* 24 * * *",
		"* * 0 * *", "* * * 13 *", "* * * * 8",
		"*/0 * * * *", "10-5 * * * *", "a * * * *",
	} {
		if _, err := ParseScheduleExpression(invalid); err == nil {
			t.Errorf("ParseScheduleExpression(%q) succeeded", invalid)
		}
	}
}

func TestScheduleExpressionDSTGapAndFold(t *testing.T) {
	location, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatal(err)
	}
	gap, err := ParseScheduleExpression("30 2 * * *")
	if err != nil {
		t.Fatal(err)
	}
	gapStart := time.Date(2026, 3, 8, 8, 0, 0, 0, time.UTC)
	if got := gap.occurrences(gapStart, gapStart.Add(5*time.Hour), location); len(got) != 0 {
		t.Fatalf("spring-forward gap occurrences = %v", got)
	}

	fold, err := ParseScheduleExpression("30 1 * * *")
	if err != nil {
		t.Fatal(err)
	}
	foldStart := time.Date(2026, 11, 1, 7, 0, 0, 0, time.UTC)
	got := fold.occurrences(foldStart, foldStart.Add(5*time.Hour), location)
	want := []time.Time{
		time.Date(2026, 11, 1, 8, 30, 0, 0, time.UTC),
		time.Date(2026, 11, 1, 9, 30, 0, 0, time.UTC),
	}
	if len(got) != len(want) || !got[0].Equal(want[0]) || !got[1].Equal(want[1]) {
		t.Fatalf("fall-back fold occurrences = %v, want %v", got, want)
	}
}

func TestScheduleTimerCatchUpOverlapAndRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := sqlite.OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	created := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	schedule := domain.Schedule{
		ID: "schedule-hourly", Name: "hourly", Version: 1, WorkflowID: "workflow-1",
		Expression: "0 * * * *", Timezone: "UTC", Overlap: domain.ScheduleOverlapForbid,
		Misfire: domain.ScheduleMisfireSkip, AfterFailure: domain.ScheduleFailureNextCycle,
		Enabled: true, Revision: 1, CreatedAt: created, UpdatedAt: created,
	}
	template := domain.ScheduleTemplate{
		ScheduleID: schedule.ID, Version: 1, WorkflowID: schedule.WorkflowID,
		Expression: schedule.Expression, Timezone: schedule.Timezone,
		Overlap: schedule.Overlap, Misfire: schedule.Misfire,
		AfterFailure: schedule.AfterFailure, CreatedAt: created,
	}
	workflow := domain.Workflow{
		ID: "workflow-1", Version: ManifestVersion, Name: "scheduled",
		Class: domain.TaskClassRequired, CreatedAt: created,
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{workflow}, Schedules: []domain.Schedule{schedule},
		ScheduleTemplates: []domain.ScheduleTemplate{template},
	}); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 9, 10, 3, 0, 30, 0, time.UTC)
	timer := ScheduleTimer{Store: store, CatchUpMax: 2, Now: func() time.Time { return now }}
	report, err := timer.Tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Results) != 2 {
		t.Fatalf("catch-up results = %d, want 2", len(report.Results))
	}
	if got := report.Results[0].Trigger.Reason; got != "misfire-skipped" {
		t.Fatalf("old catch-up reason = %q", got)
	}
	if report.Results[1].Trigger.State != domain.TriggerAccepted || report.Results[1].WorkflowRun == nil {
		t.Fatalf("current trigger = %#v", report.Results[1])
	}

	restarted := ScheduleTimer{Store: store, CatchUpMax: 2, Now: func() time.Time { return now }}
	replay, err := restarted.Tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(replay.Results) != 0 {
		t.Fatalf("restart replay created %d decisions", len(replay.Results))
	}

	now = now.Add(time.Hour)
	overlap, err := restarted.Tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(overlap.Results) != 1 ||
		overlap.Results[0].Trigger.State != domain.TriggerSuppressed ||
		overlap.Results[0].Trigger.Reason != "overlap-forbidden" {
		t.Fatalf("overlap result = %#v", overlap.Results)
	}

	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Triggers) != 3 || len(records.WorkflowRuns) != 1 {
		t.Fatalf("durable state has %d triggers and %d runs", len(records.Triggers), len(records.WorkflowRuns))
	}
}

func TestDeterministicScheduleTriggerUsesNominalUTCIdentity(t *testing.T) {
	observed := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	first := deterministicScheduleTrigger("schedule-1", time.Date(2026, 11, 1, 1, 30, 0, 0, time.FixedZone("PDT", -7*60*60)), observed)
	second := deterministicScheduleTrigger("schedule-1", first.NominalAt, observed.Add(time.Minute))
	if first.TriggerID != second.TriggerID || first.WorkflowRunID != second.WorkflowRunID {
		t.Fatalf("identities changed: %#v %#v", first, second)
	}
	folded := deterministicScheduleTrigger("schedule-1", time.Date(2026, 11, 1, 1, 30, 0, 0, time.FixedZone("PST", -8*60*60)), observed)
	if folded.TriggerID == first.TriggerID {
		t.Fatal("DST fold occurrences shared an identity")
	}
}
