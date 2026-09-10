package domain

import (
	"strings"
	"testing"
	"time"
)

func TestPrepareScheduleTemplateDefaultsAndVersions(t *testing.T) {
	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	first, err := PrepareScheduleTemplate(nil, ScheduleTemplate{
		ScheduleID: "schedule-1",
		Version:    1,
		WorkflowID: "workflow-1",
		Expression: "0 2 * * *",
		Timezone:   "America/Los_Angeles",
		CreatedAt:  now,
	})
	if err != nil {
		t.Fatalf("prepare first version: %v", err)
	}
	if first.Overlap != ScheduleOverlapForbid || first.Misfire != ScheduleMisfireSkip ||
		first.AfterFailure != ScheduleFailureNextCycle {
		t.Fatalf("defaults = %q, %q, %q", first.Overlap, first.Misfire, first.AfterFailure)
	}

	next := first
	next.Version = 2
	next.WorkflowID = "workflow-2"
	next.CreatedAt = now.Add(time.Hour)
	if _, err := PrepareScheduleTemplate(&first, next); err != nil {
		t.Fatalf("prepare next version: %v", err)
	}
	if first.Version != 1 || first.WorkflowID != "workflow-1" {
		t.Fatalf("previous version was mutated: %#v", first)
	}
}

func TestPrepareScheduleTemplateRejectsInvalidDefinitions(t *testing.T) {
	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	valid := ScheduleTemplate{
		ScheduleID:   "schedule-1",
		Version:      1,
		WorkflowID:   "workflow-1",
		Expression:   "0 2 * * *",
		Timezone:     "UTC",
		Overlap:      ScheduleOverlapForbid,
		Misfire:      ScheduleMisfireSkip,
		AfterFailure: ScheduleFailureHold,
		CreatedAt:    now,
	}
	cases := []struct {
		name string
		edit func(*ScheduleTemplate)
		want string
	}{
		{name: "missing schedule", edit: func(v *ScheduleTemplate) { v.ScheduleID = "" }, want: "schedule ID"},
		{name: "missing workflow", edit: func(v *ScheduleTemplate) { v.WorkflowID = "" }, want: "workflow ID"},
		{name: "missing expression", edit: func(v *ScheduleTemplate) { v.Expression = "" }, want: "expression"},
		{name: "invalid timezone", edit: func(v *ScheduleTemplate) { v.Timezone = "Mars/Olympus" }, want: "timezone"},
		{name: "overlap", edit: func(v *ScheduleTemplate) { v.Overlap = "allow" }, want: "overlap"},
		{name: "misfire", edit: func(v *ScheduleTemplate) { v.Misfire = "catch-up" }, want: "misfire"},
		{name: "failure", edit: func(v *ScheduleTemplate) { v.AfterFailure = "retry-now" }, want: "after-failure"},
		{name: "creation time", edit: func(v *ScheduleTemplate) { v.CreatedAt = time.Time{} }, want: "creation time"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidate := valid
			tc.edit(&candidate)
			if _, err := PrepareScheduleTemplate(nil, candidate); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestPrepareScheduleTemplateRejectsVersionHistoryBreaks(t *testing.T) {
	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	first := ScheduleTemplate{
		ScheduleID: "schedule-1", Version: 1, WorkflowID: "workflow-1",
		Expression: "0 2 * * *", Timezone: "UTC",
		Overlap: ScheduleOverlapForbid, Misfire: ScheduleMisfireSkip,
		AfterFailure: ScheduleFailureNextCycle, CreatedAt: now,
	}
	cases := []struct {
		name     string
		previous *ScheduleTemplate
		next     ScheduleTemplate
		want     string
	}{
		{name: "first version", next: func() ScheduleTemplate { v := first; v.Version = 2; return v }(), want: "must be 1"},
		{name: "skipped version", previous: &first, next: func() ScheduleTemplate { v := first; v.Version = 3; return v }(), want: "advance"},
		{name: "moved schedule", previous: &first, next: func() ScheduleTemplate { v := first; v.ScheduleID = "schedule-2"; v.Version = 2; return v }(), want: "cannot move"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := PrepareScheduleTemplate(tc.previous, tc.next); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want substring %q", err, tc.want)
			}
		})
	}
}
