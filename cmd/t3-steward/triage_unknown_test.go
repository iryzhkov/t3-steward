package main

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestTriageUnknown(t *testing.T) {
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	for _, age := range []time.Duration{9 * time.Minute, 10 * time.Minute, 11 * time.Minute, -time.Minute, 0} {
		for _, threshold := range []time.Duration{10 * time.Minute, 12 * time.Minute} {
			origin := now.Add(-age)
			if age == 0 {
				origin = time.Time{}
			}
			a := &backlogadmin.Assignment{ID: "a", Epoch: 2, WorkerID: "disconnected", State: domain.AssignmentUnknown, UnknownSince: &origin}
			before := *a
			runs := []backlogadmin.WorkflowSummary{{Run: domain.WorkflowRun{ID: "terminal", Progress: domain.ProgressSucceeded}}}
			detail := &backlogadmin.WorkflowDetail{Tasks: []backlogadmin.TaskDetail{{Task: domain.Task{ID: "task"}, Assignment: a}, {Assignment: a}}}
			sources := triageSources{query: func(context.Context, backlogadmin.Query) (backlogadmin.Response, error) {
				return backlogadmin.Response{Workflow: detail}, nil
			}}
			report, again := triageReport{GeneratedAt: now}, triageReport{GeneratedAt: now}
			triageUnknownAssignments(context.Background(), &report, sources, runs, threshold)
			triageUnknownAssignments(context.Background(), &again, sources, runs, threshold)
			want := 0
			if age <= 0 || age > threshold {
				want = 1
			}
			if len(report.Items) != want || !reflect.DeepEqual(report, again) || !reflect.DeepEqual(before, *a) {
				t.Fatalf("age %v threshold %v: %+v", age, threshold, report)
			}
			if want == 1 && (report.Items[0].Subject != "a/2" || (report.Items[0].Since == nil) != (age <= 0)) {
				t.Fatal("identity/origin")
			}
			a.UnknownSince = nil
			report = triageReport{GeneratedAt: now}
			triageUnknownAssignments(context.Background(), &report, sources, runs, threshold)
			if len(report.Items) != 1 || report.Items[0].Since != nil {
				t.Fatal("legacy age fabricated")
			}
			a.State = domain.AssignmentReleased
			report = triageReport{GeneratedAt: now}
			triageUnknownAssignments(context.Background(), &report, sources, runs, threshold)
			if len(report.Items) != 0 {
				t.Fatal("recovered custody remains")
			}
			detail = nil
			triageUnknownAssignments(context.Background(), &report, sources, runs, threshold)
			if len(report.Unavailable) != 1 {
				t.Fatal("missing detail hidden")
			}
		}
	}
}
