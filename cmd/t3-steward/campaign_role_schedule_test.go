package main

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"testing"
)

type roleScheduleStore struct {
	records sqlite.CoordinatorRecords
	request domain.ScheduleTriggerRequest
}

func (s *roleScheduleStore) LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error) {
	return s.records, nil
}
func (s *roleScheduleStore) CommitScheduleTrigger(_ context.Context, r domain.ScheduleTriggerRequest) (domain.ScheduleTriggerResult, error) {
	s.request = r
	return domain.ScheduleTriggerResult{}, nil
}

type roleScheduleQuery struct {
	calls    int
	requests []backlogadmin.ViabilityRequest
	fail     bool
}

func (q *roleScheduleQuery) Query(_ context.Context, r backlogadmin.Query) (backlogadmin.Response, error) {
	q.calls++
	q.requests = append(q.requests, *r.Viability)
	if q.fail {
		return backlogadmin.Response{}, errors.New("policy unavailable")
	}
	return backlogadmin.Response{Viability: &backlogadmin.ViabilityMatrix{Tasks: []backlogadmin.ViabilityTaskResult{{Task: "inspect", RoleSelection: &domain.RoleSelection{Role: "execute", Route: map[bool]string{true: "codex/first", false: "codex/second"}[q.calls == 1], Effort: "low"}}}}}, nil
}
func TestScheduleRoleDecoratorResolvesEveryOccurrenceAndSharesManualSeam(t *testing.T) {
	store := &roleScheduleStore{records: sqlite.CoordinatorRecords{Schedules: []domain.Schedule{{ID: "schedule", Version: 1}}, ScheduleTemplates: []domain.ScheduleTemplate{{ScheduleID: "schedule", Version: 1, WorkflowID: "workflow"}}, Workflows: []domain.Workflow{{ID: "workflow", Project: "project"}}, Tasks: []domain.Task{{ID: "task", WorkflowID: "workflow", Name: "inspect", Role: "execute", RoleEffort: "low"}}}}
	query := &roleScheduleQuery{}
	decorator := coordinatorRoleScheduleStore{Store: store, admin: query}
	for _, source := range []domain.ScheduleTriggerSource{domain.ScheduleTriggerScheduled, domain.ScheduleTriggerManual} {
		if _, err := decorator.CommitScheduleTrigger(context.Background(), domain.ScheduleTriggerRequest{ScheduleID: "schedule", Source: source}); err != nil {
			t.Fatal(err)
		}
		selection := store.request.RouteSelections["task"]
		if selection.Route != map[bool]string{true: "codex/first", false: "codex/second"}[query.calls == 1] {
			t.Fatalf("selection = %#v", selection)
		}
		if query.requests[query.calls-1].SchemaVersion != campaignCheckSchemaVersion {
			t.Fatalf("invalid schedule readiness schema: %d", query.requests[query.calls-1].SchemaVersion)
		}
		if query.requests[query.calls-1].Tasks[0].Role != "execute" {
			t.Fatal("role not projected")
		}
	}
	query.fail = true
	r, err := decorator.ResolveScheduleTrigger(context.Background(), domain.ScheduleTriggerRequest{ScheduleID: "schedule"})
	if err != nil {
		t.Fatal(err)
	}
	if r.RoleResolutionError == "" || len(r.RouteSelections) != 0 {
		t.Fatalf("failed resolution not forwarded: %#v", r)
	}
}
