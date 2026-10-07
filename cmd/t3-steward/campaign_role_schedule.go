package main

import (
	"context"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type coordinatorRoleScheduleStore struct {
	Store backlog.ScheduleTimerStore
	admin roleReadinessQuery
}

func (s coordinatorRoleScheduleStore) LoadCoordinatorRecords(ctx context.Context) (sqlite.CoordinatorRecords, error) {
	return s.Store.LoadCoordinatorRecords(ctx)
}
func (s coordinatorRoleScheduleStore) CommitScheduleTrigger(ctx context.Context, r domain.ScheduleTriggerRequest) (domain.ScheduleTriggerResult, error) {
	resolved, err := s.ResolveScheduleTrigger(ctx, r)
	if err != nil {
		return domain.ScheduleTriggerResult{}, err
	}
	return s.Store.CommitScheduleTrigger(ctx, resolved)
}

// Both the recurring timer and manual admin command invoke this seam before the
// store's occurrence transaction. Resolution errors suppress rather than fail runs.
func (s coordinatorRoleScheduleStore) ResolveScheduleTrigger(ctx context.Context, r domain.ScheduleTriggerRequest) (domain.ScheduleTriggerRequest, error) {
	records, err := s.Store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return r, err
	}
	var workflowID string
	for _, schedule := range records.Schedules {
		if schedule.ID != r.ScheduleID {
			continue
		}
		for _, template := range records.ScheduleTemplates {
			if template.ScheduleID == schedule.ID && template.Version == schedule.Version {
				workflowID = template.WorkflowID
				break
			}
		}
		break
	}
	if workflowID == "" {
		return r, fmt.Errorf("schedule %s current template missing", r.ScheduleID)
	}
	var workflow domain.Workflow
	for _, w := range records.Workflows {
		if w.ID == workflowID {
			workflow = w
			break
		}
	}
	request := backlogadmin.ViabilityRequest{SchemaVersion: campaignCheckSchemaVersion}
	names := map[string]string{}
	hasRole := false
	for _, task := range records.Tasks {
		if task.WorkflowID != workflowID || task.RunID != "" {
			continue
		}
		request.Tasks = append(request.Tasks, backlogadmin.ViabilityTask{Name: task.Name, Project: workflow.Project, Type: workflow.Environment.Type, Ref: workflow.Environment.Ref, Class: task.Class, Hosts: task.Placement.Hosts, Capabilities: task.Placement.Capabilities, Resources: task.ResourceDemand, ResourcePreset: task.ResourcePreset, Routes: task.Routes, ResourceLocks: task.ResourceLocks, Role: task.Role, RoleEffort: task.RoleEffort, Needs: task.Needs, Producers: producerTaskNames(task), ReviewType: task.ReviewOutput != nil})
		names[task.Name] = task.ID
		hasRole = hasRole || task.Role != ""
	}
	if !hasRole {
		return r, nil
	}
	selections, err := queryRoleSelections(ctx, s.admin, request)
	if err != nil {
		r.RouteSelections = nil
		r.RoleResolutionError = err.Error()
		return r, nil
	}
	r.RouteSelections = make(map[string]domain.RoleSelection, len(selections))
	for name, selection := range selections {
		r.RouteSelections[names[name]] = selection
	}
	return r, nil
}
func producerTaskNames(task domain.Task) []string {
	names := make([]string, 0, len(task.DependencyInputs))
	for name := range task.DependencyInputs {
		names = append(names, name)
	}
	return names
}
