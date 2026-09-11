package backlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type ScheduleDefinitionStore interface {
	LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error)
	SaveCoordinatorRecords(context.Context, sqlite.CoordinatorRecords) error
}

type ScheduleDefinitionRequest struct {
	RequestID        string
	ID               string
	Name             string
	WorkflowID       string
	Expression       string
	Timezone         string
	AfterFailure     domain.ScheduleFailurePolicy
	Enabled          bool
	ExpectedRevision int64
	Actor            string
	Reason           string
}

type ScheduleDefinitionResult struct {
	Schedule domain.Schedule
	Replay   bool
}

type ScheduleDefinitionService struct {
	Store ScheduleDefinitionStore
	Now   func() time.Time

	mu sync.Mutex
}

type scheduleDefinitionEventDetail struct {
	Request  ScheduleDefinitionRequest `json:"request"`
	Schedule domain.Schedule           `json:"schedule"`
}

func (s *ScheduleDefinitionService) Put(ctx context.Context, request ScheduleDefinitionRequest) (ScheduleDefinitionResult, error) {
	if s == nil || s.Store == nil {
		return ScheduleDefinitionResult{}, errors.New("schedule definition store is required")
	}
	if err := validateScheduleDefinitionRequest(request); err != nil {
		return ScheduleDefinitionResult{}, err
	}
	if _, err := ParseScheduleExpression(request.Expression); err != nil {
		return ScheduleDefinitionResult{}, fmt.Errorf("schedule expression: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	records, err := s.Store.LoadCoordinatorRecords(ctx)
	if err != nil {
		return ScheduleDefinitionResult{}, err
	}
	eventID := "schedule-definition:" + request.RequestID
	for _, event := range records.AuditEvents {
		if event.ID != eventID {
			continue
		}
		var detail scheduleDefinitionEventDetail
		if err := json.Unmarshal(event.Detail, &detail); err != nil {
			return ScheduleDefinitionResult{}, fmt.Errorf("decode schedule definition replay %q: %w", request.RequestID, err)
		}
		if !reflect.DeepEqual(detail.Request, request) {
			return ScheduleDefinitionResult{}, fmt.Errorf("schedule definition request %q already has different content", request.RequestID)
		}
		return ScheduleDefinitionResult{Schedule: detail.Schedule, Replay: true}, nil
	}
	if !workflowExists(records.Workflows, request.WorkflowID) {
		return ScheduleDefinitionResult{}, fmt.Errorf("schedule workflow %q not found", request.WorkflowID)
	}
	now := time.Now().UTC()
	if s.Now != nil {
		now = s.Now().UTC()
	}
	var previousTemplate *domain.ScheduleTemplate
	var schedule domain.Schedule
	found := false
	for _, item := range records.Schedules {
		if item.ID != request.ID {
			continue
		}
		found = true
		if item.Revision != request.ExpectedRevision {
			return ScheduleDefinitionResult{}, fmt.Errorf("schedule %q revision is %d, expected %d", request.ID, item.Revision, request.ExpectedRevision)
		}
		schedule = item
		template, ok := currentScheduleTemplate(records.ScheduleTemplates, item)
		if !ok {
			return ScheduleDefinitionResult{}, fmt.Errorf("schedule %q current template %d is missing", item.ID, item.Version)
		}
		previousTemplate = &template
		break
	}
	if !found && request.ExpectedRevision != 0 {
		return ScheduleDefinitionResult{}, fmt.Errorf("new schedule %q expected revision must be zero", request.ID)
	}
	version := 1
	revision := int64(1)
	createdAt := now
	if found {
		version = schedule.Version + 1
		revision = schedule.Revision + 1
		createdAt = schedule.CreatedAt
	}
	template, err := domain.PrepareScheduleTemplate(previousTemplate, domain.ScheduleTemplate{
		ScheduleID: request.ID, Version: version, WorkflowID: request.WorkflowID,
		Expression: request.Expression, Timezone: request.Timezone,
		Overlap: domain.ScheduleOverlapForbid, Misfire: domain.ScheduleMisfireSkip,
		AfterFailure: request.AfterFailure, CreatedAt: now,
	})
	if err != nil {
		return ScheduleDefinitionResult{}, err
	}
	next := domain.Schedule{
		ID: request.ID, Name: request.Name, Version: version,
		WorkflowID: request.WorkflowID, Expression: request.Expression,
		Timezone: request.Timezone, Overlap: template.Overlap, Misfire: template.Misfire,
		AfterFailure: template.AfterFailure, Enabled: request.Enabled,
		Revision: revision, CreatedAt: createdAt, UpdatedAt: now,
	}
	if found {
		next.ActiveRunID = schedule.ActiveRunID
		next.NextNotBefore = schedule.NextNotBefore
	}
	detail, err := json.Marshal(scheduleDefinitionEventDetail{Request: request, Schedule: next})
	if err != nil {
		return ScheduleDefinitionResult{}, err
	}
	event := domain.AuditEvent{
		ID: eventID, Kind: "schedule-definition-updated",
		TargetType: domain.AdminTargetSchedule, TargetID: request.ID,
		Actor: request.Actor, Reason: request.Reason, Detail: detail, CreatedAt: now,
	}
	if !found {
		event.Kind = "schedule-definition-created"
	}
	if err := s.Store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Schedules: []domain.Schedule{next}, ScheduleTemplates: []domain.ScheduleTemplate{template},
		AuditEvents: []domain.AuditEvent{event},
	}); err != nil {
		return ScheduleDefinitionResult{}, err
	}
	return ScheduleDefinitionResult{Schedule: next}, nil
}

func validateScheduleDefinitionRequest(request ScheduleDefinitionRequest) error {
	for label, value := range map[string]string{
		"request ID": request.RequestID, "schedule ID": request.ID,
		"name": request.Name, "workflow ID": request.WorkflowID,
		"expression": request.Expression, "timezone": request.Timezone,
		"actor": request.Actor, "reason": request.Reason,
	} {
		if strings.TrimSpace(value) != value || value == "" {
			return fmt.Errorf("schedule definition %s must be nonempty and trimmed", label)
		}
	}
	if request.ExpectedRevision < 0 {
		return errors.New("schedule definition expected revision cannot be negative")
	}
	if _, err := time.LoadLocation(request.Timezone); err != nil {
		return fmt.Errorf("schedule timezone %q: %w", request.Timezone, err)
	}
	switch request.AfterFailure {
	case "", domain.ScheduleFailureNextCycle, domain.ScheduleFailureHold:
	default:
		return fmt.Errorf("unsupported schedule after-failure policy %q", request.AfterFailure)
	}
	return nil
}

func workflowExists(workflows []domain.Workflow, id string) bool {
	for _, workflow := range workflows {
		if workflow.ID == id {
			return true
		}
	}
	return false
}
