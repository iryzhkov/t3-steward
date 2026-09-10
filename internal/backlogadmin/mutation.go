package backlogadmin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

var ErrReadOnly = errors.New("backlog admin service is read-only")

type Mutator interface {
	SubmitAdminCommand(context.Context, domain.AdminCommand) (domain.AdminCommandDecision, error)
	CompleteAdminCommand(context.Context, domain.AdminCommandOutcome) (domain.AdminCommandDecision, error)
}

func (s *Service) Mutate(ctx context.Context, request Mutation) (MutationResponse, error) {
	if request.Version != Version {
		return MutationResponse{}, fmt.Errorf("%w: got %q, want %q", ErrUnsupportedVersion, request.Version, Version)
	}
	if strings.TrimSpace(request.ID) != request.ID || request.ID == "" ||
		strings.TrimSpace(string(request.Kind)) != string(request.Kind) || request.Kind == "" ||
		strings.TrimSpace(request.Principal.ID) != request.Principal.ID || request.Principal.ID == "" ||
		strings.TrimSpace(request.WorkflowRunID) != request.WorkflowRunID ||
		strings.TrimSpace(request.TaskID) != request.TaskID ||
		strings.TrimSpace(request.ScheduleID) != request.ScheduleID ||
		(request.ScheduleID == "" && request.WorkflowRunID == "") ||
		(request.ScheduleID != "" && (request.WorkflowRunID != "" || request.TaskID != "")) ||
		request.ExpectedRevision < 0 ||
		strings.TrimSpace(request.Reason) != request.Reason || request.Reason == "" ||
		(len(request.Payload) != 0 && !json.Valid(request.Payload)) {
		return MutationResponse{}, fmt.Errorf("%w: malformed mutation %q", ErrInvalidQuery, request.ID)
	}
	action := Action{
		CommandKind: request.Kind, WorkflowRunID: request.WorkflowRunID,
		TaskID: request.TaskID, ScheduleID: request.ScheduleID,
	}
	if err := s.authorizer.Authorize(ctx, request.Principal, action); err != nil {
		return MutationResponse{}, fmt.Errorf("authorize %s: %w", request.Kind, err)
	}
	mutator, ok := s.reader.(Mutator)
	if !ok {
		return MutationResponse{}, ErrReadOnly
	}
	records, err := s.reader.LoadCoordinatorRecords(ctx)
	if err != nil {
		return MutationResponse{}, fmt.Errorf("load coordinator snapshot: %w", err)
	}
	if existing, ok := existingAdminCommand(records.AdminCommands, request.ID); ok {
		if !mutationReplayMatches(records, request, existing) {
			return MutationResponse{}, fmt.Errorf("admin command replay %q conflicts with immutable request", request.ID)
		}
		replay := existing
		replay.State, replay.Failure, replay.AppliedAt = domain.AdminCommandPending, "", nil
		decision, err := mutator.SubmitAdminCommand(ctx, replay)
		if err != nil {
			return MutationResponse{}, fmt.Errorf("submit admin command replay: %w", err)
		}
		return mutationResponse(decision), nil
	}
	targetType, targetID, err := resolveMutationTarget(records, request)
	if err != nil {
		return MutationResponse{}, err
	}
	command := domain.AdminCommand{
		ID: request.ID, Kind: request.Kind, TargetType: targetType, TargetID: targetID,
		ExpectedRevision: request.ExpectedRevision, Reason: request.Reason,
		RequestedBy: request.Principal.ID, Payload: append(json.RawMessage(nil), request.Payload...),
		State: domain.AdminCommandPending, CreatedAt: s.now().UTC(),
	}
	decision, err := mutator.SubmitAdminCommand(ctx, command)
	if err != nil {
		return MutationResponse{}, fmt.Errorf("submit admin command: %w", err)
	}
	return mutationResponse(decision), nil
}

func (s *Service) CompleteCommand(ctx context.Context, outcome CommandOutcome) (MutationResponse, error) {
	if outcome.Version != Version {
		return MutationResponse{}, fmt.Errorf("%w: got %q, want %q", ErrUnsupportedVersion, outcome.Version, Version)
	}
	if strings.TrimSpace(outcome.CommandID) != outcome.CommandID || outcome.CommandID == "" ||
		strings.TrimSpace(outcome.Principal.ID) != outcome.Principal.ID || outcome.Principal.ID == "" {
		return MutationResponse{}, fmt.Errorf("%w: malformed command outcome", ErrInvalidQuery)
	}
	action := Action{
		CommandID: outcome.CommandID, OutcomeState: outcome.State,
	}
	if err := s.authorizer.Authorize(ctx, outcome.Principal, action); err != nil {
		return MutationResponse{}, fmt.Errorf("authorize command outcome: %w", err)
	}
	mutator, ok := s.reader.(Mutator)
	if !ok {
		return MutationResponse{}, ErrReadOnly
	}
	decision, err := mutator.CompleteAdminCommand(ctx, domain.AdminCommandOutcome{
		CommandID: outcome.CommandID, ExpectedState: outcome.ExpectedState,
		State: outcome.State, Failure: outcome.Failure, AppliedAt: s.now().UTC(),
	})
	if err != nil {
		return MutationResponse{}, fmt.Errorf("complete admin command: %w", err)
	}
	return mutationResponse(decision), nil
}

func existingAdminCommand(commands []domain.AdminCommand, id string) (domain.AdminCommand, bool) {
	for _, command := range commands {
		if command.ID == id {
			return command, true
		}
	}
	return domain.AdminCommand{}, false
}

func mutationReplayMatches(records sqlite.CoordinatorRecords, request Mutation, command domain.AdminCommand) bool {
	if command.Kind != request.Kind || command.Reason != request.Reason ||
		command.RequestedBy != request.Principal.ID || !sameMutationJSON(command.Payload, request.Payload) {
		return false
	}
	if request.ScheduleID != "" {
		return command.TargetType == domain.AdminTargetSchedule && command.TargetID == request.ScheduleID
	}
	if command.TargetType == domain.AdminTargetWorkflowRun {
		return request.TaskID == "" && command.TargetID == request.WorkflowRunID
	}
	if command.TargetType != domain.AdminTargetAttempt || request.TaskID == "" {
		return false
	}
	for _, attempt := range records.Attempts {
		if attempt.ID != command.TargetID || attempt.WorkflowRunID != request.WorkflowRunID {
			continue
		}
		for _, task := range records.Tasks {
			if task.ID == attempt.TaskID && (task.ID == request.TaskID || task.Name == request.TaskID) {
				return true
			}
		}
	}
	return false
}

func sameMutationJSON(left, right json.RawMessage) bool {
	if len(left) == 0 || string(left) == "null" {
		return len(right) == 0 || string(right) == "null"
	}
	var leftValue, rightValue any
	return json.Unmarshal(left, &leftValue) == nil && json.Unmarshal(right, &rightValue) == nil &&
		reflect.DeepEqual(leftValue, rightValue)
}

func resolveMutationTarget(records sqlite.CoordinatorRecords, request Mutation) (domain.AdminTargetType, string, error) {
	if request.ScheduleID != "" {
		if request.WorkflowRunID != "" || request.TaskID != "" {
			return "", "", fmt.Errorf("%w: schedule and workflow targets are mutually exclusive", ErrInvalidQuery)
		}
		for _, schedule := range records.Schedules {
			if schedule.ID == request.ScheduleID {
				return domain.AdminTargetSchedule, schedule.ID, nil
			}
		}
		return "", "", notFound("schedule", request.ScheduleID)
	}
	if request.WorkflowRunID == "" {
		return "", "", fmt.Errorf("%w: workflow run or schedule target is required", ErrInvalidQuery)
	}
	if request.TaskID == "" {
		for _, run := range records.WorkflowRuns {
			if run.ID == request.WorkflowRunID {
				return domain.AdminTargetWorkflowRun, run.ID, nil
			}
		}
		return "", "", notFound("workflow run", request.WorkflowRunID)
	}
	v := newView(records, nil, nil, time.Time{})
	task, ok := v.resolveTask(request.WorkflowRunID, request.TaskID)
	if !ok {
		return "", "", notFound("task", request.WorkflowRunID+"/"+request.TaskID)
	}
	attempt := latestAttempt(v.attempts[request.WorkflowRunID+"\x00"+task.ID])
	if attempt == nil {
		return "", "", notFound("attempt", request.WorkflowRunID+"/"+task.ID)
	}
	return domain.AdminTargetAttempt, attempt.ID, nil
}

func mutationResponse(decision domain.AdminCommandDecision) MutationResponse {
	return MutationResponse{
		Version: Version, Command: commandDTO(decision.Command),
		Event: auditEventDTO(decision.Event), CurrentTarget: decision.CurrentTarget,
	}
}

func commandDTO(command domain.AdminCommand) Command {
	return Command{
		ID: command.ID, Kind: command.Kind, TargetType: command.TargetType,
		TargetID: command.TargetID, ExpectedRevision: command.ExpectedRevision,
		Reason: command.Reason, RequestedBy: command.RequestedBy,
		Payload: append(json.RawMessage(nil), command.Payload...), State: command.State,
		Failure: command.Failure, CreatedAt: command.CreatedAt, AppliedAt: command.AppliedAt,
	}
}

func auditEventDTO(item domain.AuditEvent) Event {
	return Event{
		ID: item.ID, Sequence: item.Sequence, WorkflowRunID: item.WorkflowRunID,
		TaskID: item.TaskID, AttemptID: item.AttemptID, Kind: item.Kind,
		TargetType: item.TargetType, TargetID: item.TargetID,
		Actor: item.Actor, Reason: item.Reason, At: item.CreatedAt,
		Detail: append(json.RawMessage(nil), item.Detail...),
	}
}

func (v view) commands(query Query) []Command {
	attemptIDs := make(map[string]struct{})
	if query.WorkflowRunID != "" {
		for key, attempts := range v.attempts {
			if strings.HasPrefix(key, query.WorkflowRunID+"\x00") {
				if query.TaskID == "" {
					for _, attempt := range attempts {
						attemptIDs[attempt.ID] = struct{}{}
					}
					continue
				}
				task, ok := v.resolveTask(query.WorkflowRunID, query.TaskID)
				if ok && strings.HasSuffix(key, "\x00"+task.ID) {
					for _, attempt := range attempts {
						attemptIDs[attempt.ID] = struct{}{}
					}
				}
			}
		}
	}
	result := make([]Command, 0)
	for _, command := range v.records.AdminCommands {
		if query.CommandID != "" && command.ID != query.CommandID {
			continue
		}
		if query.WorkflowRunID != "" {
			matches := command.TargetType == domain.AdminTargetWorkflowRun && command.TargetID == query.WorkflowRunID
			if command.TargetType == domain.AdminTargetAttempt {
				_, matches = attemptIDs[command.TargetID]
			}
			if !matches {
				continue
			}
		}
		result = append(result, commandDTO(command))
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].ID < result[j].ID
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result
}
