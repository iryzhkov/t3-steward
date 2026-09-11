package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type scheduleDefinitionInvocation struct {
	request backlogadmin.LocalScheduleDefinitionRequest
	asJSON  bool
}

func parseScheduleDefinition(args []string) (scheduleDefinitionInvocation, error) {
	const usage = "schedule definition usage: put <schedule> --name TEXT --workflow ID --cron EXPR --timezone IANA --reason TEXT"
	if len(args) < 2 || args[0] != "put" {
		return scheduleDefinitionInvocation{}, errors.New(usage)
	}
	if err := validateScheduleDefinitionValue("schedule id", args[1]); err != nil {
		return scheduleDefinitionInvocation{}, err
	}
	invocation := scheduleDefinitionInvocation{
		request: backlogadmin.LocalScheduleDefinitionRequest{
			ID: args[1], Enabled: true, AfterFailure: domain.ScheduleFailureNextCycle,
		},
	}
	seen := make(map[string]bool)
	for index := 2; index < len(args); index++ {
		option := args[index]
		if seen[option] {
			return scheduleDefinitionInvocation{}, fmt.Errorf("schedule definition option %s was provided more than once", option)
		}
		seen[option] = true
		switch option {
		case "--json":
			invocation.asJSON = true
		case "--disabled":
			invocation.request.Enabled = false
		case "--name", "--workflow", "--cron", "--timezone", "--reason", "--after-failure", "--expected-revision", "--request-id":
			if index+1 >= len(args) {
				return scheduleDefinitionInvocation{}, fmt.Errorf("schedule definition option %s needs a value", option)
			}
			index++
			value := args[index]
			switch option {
			case "--name":
				invocation.request.Name = value
			case "--workflow":
				invocation.request.WorkflowID = value
			case "--cron":
				invocation.request.Expression = value
			case "--timezone":
				invocation.request.Timezone = value
			case "--reason":
				invocation.request.Reason = value
			case "--after-failure":
				invocation.request.AfterFailure = domain.ScheduleFailurePolicy(value)
			case "--expected-revision":
				revision, err := strconv.ParseInt(value, 10, 64)
				if err != nil || revision < 0 {
					return scheduleDefinitionInvocation{}, fmt.Errorf("invalid expected revision %q", value)
				}
				invocation.request.ExpectedRevision = revision
			case "--request-id":
				invocation.request.RequestID = value
			}
		default:
			return scheduleDefinitionInvocation{}, fmt.Errorf("unknown schedule definition option %q", option)
		}
	}
	for _, field := range []struct {
		label string
		value string
	}{
		{"name", invocation.request.Name},
		{"workflow id", invocation.request.WorkflowID},
		{"cron expression", invocation.request.Expression},
		{"timezone", invocation.request.Timezone},
		{"reason", invocation.request.Reason},
	} {
		if err := validateScheduleDefinitionValue(field.label, field.value); err != nil {
			return scheduleDefinitionInvocation{}, err
		}
	}
	if invocation.request.RequestID != "" {
		if err := validateScheduleDefinitionValue("request id", invocation.request.RequestID); err != nil {
			return scheduleDefinitionInvocation{}, err
		}
	}
	switch invocation.request.AfterFailure {
	case domain.ScheduleFailureNextCycle, domain.ScheduleFailureHold:
	default:
		return scheduleDefinitionInvocation{}, fmt.Errorf(
			"unsupported schedule after-failure policy %q", invocation.request.AfterFailure,
		)
	}
	return invocation, nil
}

func validateScheduleDefinitionValue(label, value string) error {
	if value == "" || strings.TrimSpace(value) != value {
		return fmt.Errorf("schedule definition %s must be nonempty and trimmed", label)
	}
	if label == "schedule id" && strings.Contains(value, "/") {
		return fmt.Errorf("invalid schedule id %q", value)
	}
	return nil
}

func (c backlogAdminCLI) runScheduleDefinition(ctx context.Context, args []string) error {
	if c.scheduleDefinitions == nil {
		return backlogadmin.ErrReadOnly
	}
	invocation, err := parseScheduleDefinition(args)
	if err != nil {
		return err
	}
	if invocation.request.RequestID == "" {
		generate := c.newCommandID
		if generate == nil {
			generate = newAdminCommandID
		}
		invocation.request.RequestID, err = generate()
		if err != nil {
			return fmt.Errorf("create schedule definition request id: %w", err)
		}
	}
	response, err := c.scheduleDefinitions.PutSchedule(ctx, c.principal, invocation.request)
	if err != nil {
		return err
	}
	if invocation.asJSON {
		encoder := json.NewEncoder(c.stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(response)
	}
	outcome := "updated"
	if response.Schedule.Revision == 1 {
		outcome = "created"
	}
	if response.Replay {
		outcome = "replayed"
	}
	_, err = fmt.Fprintf(c.stdout, "schedule %s revision %d (%s)\n", response.Schedule.ID, response.Schedule.Revision, outcome)
	return err
}
