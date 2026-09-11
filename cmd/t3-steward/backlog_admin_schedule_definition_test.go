package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type fakeScheduleDefinitionService struct {
	principal backlogadmin.Principal
	request   backlogadmin.LocalScheduleDefinitionRequest
	response  backlogadmin.LocalScheduleDefinitionResponse
	err       error
	calls     int
}

func (s *fakeScheduleDefinitionService) PutSchedule(
	_ context.Context,
	principal backlogadmin.Principal,
	request backlogadmin.LocalScheduleDefinitionRequest,
) (backlogadmin.LocalScheduleDefinitionResponse, error) {
	s.calls++
	s.principal = principal
	s.request = request
	return s.response, s.err
}

func TestParseScheduleDefinition(t *testing.T) {
	invocation, err := parseScheduleDefinition([]string{
		"put", "nightly", "--name", "Nightly checks", "--workflow", "workflow-1",
		"--cron", "30 2 * * *", "--timezone", "America/Los_Angeles",
		"--after-failure", "hold", "--disabled", "--expected-revision", "3",
		"--request-id", "definition-1", "--reason", "change window", "--json",
	})
	if err != nil {
		t.Fatal(err)
	}
	request := invocation.request
	if request.ID != "nightly" || request.Name != "Nightly checks" ||
		request.WorkflowID != "workflow-1" || request.Expression != "30 2 * * *" ||
		request.Timezone != "America/Los_Angeles" || request.AfterFailure != domain.ScheduleFailureHold ||
		request.Enabled || request.ExpectedRevision != 3 || request.RequestID != "definition-1" ||
		request.Reason != "change window" || !invocation.asJSON {
		t.Fatalf("invocation = %+v", invocation)
	}
}

func TestParseScheduleDefinitionRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"missing fields", []string{"put", "nightly"}, "name must be nonempty"},
		{"invalid id", []string{"put", "bad/id"}, "invalid schedule id"},
		{"negative revision", []string{
			"put", "nightly", "--name", "Nightly", "--workflow", "workflow-1",
			"--cron", "0 2 * * *", "--timezone", "UTC", "--reason", "create",
			"--expected-revision", "-1",
		}, "invalid expected revision"},
		{"unknown policy", []string{
			"put", "nightly", "--name", "Nightly", "--workflow", "workflow-1",
			"--cron", "0 2 * * *", "--timezone", "UTC", "--reason", "create",
			"--after-failure", "ignore",
		}, "unsupported schedule after-failure"},
		{"duplicate option", []string{
			"put", "nightly", "--name", "Nightly", "--name", "Again",
		}, "provided more than once"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseScheduleDefinition(test.args)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestRunScheduleDefinitionGeneratesIdentityAndRendersResult(t *testing.T) {
	fake := &fakeScheduleDefinitionService{
		response: backlogadmin.LocalScheduleDefinitionResponse{
			Schedule: domain.Schedule{ID: "nightly", Revision: 1},
		},
	}
	var output bytes.Buffer
	cli := backlogAdminCLI{
		scheduleDefinitions: fake,
		principal:           backlogadmin.Principal{ID: "operator", Roles: []string{"local-admin"}},
		stdout:              &output,
		newCommandID: func() (string, error) {
			return "definition-generated", nil
		},
	}
	err := cli.runSchedules(context.Background(), []string{
		"put", "nightly", "--name", "Nightly", "--workflow", "workflow-1",
		"--cron", "0 2 * * *", "--timezone", "UTC", "--reason", "create",
	})
	if err != nil {
		t.Fatal(err)
	}
	if fake.calls != 1 || fake.principal.ID != "operator" ||
		fake.request.RequestID != "definition-generated" || !fake.request.Enabled ||
		fake.request.AfterFailure != domain.ScheduleFailureNextCycle {
		t.Fatalf("calls = %d, principal = %+v, request = %+v", fake.calls, fake.principal, fake.request)
	}
	if got := output.String(); got != "schedule nightly revision 1 (created)\n" {
		t.Fatalf("output = %q", got)
	}
}

func TestRunScheduleDefinitionStopsBeforeTransportOnIdentityFailure(t *testing.T) {
	fake := &fakeScheduleDefinitionService{}
	cli := backlogAdminCLI{
		scheduleDefinitions: fake,
		stdout:              &bytes.Buffer{},
		newCommandID: func() (string, error) {
			return "", errors.New("entropy unavailable")
		},
	}
	err := cli.runScheduleDefinition(context.Background(), []string{
		"put", "nightly", "--name", "Nightly", "--workflow", "workflow-1",
		"--cron", "0 2 * * *", "--timezone", "UTC", "--reason", "create",
	})
	if err == nil || !strings.Contains(err.Error(), "entropy unavailable") || fake.calls != 0 {
		t.Fatalf("error = %v, calls = %d", err, fake.calls)
	}
}
