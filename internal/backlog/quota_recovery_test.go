package backlog

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestDeriveQuotaPlanningStateReconstructsPausedReservationsAndSlots(t *testing.T) {
	input := quotaRecoveryFixture()
	got, err := DeriveQuotaPlanningState(input)
	if err != nil {
		t.Fatalf("DeriveQuotaPlanningState: %v", err)
	}
	if len(got.QuotaPools) != 1 || got.QuotaPools[0].ActiveAssignments != 2 {
		t.Fatalf("runtime occupancy = %#v, want 2 active slots", got.QuotaPools)
	}
	if len(got.QuotaWindows) != 2 {
		t.Fatalf("quota windows = %#v", got.QuotaWindows)
	}
	for _, window := range got.QuotaWindows {
		if window.PausedRequiredWorkRemainder != 35 {
			t.Fatalf("window %q paused remainder = %v, want 35", window.WindowID, window.PausedRequiredWorkRemainder)
		}
	}
	wantReservations := []QuotaResumeReservation{
		{AttemptID: "forced", QuotaPoolID: "shared", Class: domain.TaskClassRequired, Status: domain.ResumePending, RemainingCost: 20, StopEpoch: "directive-forced"},
		{AttemptID: "paused", QuotaPoolID: "shared", Class: domain.TaskClassRequired, Status: domain.ResumePending, RemainingCost: 10, StopEpoch: "directive-paused"},
		{AttemptID: "resuming", QuotaPoolID: "shared", Class: domain.TaskClassRequired, Status: domain.ResumeResuming, RemainingCost: 5, StopEpoch: "directive-resuming"},
		{AttemptID: "surplus", QuotaPoolID: "shared", Class: domain.TaskClassSurplus, Status: domain.ResumePending, RemainingCost: 40, StopEpoch: "directive-surplus"},
	}
	if !reflect.DeepEqual(got.ResumeReservations, wantReservations) {
		t.Fatalf("resume reservations = %#v, want %#v", got.ResumeReservations, wantReservations)
	}
	for _, reservation := range got.ResumeReservations {
		assignment := recoveryAssignment(input.Assignments, reservation.AttemptID)
		record := recoveryThrottle(input.ThrottleRecords, reservation.AttemptID)
		if record.Command.WorkerID != assignment.WorkerID ||
			record.Command.ThreadID != assignment.ThreadID ||
			!reflect.DeepEqual(record.Command.Route, assignment.Route) {
			t.Fatalf("reservation %q changed execution identity", reservation.AttemptID)
		}
	}
	if input.QuotaPools[0].ActiveAssignments != 99 || input.QuotaWindows[0].PausedRequiredWorkRemainder != 99 {
		t.Fatal("derivation mutated caller input")
	}
}

func TestDeriveQuotaPlanningStateIsStableAcrossRestartAndInputOrder(t *testing.T) {
	input := quotaRecoveryFixture()
	first, err := DeriveQuotaPlanningState(input)
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(input.Tasks)
	slices.Reverse(input.Attempts)
	slices.Reverse(input.Assignments)
	slices.Reverse(input.ThrottleRecords)
	slices.Reverse(input.RouteEstimates)
	slices.Reverse(input.QuotaWindows)
	second, err := DeriveQuotaPlanningState(input)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("restart reconstruction changed with input order: first %#v second %#v", first, second)
	}
}

func TestDeriveQuotaPlanningStateRejectsContradictoryDurableState(t *testing.T) {
	tests := []struct {
		name string
		edit func(*QuotaPlanningStateInput)
		want string
	}{
		{name: "duplicate attempt", edit: func(input *QuotaPlanningStateInput) {
			input.Attempts = append(input.Attempts, input.Attempts[0])
		}, want: "repeat"},
		{name: "duplicate throttle record", edit: func(input *QuotaPlanningStateInput) {
			input.ThrottleRecords = append(input.ThrottleRecords, input.ThrottleRecords[0])
		}, want: "repeat"},
		{name: "missing fixed-route estimate", edit: func(input *QuotaPlanningStateInput) {
			input.RouteEstimates = input.RouteEstimates[1:]
		}, want: "no remaining-cost estimate"},
		{name: "stale throttle projection", edit: func(input *QuotaPlanningStateInput) {
			input.ThrottleRecords[0].Control = domain.ControlRunning
		}, want: "contradicts latest throttle control"},
		{name: "changed worker identity", edit: func(input *QuotaPlanningStateInput) {
			input.ThrottleRecords[0].Command.WorkerID = "other-worker"
		}, want: "execution identity contradicts"},
		{name: "settled assignment", edit: func(input *QuotaPlanningStateInput) {
			input.Assignments[0].State = domain.AssignmentReleased
		}, want: "settled assignment"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := quotaRecoveryFixture()
			test.edit(&input)
			_, err := DeriveQuotaPlanningState(input)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestPlanThrottleResumesReacquiresSharedPoolSlotDeterministically(t *testing.T) {
	input := quotaRecoveryFixture()
	var records []domain.ThrottleAttemptRecord
	for _, attemptID := range []string{"surplus", "paused"} {
		records = append(records, recoveryThrottle(input.ThrottleRecords, attemptID))
	}
	slices.Reverse(records)
	admissions := []domain.QuotaAdmissionRecord{{
		QuotaPoolID: "shared", Revision: 2, Admission: domain.AdmissionRecovering,
	}}
	pools := []domain.QuotaPool{{ID: "shared", MaxConcurrent: 2, ActiveAssignments: 1}}
	transitions, commands, err := PlanThrottleResumes(records, admissions, pools, throttleDeliveryTime.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(transitions) != 1 || len(commands) != 1 || commands[0].AttemptID != "paused" {
		t.Fatalf("slot-limited resume = %#v %#v, want paused attempt only", transitions, commands)
	}
	original := recoveryThrottle(input.ThrottleRecords, "paused").Command
	if commands[0].AssignmentID != original.AssignmentID ||
		commands[0].WorkerID != original.WorkerID ||
		commands[0].ThreadID != original.ThreadID ||
		commands[0].WorkspacePath != original.WorkspacePath ||
		!reflect.DeepEqual(commands[0].Route, original.Route) {
		t.Fatalf("resume changed fixed execution identity: %#v", commands[0])
	}

	full := []domain.QuotaPool{{ID: "shared", MaxConcurrent: 2, ActiveAssignments: 2}}
	transitions, commands, err = PlanThrottleResumes(records, admissions, full, throttleDeliveryTime.Add(2*time.Hour))
	if err != nil || len(transitions) != 0 || len(commands) != 0 {
		t.Fatalf("full pool resume = %#v %#v, err = %v", transitions, commands, err)
	}
}

func quotaRecoveryFixture() QuotaPlanningStateInput {
	type spec struct {
		id      string
		control domain.ControlState
		class   domain.TaskClass
		cost    float64
	}
	specs := []spec{
		{id: "paused", control: domain.ControlPaused, class: domain.TaskClassRequired, cost: 10},
		{id: "forced", control: domain.ControlPausedUncheckpointed, class: domain.TaskClassRequired, cost: 20},
		{id: "surplus", control: domain.ControlPaused, class: domain.TaskClassSurplus, cost: 40},
		{id: "running", control: domain.ControlRunning, class: domain.TaskClassRequired, cost: 8},
		{id: "resuming", control: domain.ControlResuming, class: domain.TaskClassRequired, cost: 5},
	}
	input := QuotaPlanningStateInput{
		QuotaPools: []domain.QuotaPool{{ID: "shared", MaxConcurrent: 3, ActiveAssignments: 99}},
		QuotaWindows: []QuotaWindowBudget{
			{QuotaPoolID: "shared", WindowID: "weekly", PausedRequiredWorkRemainder: 99},
			{QuotaPoolID: "shared", WindowID: "primary", PausedRequiredWorkRemainder: 99},
		},
	}
	for _, item := range specs {
		taskID := "task-" + item.id
		assignmentID := "assignment-" + item.id
		route := domain.ProviderRoute{
			WorkerID: "normandy", ProviderInstanceID: "codex", Model: "gpt-5.6-sol",
			Options: map[string]string{"effort": "medium"}, QuotaPoolID: "shared",
		}
		input.Tasks = append(input.Tasks, domain.Task{ID: taskID, Class: item.class})
		input.Attempts = append(input.Attempts, domain.Attempt{
			ID: item.id, TaskID: taskID, Progress: domain.ProgressActive,
			Control: item.control, AssignmentID: assignmentID, ThreadID: "thread-" + item.id,
		})
		input.Assignments = append(input.Assignments, domain.Assignment{
			ID: assignmentID, AttemptID: item.id, WorkerID: "normandy", Route: route,
			State: domain.AssignmentClaimed, Epoch: 7, ThreadID: "thread-" + item.id,
		})
		input.RouteEstimates = append(input.RouteEstimates, RouteEstimate{
			AttemptID: item.id, WorkerID: "normandy", ProviderInstanceID: "codex",
			Model: "gpt-5.6-sol", Options: map[string]string{"effort": "medium"},
			Estimate: TaskAdmissionEstimate{
				RemainingCost: item.cost, ExpectedRuntime: time.Hour, CheckpointMargin: 5 * time.Minute,
			},
		})
		if item.control == domain.ControlRunning {
			continue
		}
		kind := domain.ThrottleCommandDrain
		delivery := domain.ThrottleDeliveryAcknowledged
		if item.control == domain.ControlPausedUncheckpointed {
			kind = domain.ThrottleCommandHardStop
		}
		if item.control == domain.ControlResuming {
			kind = domain.ThrottleCommandResume
			delivery = domain.ThrottleDeliveryPending
		}
		input.ThrottleRecords = append(input.ThrottleRecords, domain.ThrottleAttemptRecord{
			DirectiveID: "directive-" + item.id, AttemptID: item.id, Revision: 2,
			Command: domain.ThrottleCommand{
				ID: "command-" + item.id, DirectiveID: "directive-" + item.id,
				AttemptID: item.id, AssignmentID: assignmentID, AssignmentEpoch: 7,
				WorkerID: "normandy", ThreadID: "thread-" + item.id,
				WorkspacePath: "/runs/" + item.id + "/workspace", Route: route,
				Kind: kind, QuotaPoolID: "shared", CreatedAt: throttleDeliveryTime,
			},
			Delivery: delivery, Control: item.control, UpdatedAt: throttleDeliveryTime,
		})
	}
	return input
}

func recoveryAssignment(assignments []domain.Assignment, attemptID string) domain.Assignment {
	for _, assignment := range assignments {
		if assignment.AttemptID == attemptID {
			return assignment
		}
	}
	return domain.Assignment{}
}

func recoveryThrottle(records []domain.ThrottleAttemptRecord, attemptID string) domain.ThrottleAttemptRecord {
	for _, record := range records {
		if record.AttemptID == attemptID {
			return record
		}
	}
	return domain.ThrottleAttemptRecord{}
}
