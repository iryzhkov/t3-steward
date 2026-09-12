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
		{AttemptID: "forced", TaskID: "task-forced", QuotaPoolID: "shared", Class: domain.TaskClassRequired, Status: domain.ResumePending, RemainingCost: 20, StopEpoch: "directive-forced"},
		{AttemptID: "paused", TaskID: "task-paused", QuotaPoolID: "shared", Class: domain.TaskClassRequired, Status: domain.ResumePending, RemainingCost: 10, StopEpoch: "directive-paused"},
		{AttemptID: "resuming", TaskID: "task-resuming", QuotaPoolID: "shared", Class: domain.TaskClassRequired, Status: domain.ResumeResuming, RemainingCost: 5, StopEpoch: "directive-resuming"},
		{AttemptID: "surplus", TaskID: "task-surplus", QuotaPoolID: "shared", Class: domain.TaskClassSurplus, Status: domain.ResumePending, RemainingCost: 40, StopEpoch: "directive-surplus"},
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

func TestDeriveQuotaPlanningStateCarriesDetachedRecoveryMetadata(t *testing.T) {
	input := quotaRecoveryFixture()
	deadline := throttleDeliveryTime.Add(3 * time.Hour)
	expires := throttleDeliveryTime.Add(6 * time.Hour)
	for index := range input.Tasks {
		if input.Tasks[index].ID == "task-paused" {
			input.Tasks[index].Deadline = &deadline
			input.Tasks[index].ExpiresAt = &expires
		}
	}
	for index := range input.Attempts {
		if input.Attempts[index].ID == "paused" {
			input.Attempts[index].Revision = 9
		}
	}
	got, err := DeriveQuotaPlanningState(input)
	if err != nil {
		t.Fatal(err)
	}
	var reservation QuotaResumeReservation
	for _, candidate := range got.ResumeReservations {
		if candidate.AttemptID == "paused" {
			reservation = candidate
		}
	}
	if reservation.TaskID != "task-paused" || reservation.AttemptRevision != 9 ||
		reservation.Deadline == nil || !reservation.Deadline.Equal(deadline) ||
		reservation.ExpiresAt == nil || !reservation.ExpiresAt.Equal(expires) {
		t.Fatalf("recovery metadata = %#v", reservation)
	}
	originalDeadline := deadline
	deadline = deadline.Add(time.Hour)
	if !reservation.Deadline.Equal(originalDeadline) {
		t.Fatal("recovery deadline aliases caller input")
	}
}

func TestDeriveQuotaPlanningStateLeavesForcedNonQuotaPauseOperatorManaged(t *testing.T) {
	input := quotaRecoveryFixture()
	for index := range input.Attempts {
		if input.Attempts[index].ID == "paused" {
			input.Attempts[index].AdminForceStart = true
		}
	}
	filtered := input.ThrottleRecords[:0]
	for _, record := range input.ThrottleRecords {
		if record.AttemptID != "paused" {
			filtered = append(filtered, record)
		}
	}
	input.ThrottleRecords = filtered
	state, err := DeriveQuotaPlanningState(input)
	if err != nil {
		t.Fatal(err)
	}
	for _, reservation := range state.ResumeReservations {
		if reservation.AttemptID == "paused" {
			t.Fatal("operator-managed pause gained automatic resume authority")
		}
	}
	for _, window := range state.QuotaWindows {
		if window.PausedRequiredWorkRemainder != 25 {
			t.Fatalf("paused remainder = %v, want 25", window.PausedRequiredWorkRemainder)
		}
	}

	for index := range input.Attempts {
		if input.Attempts[index].ID == "paused" {
			input.Attempts[index].AdminForceStart = false
		}
	}
	if _, err := DeriveQuotaPlanningState(input); err == nil || !strings.Contains(err.Error(), "no durable throttle record") {
		t.Fatalf("unforced missing throttle error = %v", err)
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

func TestDeriveQuotaPlanningStateAcceptsCanonicalPauseAfterDurableDrain(t *testing.T) {
	for _, control := range []domain.ControlState{
		domain.ControlPaused,
		domain.ControlPausedUncheckpointed,
	} {
		t.Run(string(control), func(t *testing.T) {
			input := quotaRecoveryFixture()
			for index := range input.Attempts {
				if input.Attempts[index].ID == "paused" {
					input.Attempts[index].Control = control
				}
			}
			for index := range input.ThrottleRecords {
				if input.ThrottleRecords[index].AttemptID == "paused" {
					input.ThrottleRecords[index].Control = domain.ControlDraining
				}
			}
			if _, err := DeriveQuotaPlanningState(input); err != nil {
				t.Fatalf("canonical %q after durable drain: %v", control, err)
			}
		})
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
			input.Assignments[0].Estimate = nil
		}, want: "no durable remaining-cost estimate"},
		{name: "contradictory fixed-route estimate", edit: func(input *QuotaPlanningStateInput) {
			assignment := input.Assignments[0]
			estimate := *assignment.Estimate
			estimate.RemainingCost++
			input.RouteEstimates = []RouteEstimate{{
				AttemptID: assignment.AttemptID, WorkerID: assignment.Route.WorkerID,
				ProviderInstanceID: assignment.Route.ProviderInstanceID,
				Model:              assignment.Route.Model, Options: assignment.Route.Options, Estimate: estimate,
			}}
		}, want: "durable estimate contradicts"},
		{name: "stale throttle projection", edit: func(input *QuotaPlanningStateInput) {
			input.ThrottleRecords[0].Control = domain.ControlRunning
		}, want: "contradicts latest throttle control"},
		{name: "changed worker identity", edit: func(input *QuotaPlanningStateInput) {
			input.ThrottleRecords[0].Command.WorkerID = "other-worker"
		}, want: "execution identity contradicts"},
		{name: "settled assignment", edit: func(input *QuotaPlanningStateInput) {
			input.Assignments[0].State = domain.AssignmentReleased
		}, want: "settled assignment"},
		{name: "duplicate attempt assignment", edit: func(input *QuotaPlanningStateInput) {
			duplicate := input.Assignments[0]
			duplicate.ID += "-duplicate"
			input.Assignments = append(input.Assignments, duplicate)
		}, want: "has assignments"},
		{name: "assignment for unknown attempt", edit: func(input *QuotaPlanningStateInput) {
			unknown := input.Assignments[0]
			unknown.ID = "assignment-unknown"
			unknown.AttemptID = "unknown"
			input.Assignments = append(input.Assignments, unknown)
		}, want: "names unknown attempt"},
		{name: "canonical thread mismatch", edit: func(input *QuotaPlanningStateInput) {
			input.Attempts[0].ThreadID = "other-thread"
		}, want: "canonical execution identity contradicts"},
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

func TestDeriveQuotaPlanningStateAccountsOfferedAssignmentReservation(t *testing.T) {
	input := quotaRecoveryFixture()
	estimate := domain.TaskAdmissionEstimate{
		RemainingCost: 7, ExpectedRuntime: time.Hour, CheckpointMargin: 5 * time.Minute,
	}
	input.Tasks = append(input.Tasks, domain.Task{ID: "task-offered", Class: domain.TaskClassRequired})
	input.Attempts = append(input.Attempts, domain.Attempt{
		ID: "offered", TaskID: "task-offered", Progress: domain.ProgressReady,
		Control: domain.ControlUnassigned, AssignmentID: "assignment-offered",
	})
	input.Assignments = append(input.Assignments, domain.Assignment{
		ID: "assignment-offered", AttemptID: "offered", WorkerID: "normandy",
		Route: domain.ProviderRoute{
			WorkerID: "normandy", ProviderInstanceID: "codex", Model: "gpt-5.6-sol",
			QuotaPoolID: "shared",
		},
		Estimate: &estimate, State: domain.AssignmentOffered,
	})
	state, err := DeriveQuotaPlanningState(input)
	if err != nil {
		t.Fatal(err)
	}
	for _, window := range state.QuotaWindows {
		if window.ActiveConsumption != 13 ||
			window.PausedRequiredWorkRemainder != 35 ||
			window.CommittedReservations != 7 {
			t.Fatalf("assignment accounting window = %#v", window)
		}
	}
}

func TestDeriveQuotaPlanningStateAllowsCompletedAssignmentDuringVerification(t *testing.T) {
	input := quotaRecoveryFixture()
	for index := range input.Attempts {
		if input.Attempts[index].ID == "running" {
			input.Attempts[index].Progress = domain.ProgressVerifying
			input.Attempts[index].Control = domain.ControlStopped
		}
	}
	for index := range input.Assignments {
		if input.Assignments[index].AttemptID == "running" {
			input.Assignments[index].State = domain.AssignmentCompleted
		}
	}
	if _, err := DeriveQuotaPlanningState(input); err != nil {
		t.Fatalf("completed assignment awaiting result import blocked planning: %v", err)
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
		estimate := domain.TaskAdmissionEstimate{
			RemainingCost: item.cost, ExpectedRuntime: time.Hour, CheckpointMargin: 5 * time.Minute,
		}
		input.Tasks = append(input.Tasks, domain.Task{ID: taskID, Class: item.class})
		input.Attempts = append(input.Attempts, domain.Attempt{
			ID: item.id, TaskID: taskID, Progress: domain.ProgressActive,
			Control: item.control, AssignmentID: assignmentID, ThreadID: "thread-" + item.id,
		})
		input.Assignments = append(input.Assignments, domain.Assignment{
			ID: assignmentID, AttemptID: item.id, WorkerID: "normandy", Route: route,
			Estimate: &estimate, State: domain.AssignmentClaimed, Epoch: 7,
			ThreadID: "thread-" + item.id,
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
