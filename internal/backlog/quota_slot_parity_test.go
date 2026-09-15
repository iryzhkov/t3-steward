package backlog

import (
	"context"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// slotParityInput is one attempt with its task and assignment, shaped so that
// both the enabled and the disabled quota path can reconstruct occupancy from
// it.
func slotParityInput(control domain.ControlState, progress domain.ProgressState, state domain.AssignmentState) QuotaPlanningStateInput {
	return QuotaPlanningStateInput{
		Tasks: []domain.Task{{ID: "alpha", Class: domain.TaskClassRequired}},
		Attempts: []domain.Attempt{{
			ID: "alpha-1", TaskID: "alpha", AssignmentID: "assignment-1",
			Progress: progress, Control: control,
		}},
		Assignments: []domain.Assignment{{
			ID: "assignment-1", AttemptID: "alpha-1", State: state,
			Route: domain.ProviderRoute{QuotaPoolID: "pool"},
		}},
	}
}

// TestProviderSlotOccupancyIsTheSameWithAndWithoutQuotaChecks pins the two
// paths to one answer.
//
// The disabled path counted every assignment that was not completed or
// released, ignoring whether its attempt still held a provider slot. On a
// one-slot pool that kept the slot for the whole of an external wait: a second
// campaign on another worker stayed ready and unassigned while explain called
// the task eligible, and an operator had to debug that contradiction from
// scratch.
//
// The disabled path is the one that actually runs. A synthetic provider
// produces no quota observations, so enabling checks closes admission
// entirely, which means every disposable test and every quota-free deployment
// takes the branch nobody was checking.
func TestProviderSlotOccupancyIsTheSameWithAndWithoutQuotaChecks(t *testing.T) {
	tests := []struct {
		name     string
		control  domain.ControlState
		progress domain.ProgressState
		state    domain.AssignmentState
		want     int
	}{
		{
			name:    "a running attempt holds its slot",
			control: domain.ControlRunning, progress: domain.ProgressActive,
			state: domain.AssignmentClaimed, want: 1,
		},
		{
			name:    "a preparing attempt holds its slot",
			control: domain.ControlPreparing, progress: domain.ProgressActive,
			state: domain.AssignmentOffered, want: 1,
		},
		{
			name:    "a draining attempt still holds its slot",
			control: domain.ControlDraining, progress: domain.ProgressActive,
			state: domain.AssignmentClaimed, want: 1,
		},
		{
			// The H4 contract: the provider slot is released while waiting.
			name:    "an attempt parked on an external wait releases its slot",
			control: domain.ControlWaitingExternal, progress: domain.ProgressWaitingExternal,
			state: domain.AssignmentClaimed, want: 0,
		},
		{
			name:    "a paused attempt releases its slot",
			control: domain.ControlPaused, progress: domain.ProgressActive,
			state: domain.AssignmentClaimed, want: 0,
		},
		{
			name:    "an unassigned attempt holds nothing",
			control: domain.ControlUnassigned, progress: domain.ProgressReady,
			state: domain.AssignmentClaimed, want: 0,
		},
		{
			name:    "a released assignment holds nothing",
			control: domain.ControlRunning, progress: domain.ProgressActive,
			state: domain.AssignmentReleased, want: 0,
		},
		{
			name:    "a completed assignment holds nothing",
			control: domain.ControlRunning, progress: domain.ProgressActive,
			state: domain.AssignmentCompleted, want: 0,
		},
		{
			name:    "a terminal attempt holds nothing",
			control: domain.ControlUnassigned, progress: domain.ProgressSucceeded,
			state: domain.AssignmentClaimed, want: 0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := slotParityInput(test.control, test.progress, test.state)
			bindings := []QuotaPoolBinding{{
				ID: "pool", Provider: "codex",
				ProviderInstanceIDs: []string{"codex"}, MaxConcurrent: 2,
			}}

			disabled := QuotaBridge{
				Disabled: true, Pools: bindings,
				Now: func() time.Time { return plannerTestTime },
			}
			report, err := disabled.ReconcileState(context.Background(), input)
			if err != nil {
				t.Fatalf("disabled path: %v", err)
			}
			if len(report.Pools) != 1 {
				t.Fatalf("disabled pools = %+v", report.Pools)
			}

			// The enabled path's own derivation, given the same snapshot and the
			// same pools. Comparing against it rather than against a second copy
			// of the rule is what keeps the two from drifting again.
			pools, _, err := quotaBridgePools(bindings)
			if err != nil {
				t.Fatal(err)
			}
			enabledInput := input
			enabledInput.QuotaPools = pools
			state, err := DeriveQuotaPlanningState(enabledInput)
			if err != nil {
				t.Fatalf("enabled path: %v", err)
			}
			if len(state.QuotaPools) != 1 {
				t.Fatalf("enabled pools = %+v", state.QuotaPools)
			}

			if report.Pools[0].ActiveAssignments != state.QuotaPools[0].ActiveAssignments {
				t.Fatalf("occupancy disagrees: disabled %d, enabled %d",
					report.Pools[0].ActiveAssignments, state.QuotaPools[0].ActiveAssignments)
			}
			if report.Pools[0].ActiveAssignments != test.want {
				t.Fatalf("occupancy = %d, want %d", report.Pools[0].ActiveAssignments, test.want)
			}
		})
	}
}

// TestAParkedAttemptFreesItsPoolForOtherWork is the observed symptom, stated as
// a property: with one slot and one parked attempt, another task can still be
// planned onto the pool.
func TestAParkedAttemptFreesItsPoolForOtherWork(t *testing.T) {
	bridge := QuotaBridge{
		Disabled: true,
		Pools: []QuotaPoolBinding{{
			ID: "pool", Provider: "codex",
			ProviderInstanceIDs: []string{"codex"}, MaxConcurrent: 1,
		}},
		Now: func() time.Time { return plannerTestTime },
	}
	report, err := bridge.ReconcileState(context.Background(),
		slotParityInput(domain.ControlWaitingExternal, domain.ProgressWaitingExternal, domain.AssignmentClaimed))
	if err != nil {
		t.Fatal(err)
	}
	if report.Pools[0].ActiveAssignments != 0 {
		t.Fatalf("a parked attempt still occupies %d of one slot", report.Pools[0].ActiveAssignments)
	}

	// The second campaign, on the other worker, can now be planned.
	policy, err := NewQuotaAdmissionPolicy(QuotaAdmissionInput{Disabled: true, MaxObservationAge: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	task := routingTask("beta", "codex", "gpt")
	input := plannerInput([]domain.Task{task},
		[]domain.WorkerInventory{routingWorker("omarchy-pc", routingProvider("codex", "pool", true, "gpt"))})
	input.QuotaPools = report.Pools
	input.RouteEstimates = []RouteEstimate{routingEstimate("beta-1", "omarchy-pc", "codex", "gpt", nil, 1000)}
	input.Constraints = []PlanningConstraint{policy}
	plan, err := BuildPlan(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Proposals) != 1 {
		t.Fatalf("a parked attempt blocked the pool: proposals = %d", len(plan.Proposals))
	}
}
