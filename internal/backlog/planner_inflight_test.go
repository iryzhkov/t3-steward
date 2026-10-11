package backlog

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestBuildPlanPrefersInFlightWithBoundedDeferrals(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*PlanInput)
		want   string
	}{
		{name: "in-flight before older deferred fresh task", want: "run-z/review"},
		{name: "below bound retains preference", mutate: func(input *PlanInput) {
			input.Ordering.Attempts["run-a-implement-1"] = PlanningAttemptOrdering{
				ReadySince: plannerTestTime.Add(-time.Hour), PriorDeferrals: InFlightDeferralBound - 1,
			}
		}, want: "run-z/review"},
		{name: "bound restores fairness", mutate: func(input *PlanInput) {
			input.Ordering.Attempts["run-a-implement-1"] = PlanningAttemptOrdering{
				ReadySince: plannerTestTime.Add(-time.Hour), PriorDeferrals: InFlightDeferralBound,
			}
		}, want: "run-a/implement"},
		{name: "above bound restores fairness", mutate: func(input *PlanInput) {
			input.Ordering.Attempts["run-a-implement-1"] = PlanningAttemptOrdering{
				ReadySince: plannerTestTime.Add(-time.Hour), PriorDeferrals: InFlightDeferralBound + 1,
			}
		}, want: "run-a/implement"},
		{name: "higher importance precedes in-flight", mutate: func(input *PlanInput) {
			input.Workflows[0].State.Tasks[0].Importance = 5
		}, want: "run-a/implement"},
		{name: "deadline risk precedes in-flight", mutate: func(input *PlanInput) {
			task := &input.Workflows[0].State.Tasks[0]
			task.Class = domain.TaskClassRequired
			deadline := plannerTestTime.Add(time.Minute)
			task.Deadline = &deadline
		}, want: "run-a/implement"},
		{name: "deadline slack precedes in-flight", mutate: func(input *PlanInput) {
			for i := range input.Workflows {
				task := &input.Workflows[i].State.Tasks[0]
				task.Class = domain.TaskClassRequired
				deadline := plannerTestTime.Add(time.Duration(i+1) * time.Minute)
				task.Deadline = &deadline
			}
		}, want: "run-a/implement"},
		{name: "both in-flight keep fairness", mutate: func(input *PlanInput) {
			input.Workflows[0] = inFlightTestWorkflow("run-a", "implement", domain.ProgressSucceeded)
		}, want: "run-a/implement"},
		{name: "failed task is not in-flight", mutate: func(input *PlanInput) {
			input.Workflows[1].State.Attempts[1].Progress = domain.ProgressFailed
		}, want: "run-a/implement"},
		{name: "skipped task is not in-flight", mutate: func(input *PlanInput) {
			input.Workflows[1].State.Attempts[1].Progress = domain.ProgressSkipped
		}, want: "run-a/implement"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fresh := fairnessWorkflow("run-a", "implement")
			flight := inFlightTestWorkflow("run-z", "review", domain.ProgressSucceeded)
			input := fairnessPlanInput(fresh, flight)
			input.Ordering.Attempts["run-a-implement-1"] = PlanningAttemptOrdering{
				ReadySince: plannerTestTime.Add(-time.Hour), PriorDeferrals: 1,
			}
			if test.mutate != nil {
				test.mutate(&input)
			}
			// Mutations may replace a workflow; retain contention to prove dispatch.
			for i := range input.Workflows {
				input.Workflows[i].State.Tasks[0].ResourceLocks = []string{"shared"}
			}
			got, err := BuildPlan(input)
			if err != nil {
				t.Fatal(err)
			}
			if order := proposalOrder(got); !reflect.DeepEqual(order, []string{test.want}) {
				t.Fatalf("proposals = %v, want %s", order, test.want)
			}
			for _, decision := range got.Decisions {
				wantFlight := false
				for _, workflow := range input.Workflows {
					if workflow.State.Run.ID == decision.WorkflowRunID {
						for _, attempt := range workflow.State.Attempts {
							wantFlight = wantFlight || attempt.Progress == domain.ProgressSucceeded
						}
					}
				}
				if decision.Order.InFlight != wantFlight {
					t.Fatalf("order = %#v, in-flight want %v", decision.Order, wantFlight)
				}
				if decision.Proposed && wantFlight &&
					!strings.Contains(decision.Order.Reason, "in-flight preference") {
					t.Fatalf("reason = %q", decision.Order.Reason)
				}
				if decision.Order.PriorDeferrals >= InFlightDeferralBound &&
					!strings.Contains(decision.Order.Reason, "bypassed at deferral bound") {
					t.Fatalf("bounded reason = %q", decision.Order.Reason)
				}
			}
			input.Workflows[0], input.Workflows[1] = input.Workflows[1], input.Workflows[0]
			reversed, err := BuildPlan(input)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, reversed) {
				t.Fatal("plan depends on workflow input order")
			}
		})
	}
}

func TestPlanningInFlightBoundKeepsComparatorTransitive(t *testing.T) {
	var entries []planningTaskEntry
	for _, inFlight := range []bool{false, true} {
		for _, deferrals := range []int{0, InFlightDeferralBound - 1, InFlightDeferralBound, InFlightDeferralBound + 1} {
			entries = append(entries, planningTaskEntry{ready: true, order: PlanningOrder{
				InFlight: inFlight, PriorDeferrals: deferrals, ReadySince: plannerTestTime,
			}})
		}
	}
	for _, a := range entries {
		for _, b := range entries {
			if comparePlanningPriority(a, b) != -comparePlanningPriority(b, a) {
				t.Fatal("comparator is not antisymmetric")
			}
			for _, c := range entries {
				if comparePlanningPriority(a, b) < 0 && comparePlanningPriority(b, c) < 0 &&
					comparePlanningPriority(a, c) >= 0 {
					t.Fatalf("non-transitive order: %#v, %#v, %#v", a.order, b.order, c.order)
				}
			}
		}
	}
}

func inFlightTestWorkflow(runID, readyName string, progress domain.ProgressState) PlanningWorkflow {
	workflow := fairnessWorkflow(runID, readyName, "completed")
	workflow.State.Attempts[1].Progress = progress
	return workflow
}
