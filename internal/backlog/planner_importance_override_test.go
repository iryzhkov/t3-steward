package backlog

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestPlanningImportanceOverridePreservesPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name        string
		left, right int
		want        string
	}{
		{"override", 5, 1, "run-z-beta-1"},
		{"nil legacy fallback", 0, 0, "run-a-alpha-1"},
		{"equal effective run tie", 5, 5, "run-a-alpha-1"},
		{"ready", 1, 5, "run-z-beta-1"},
		{"forced", 1, 5, "run-z-beta-1"},
		{"risk", 1, 5, "run-z-beta-1"},
		{"slack", 1, 5, "run-z-beta-1"},
		{"in-flight", 5, 5, "run-z-beta-1"},
		{"bound", 5, 5, "run-a-alpha-1"},
		{"deferrals", 5, 5, "run-z-beta-1"},
		{"age", 5, 5, "run-z-beta-1"},
		{"name and round", 5, 5, "run-a-alpha-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := fairnessPlanInput(fairnessWorkflow("run-z", "beta", "aardvark"), fairnessWorkflow("run-a", "alpha", "blocker"))
			left, right := &input.Workflows[0].State, &input.Workflows[1].State
			left.Tasks[0].Importance, right.Tasks[0].Importance = 2, 3
			left.Attempts[1].Progress, right.Attempts[1].Progress = domain.ProgressSkipped, domain.ProgressSkipped
			for i, value := range []int{tc.left, tc.right} {
				if value != 0 {
					input.Workflows[i].State.Attempts[0].AdminImportance = &value
				}
			}
			switch tc.name {
			case "nil legacy fallback":
				right.Tasks[0].Importance = 10
			case "ready":
				right.Tasks[0].Needs = []string{"blocker"}
				right.Attempts[1].Progress = domain.ProgressActive
			case "forced":
				left.Attempts[0].AdminForceStart = true
			case "risk", "slack":
				deadline := plannerTestTime.Add(time.Minute)
				left.Tasks[0].Class, left.Tasks[0].Deadline = domain.TaskClassRequired, &deadline
				if tc.name == "slack" {
					later := deadline.Add(time.Minute)
					right.Tasks[0].Class, right.Tasks[0].Deadline = domain.TaskClassRequired, &later
				}
			case "in-flight", "bound":
				left.Attempts[1].Progress = domain.ProgressSucceeded
				deferrals := InFlightDeferralBound - 1
				if tc.name == "bound" {
					deferrals++
				}
				input.Ordering.Attempts[right.Attempts[0].ID] = PlanningAttemptOrdering{ReadySince: plannerTestTime, PriorDeferrals: deferrals}
			case "deferrals":
				input.Ordering.Attempts[left.Attempts[0].ID] = PlanningAttemptOrdering{ReadySince: plannerTestTime, PriorDeferrals: 1}
			case "age":
				input.Ordering.Attempts[left.Attempts[0].ID] = PlanningAttemptOrdering{ReadySince: plannerTestTime.Add(-time.Hour), NestedUnder: "parent"}
			case "name and round":
				left.Attempts[1].Progress = domain.ProgressReady
				left.Attempts[1].AdminImportance = &tc.left
			}
			before := cloneDAGState(*left)
			got, err := BuildPlan(input)
			if err != nil {
				t.Fatal(err)
			}
			positions := map[string]int{}
			for i, decision := range got.Decisions {
				positions[decision.AttemptID] = i
				want := map[string]int{"run-z-beta-1": tc.left, "run-a-alpha-1": tc.right, "run-z-aardvark-1": tc.left}[decision.AttemptID]
				if tc.name == "nil legacy fallback" {
					want = map[string]int{"run-z-beta-1": 2, "run-a-alpha-1": 10}[decision.AttemptID]
				}
				if decision.Order.Importance != want || !strings.Contains(decision.Order.Reason, fmt.Sprintf("importance %d;", want)) {
					t.Fatalf("effective order: %#v, want %d", decision.Order, want)
				}
			}
			other := "run-z-beta-1"
			if tc.want == other {
				other = "run-a-alpha-1"
			}
			win, winOK := positions[tc.want]
			lose, loseOK := positions[other]
			if !winOK || !loseOK || win >= lose {
				t.Fatalf("pair ordering: %v", positions)
			}
			if tc.name == "name and round" && !(positions["run-z-aardvark-1"] < positions["run-z-beta-1"] && positions["run-a-alpha-1"] < positions["run-z-beta-1"]) {
				t.Fatalf("name/round ordering: %v", positions)
			}
			if !reflect.DeepEqual(before, cloneDAGState(*left)) {
				t.Fatal("planner mutated input state")
			}
			slices.Reverse(input.Workflows)
			for i := range input.Workflows {
				slices.Reverse(input.Workflows[i].State.Tasks)
				slices.Reverse(input.Workflows[i].State.Attempts)
			}
			permuted, err := BuildPlan(input)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, permuted) {
				t.Fatal("plan depends on input permutation")
			}
		})
	}
}
