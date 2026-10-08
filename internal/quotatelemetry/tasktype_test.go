package quotatelemetry

import (
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestDerivedTaskType(t *testing.T) {
	cases := []struct {
		name     string
		task     domain.Task
		attempt  domain.Attempt
		role     domain.ExecutionRole
		category string
		rule     string
	}{
		{name: "supervision activation id", task: domain.Task{Name: "implement"}, attempt: domain.Attempt{SupervisionActivationID: "act-1"},
			role: domain.ExecutionRoleExecutor, category: "supervision", rule: RuleSupervision},
		{name: "supervisor role", task: domain.Task{Name: "review"}, role: domain.ExecutionRoleSupervisorActivation,
			category: "supervision", rule: RuleSupervision},
		{name: "repair executor", task: domain.Task{Name: "implement"}, role: domain.ExecutionRoleRepairExecutor, category: "fix", rule: RuleRole},
		{name: "gate reviewer", task: domain.Task{Name: "implement"}, role: domain.ExecutionRoleGateReviewer, category: "review", rule: RuleRole},
		{name: "review judge", task: domain.Task{Name: "implement", ReviewJudge: true, ReviewOutput: &domain.ReviewOutput{}},
			role: domain.ExecutionRoleExecutor, category: "review-judge", rule: RuleReviewJudge},
		{name: "review output", task: domain.Task{Name: "swarm-3", ReviewOutput: &domain.ReviewOutput{}}, role: domain.ExecutionRoleExecutor,
			category: "review", rule: RuleReviewOutput},
		{name: "name implement", task: domain.Task{Name: "Implement-Unit_x"}, category: "implement", rule: RuleTaskName},
		{name: "name review with digits", task: domain.Task{Name: "review2"}, category: "review", rule: RuleTaskName},
		{name: "name fix with dot", task: domain.Task{Name: "fix1.round"}, category: "fix", rule: RuleTaskName},
		{name: "name gate underscore", task: domain.Task{Name: "gate_final"}, category: "gate", rule: RuleTaskName},
		{name: "name not a word", task: domain.Task{Name: "implementation"}, category: "unknown", rule: RuleNone},
		// Everything that says what this task does is in its prompt; the
		// prompt is never read, so it stays unknown.
		{name: "prompt only", task: domain.Task{Name: "step-a", PromptArtifactID: "prompt-implement-the-fix-and-review",
			Verification: []string{"go test ./..."}, Gate: &domain.TaskGate{Commands: []string{"make"}}, Class: domain.TaskClassSurplus},
			role: domain.ExecutionRoleExecutor, category: "unknown", rule: RuleNone},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DeriveTaskType(tc.task, tc.attempt, tc.role)
			if got.Category != tc.category || got.Rule != tc.rule || !got.Derived {
				t.Fatalf("DeriveTaskType = %+v; want %s by %s, derived", got, tc.category, tc.rule)
			}
			p := got.Provenance
			if p.TaskName != tc.task.Name || p.ExecutionRole != string(tc.role) || p.ReviewOutputDeclared != (tc.task.ReviewOutput != nil) ||
				p.ReviewJudge != tc.task.ReviewJudge || p.GateDeclared != (tc.task.Gate != nil) ||
				p.VerificationCount != len(tc.task.Verification) || p.Class != string(tc.task.Class) ||
				p.SupervisionActivationID != tc.attempt.SupervisionActivationID {
				t.Fatalf("provenance = %+v; want every raw input kept", p)
			}
		})
	}
}
