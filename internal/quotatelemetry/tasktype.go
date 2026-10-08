package quotatelemetry

import (
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// No manifest declares a task type, so phase 0 records a provisional category
// derived from structured fields that already exist, labelled as derived and
// carrying the raw inputs so a later rule can re-label old records. Prompts,
// prompt artifacts and any other free text are never read.

// Derivation rules, in the order they are tried.
const (
	RuleSupervision  = "supervision"
	RuleRole         = "role"
	RuleReviewJudge  = "review-judge"
	RuleReviewOutput = "review-output"
	RuleTaskName     = "name"
	RuleNone         = "none"
)

// TaskType is the derived category of one assignment's work.
type TaskType struct {
	Category   string             `json:"category"`
	Derived    bool               `json:"derived"`
	Rule       string             `json:"rule"`
	Provenance TaskTypeProvenance `json:"provenance"`
}

// TaskTypeProvenance keeps every input the rules read.
type TaskTypeProvenance struct {
	TaskName                string `json:"taskName"`
	ExecutionRole           string `json:"executionRole"`
	ReviewOutputDeclared    bool   `json:"reviewOutputDeclared"`
	ReviewJudge             bool   `json:"reviewJudge"`
	GateDeclared            bool   `json:"gateDeclared"`
	VerificationCount       int    `json:"verificationCount"`
	Class                   string `json:"class"`
	SupervisionActivationID string `json:"supervisionActivationId"`
}

// nameCategories are the chain positions a task name can declare.
var nameCategories = map[string]bool{"implement": true, "review": true, "fix": true, "gate": true}

// DeriveTaskType applies the first matching rule:
//
//  1. a supervision activation id, or role supervisor-activation: supervision;
//  2. role repair-executor: fix; role gate-reviewer: review;
//  3. a review judge: review-judge;
//  4. a declared review output: review;
//  5. the task name, lowercased, cut at the first '-', '_' or '.', with
//     trailing digits removed, when it is implement, review, fix or gate;
//  6. otherwise unknown.
func DeriveTaskType(task domain.Task, attempt domain.Attempt, role domain.ExecutionRole) TaskType {
	result := TaskType{Derived: true, Provenance: TaskTypeProvenance{
		TaskName: task.Name, ExecutionRole: string(role),
		ReviewOutputDeclared: task.ReviewOutput != nil, ReviewJudge: task.ReviewJudge,
		GateDeclared: task.Gate != nil, VerificationCount: len(task.Verification),
		Class: string(task.Class), SupervisionActivationID: attempt.SupervisionActivationID,
	}}
	switch {
	case attempt.SupervisionActivationID != "" || role == domain.ExecutionRoleSupervisorActivation:
		result.Category, result.Rule = "supervision", RuleSupervision
	case role == domain.ExecutionRoleRepairExecutor:
		result.Category, result.Rule = "fix", RuleRole
	case role == domain.ExecutionRoleGateReviewer:
		result.Category, result.Rule = "review", RuleRole
	case task.ReviewJudge:
		result.Category, result.Rule = "review-judge", RuleReviewJudge
	case task.ReviewOutput != nil:
		result.Category, result.Rule = "review", RuleReviewOutput
	default:
		if word := taskNameWord(task.Name); nameCategories[word] {
			result.Category, result.Rule = word, RuleTaskName
		} else {
			result.Category, result.Rule = "unknown", RuleNone
		}
	}
	return result
}

func taskNameWord(name string) string {
	word := strings.ToLower(name)
	if cut := strings.IndexAny(word, "-_."); cut >= 0 {
		word = word[:cut]
	}
	return strings.TrimRight(word, "0123456789")
}
