package main

import (
	"context"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Failed task counts identify runs whose latest attempts can need this action.
// Read settled runs too: exhaustion ends the task, so a failed run remains
// actionable until the lead starts a new run with a fresh review budget.
func triageReviewRoundLimits(ctx context.Context, report *triageReport, sources triageSources, runs []backlogadmin.WorkflowSummary) {
	for _, summary := range runs {
		if summary.Progress.Failed == 0 && summary.Run.Progress != domain.ProgressFailed {
			continue
		}
		runID := summary.Run.ID
		response, err := sources.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryWorkflow, WorkflowRunID: runID})
		if err != nil {
			report.unavailable("review round limits of "+runID, err)
			continue
		}
		if response.Workflow == nil {
			report.unavailable("review round limits of "+runID, fmt.Errorf("workflow detail was not returned"))
			continue
		}
		for _, task := range response.Workflow.Tasks {
			attempt := task.Attempt
			if attempt == nil || attempt.Progress != domain.ProgressFailed || attempt.ReviewGate == nil {
				continue
			}
			gate := attempt.ReviewGate
			if gate.Code != domain.ReviewGateRoundLimitExhausted {
				continue
			}
			since := attempt.UpdatedAt
			if attempt.CompletedAt != nil {
				since = *attempt.CompletedAt
			}
			report.add(triageItem{
				Kind: "review-round-limit", Severity: "action", Subject: runID + "/" + task.Task.ID, Run: runID, Since: &since,
				Summary: fmt.Sprintf("task %s of run %s used %d of %d review rounds; its latest attempt failed because no further round can be opened; the lead can start a new run with a fresh round budget", task.Task.ID, runID, gate.RoundsUsed, gate.RoundLimit),
				Commands: []triageCommand{
					{Run: "t3-steward campaign show " + runID},
					{Run: "t3-steward campaign rerun " + runID + " --from " + task.Task.ID, When: "after deciding to give the task a fresh review budget"},
				},
			})
		}
	}
	report.Sources = append(report.Sources, "review round limits")
}
