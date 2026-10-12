package main

import (
	"context"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func triageUnknownAssignments(ctx context.Context, report *triageReport, sources triageSources, runs []backlogadmin.WorkflowSummary, threshold time.Duration) {
	if threshold <= 0 {
		threshold = 10 * time.Minute
	}
	seen := map[string]bool{}
	for _, run := range runs {
		response, err := sources.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryWorkflow, WorkflowRunID: run.Run.ID})
		if err == nil && response.Workflow == nil {
			err = fmt.Errorf("missing workflow detail")
		}
		if err != nil {
			report.unavailable("assignments of "+run.Run.ID, err)
			continue
		}
		for _, task := range response.Workflow.Tasks {
			a := task.Assignment
			if a == nil || a.State != domain.AssignmentUnknown {
				continue
			}
			subject := fmt.Sprintf("%s/%d", a.ID, a.Epoch)
			if seen[subject] {
				continue
			}
			seen[subject] = true
			age := "age unavailable"
			var since *time.Time
			if a.UnknownSince != nil && !a.UnknownSince.IsZero() && !a.UnknownSince.After(report.GeneratedAt) {
				elapsed := report.GeneratedAt.Sub(*a.UnknownSince)
				if elapsed <= threshold {
					continue
				}
				age, since = "unknown for "+elapsed.String(), a.UnknownSince
			}
			report.add(triageItem{Kind: "assignment-unknown", Severity: "action", Subject: subject, Run: run.Run.ID, Since: since,
				Summary:  fmt.Sprintf("run %s task %s assignment %s worker %s epoch %d: %s; recovery requires reviewed stop/custody evidence", run.Run.ID, task.Task.ID, a.ID, a.WorkerID, a.Epoch, age),
				Commands: []triageCommand{{Run: "t3-steward backlog show " + shellQuote(run.Run.ID)}, {Run: "t3-steward backlog recover --help"}},
			})
		}
	}
}
