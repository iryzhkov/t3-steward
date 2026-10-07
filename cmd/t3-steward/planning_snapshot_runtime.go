package main

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Retain the actual successful pass, including passes proposing no assignments.
// Failure leaves the previous evidence available, subject to its staleness bound.
func (p coordinatorPlanner) planAndRecord(ctx context.Context, input backlog.PlanInput, attempts []domain.Attempt) (backlog.AssignmentPlanningReport, error) {
	report, err := p.coordinator.PlanAndCommit(ctx, input)
	if err == nil && p.planningSnapshot != nil {
		p.planningSnapshot.Record(report.Plan, attempts, input.Now)
	}
	return report, err
}
