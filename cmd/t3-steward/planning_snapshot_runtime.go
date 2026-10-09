package main

import (
	"context"
	"log/slog"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// Retain the actual successful pass, including passes proposing no assignments.
// Failure leaves the previous evidence available, subject to its staleness bound.
func (p coordinatorPlanner) planAndRecord(ctx context.Context, input backlog.PlanInput, attempts []domain.Attempt) (backlog.AssignmentPlanningReport, error) {
	report, err := p.coordinator.PlanAndCommit(ctx, input)
	if err == nil && p.planningSnapshot != nil {
		p.annotateCapacityDeadlocks(ctx, &report.Plan)
		p.planningSnapshot.Record(report.Plan, attempts, input.Now)
	}
	return report, err
}

// annotateCapacityDeadlocks names, in the recorded explanation, every ready
// task whose only obstacle is capacity held by attempts waiting on its own run.
// It changes no record and commits nothing; a pass whose plan has no task
// blocked by capacity alone reads nothing more.
func (p coordinatorPlanner) annotateCapacityDeadlocks(ctx context.Context, plan *backlog.Plan) {
	if p.store == nil || !backlog.PlanHasCapacityOnlyBlocks(*plan) {
		return
	}
	var records sqlite.CoordinatorRecords
	waits, err := p.store.ListTaskWaits(ctx)
	if err == nil {
		records, err = p.store.LoadCoordinatorRecords(ctx)
	}
	if err != nil {
		slog.Warn("capacity deadlock detection skipped", "error", err)
		return
	}
	backlog.AnnotateCapacityDeadlocks(plan, backlog.CapacityDeadlockInput{
		WorkflowRuns: records.WorkflowRuns, Tasks: records.Tasks, Attempts: records.Attempts,
		Assignments: records.Assignments, TaskWaits: waits,
	})
}
