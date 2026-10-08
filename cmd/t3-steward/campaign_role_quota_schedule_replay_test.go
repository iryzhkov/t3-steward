package main

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestScheduleRoleQuotaDurableReplayAndGatedWaiting(t *testing.T) {
	ctx := context.Background()
	admin, store, gate, setUsed := campaignRankedQuotaFixture(t)
	seed := sqlite.CoordinatorRecords{
		Schedules:         []domain.Schedule{{ID: "schedule-replay", Version: 1, WorkflowID: "workflow-replay", Enabled: true, Revision: 1}},
		ScheduleTemplates: []domain.ScheduleTemplate{{ScheduleID: "schedule-replay", Version: 1, WorkflowID: "workflow-replay", Expression: "0 2 * * *", Timezone: "UTC", Misfire: domain.ScheduleMisfireSkip, Overlap: domain.ScheduleOverlapForbid, AfterFailure: domain.ScheduleFailureNextCycle, CreatedAt: probeNow}},
		Workflows:         []domain.Workflow{{ID: "workflow-replay", Version: 2, Project: "dev-fleet", Class: domain.TaskClassRequired, CreatedAt: probeNow}},
		Tasks:             []domain.Task{{ID: "task-replay", WorkflowID: "workflow-replay", Name: "inspect", Role: "execute", Class: domain.TaskClassRequired}},
	}
	if err := store.SaveCoordinatorRecords(ctx, seed); err != nil {
		t.Fatal(err)
	}
	decorator := coordinatorRoleScheduleStore{Store: store, admin: admin}
	r := domain.ScheduleTriggerRequest{ScheduleID: "schedule-replay", TriggerID: "trigger-replay", WorkflowRunID: "run-replay", Source: domain.ScheduleTriggerScheduled, NominalAt: probeNow, ObservedAt: probeNow}
	first, err := decorator.CommitScheduleTrigger(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if first.WorkflowRun == nil || first.WorkflowRun.RouteSelections["task-replay"].Route != "codex/model" {
		t.Fatalf("first: %+v", first)
	}
	// Replay re-resolves against changed telemetry, then the occurrence transaction
	// must return its durable receipt rather than the new preference.
	setUsed("claude", 10)
	setUsed("codex", 100)
	replay, err := decorator.CommitScheduleTrigger(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Replay || replay.WorkflowRun == nil || !reflect.DeepEqual(replay.WorkflowRun.RouteSelections, first.WorkflowRun.RouteSelections) {
		t.Fatalf("replay changed receipt: first=%+v replay=%+v", first, replay)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.WorkflowRuns) != 1 || len(records.Attempts) != 1 {
		t.Fatalf("duplicate run or attempt: runs=%d attempts=%d", len(records.WorkflowRuns), len(records.Attempts))
	}
	for _, task := range records.Tasks {
		if task.WorkflowID == "workflow-replay" && (task.RoleSelection != nil || len(task.Routes) != 0) {
			t.Fatalf("template mutated: %+v", task)
		}
	}
	setUsed("claude", 100)
	r2 := r
	r2.TriggerID = "trigger-gated"
	r2.WorkflowRunID = "run-gated"
	r2.NominalAt = probeNow.Add(time.Hour)
	run := *first.WorkflowRun
	run.Progress = domain.ProgressSucceeded
	run.Revision++
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}}); err != nil {
		t.Fatal(err)
	}
	gated, err := decorator.CommitScheduleTrigger(ctx, r2)
	if err != nil {
		t.Fatal(err)
	}
	// Temporary pressure retains the existing queued-run waiting contract;
	// quota admission still rejects execution of the ranked preference.
	if gated.WorkflowRun == nil || gated.WorkflowRun.Progress != domain.ProgressQueued {
		t.Fatalf("temporary quota pressure did not preserve queued waiting: %+v", gated)
	}
	selection := gated.WorkflowRun.RouteSelections["task-replay"]
	t.Logf("gated occurrence queued route=%s ranking=%s", selection.Route, selection.Ranking)
	gate.Bridge.Now = func() time.Time { return probeNow }
	report, err := gate.Bridge.Reconcile(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := backlog.NewQuotaAdmissionPolicy(backlog.QuotaAdmissionInput{Windows: report.Windows, MaxObservationAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	route := selection.ProviderRoute()
	route.QuotaPoolID = strings.Split(selection.Route, "/")[0] + "-pool"
	blockers := policy.StartPlan(probeNow).Evaluate(backlog.PlanningCandidate{WorkflowRunID: gated.WorkflowRun.ID, Task: domain.Task{ID: "task-replay", Class: domain.TaskClassRequired}, Attempt: domain.Attempt{ID: "attempt-replay"}, Route: &route, Estimate: &backlog.TaskAdmissionEstimate{RemainingCost: 60, ExpectedRuntime: time.Minute}})
	quotaBlocked := false
	for _, b := range blockers {
		quotaBlocked = quotaBlocked || b.Code == backlog.PlanningBlockerQuotaAdmission || b.Code == backlog.PlanningBlockerQuotaCapacity
	}
	if !quotaBlocked {
		t.Fatalf("gated schedule bypassed planning admission: %+v", blockers)
	}
	t.Logf("gated occurrence blocked before execution by quota admission and capacity")
}
