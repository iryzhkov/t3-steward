package main

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCoordinatorPlannerPublishesSuccessfulNoAssignmentPass(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitetest.OpenMigrated(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	epoch, err := store.AcquireCoordinator(ctx, "normandy")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 10, 21, 0, 0, 0, time.UTC)
	cost := 9.0
	records := sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: "workflow", Version: 1, Name: "workflow", Project: "project",
			Class: domain.TaskClassRequired, TaskIDs: []string{"task"}, CreatedAt: now,
		}},
		WorkflowRuns: []domain.WorkflowRun{{
			ID: "run", WorkflowID: "workflow", Progress: domain.ProgressActive,
			Revision: 1, CreatedAt: now, UpdatedAt: now,
		}},
		Tasks: []domain.Task{{
			ID: "task", WorkflowID: "workflow", Name: "task", Class: domain.TaskClassRequired,
			Routes: []domain.ProviderRoute{{
				ProviderInstanceID: "codex", Model: "gpt", QuotaPoolID: "pool",
			}},
			Importance: 5, Difficulty: 3, EstimatedCost: &cost, MaxTurns: 2,
		}},
		Attempts: []domain.Attempt{{
			ID: "attempt", WorkflowRunID: "run", TaskID: "task", Number: 1,
			Progress: domain.ProgressReady, Control: domain.ControlUnassigned,
			Revision: 1, UpdatedAt: now,
		}},
	}
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	snapshot := domain.WorkerSnapshot{
		WorkerID: "worker", WorkerEpoch: "worker-epoch", CoordinatorEpoch: epoch,
		Sequence: 1, Connected: true,
		Inventory: domain.WorkerInventory{
			ID: "worker", AcceptBacklog: false, Health: domain.WorkerHealthReady,
			Projects: []domain.WorkerProjectInventory{{
				Name: "project", Available: true, UpdatedAt: now,
			}},
			Providers: []domain.WorkerProviderInventory{{
				InstanceID: "codex", Models: []string{"gpt"}, QuotaPoolID: "pool", Available: true,
			}},
			ObservedAt: now,
		},
		ObservedAt: now, ValidUntil: now.Add(time.Hour),
	}
	if err := store.SaveWorkerSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	quota := backlog.QuotaBridgeReport{
		Pools: []domain.QuotaPool{{
			ID: "pool", Provider: "openai", ProviderInstanceIDs: []string{"codex"},
			Admission: domain.AdmissionOpen, MaxConcurrent: 2,
		}},
		Windows: []backlog.QuotaWindowBudget{{
			QuotaPoolID: "pool", WindowID: "primary", ObservedAt: now,
			Admission: domain.AdmissionOpen, Capacity: 100, ResetsAt: now.Add(time.Hour),
		}},
	}
	holder := backlogadmin.NewPlanningSnapshotHolder(time.Minute)
	service, err := backlogadmin.New(store, localAdminAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return now })
	service.SetPlanningSnapshotHolder(holder)
	planner := coordinatorPlanner{
		planningSnapshot: holder, store: store, coordinator: backlog.FleetCoordinator{Store: store}, epoch: epoch,
		maxWorkerSnapshotAge: time.Hour, maxQuotaObservationAge: 5 * time.Minute,
		deadlineRiskWindow: time.Hour, checkpointMargin: 5 * time.Minute,
		now: func() time.Time { return now },
	}

	explain := func() backlogadmin.Explanation {
		t.Helper()
		response, err := service.Query(ctx, backlogadmin.Query{
			Version: backlogadmin.CurrentReadVersion, Kind: backlogadmin.QueryExplanation,
			Principal:     backlogadmin.Principal{ID: "local:1000", Roles: []string{backlogadmin.LocalAdminRole}},
			WorkflowRunID: "run", TaskID: "task",
		})
		if err != nil {
			t.Fatal(err)
		}
		if response.Explanation == nil {
			t.Fatal("missing explanation")
		}
		return *response.Explanation
	}
	before := explain()
	if !strings.Contains(before.Summary, "no planning pass yet") {
		t.Fatalf("before planning: %#v", before)
	}
	report, err := planner.Tick(ctx, quota)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Assignments) != 0 || len(report.Plan.Proposals) != 0 || len(report.Plan.Decisions) != 1 {
		t.Fatalf("expected successful no-assignment pass: %#v", report)
	}
	first := explain()
	if first.Eligible || !strings.Contains(first.Summary, "not assigned in the last planning pass (0s ago)") {
		t.Fatalf("successful no-assignment pass was not published: %#v", first)
	}
	decision := report.Plan.Decisions[0]
	blockers := append([]backlog.PlanningBlocker(nil), decision.Blockers...)
	for _, candidate := range decision.Candidates {
		for _, blocker := range candidate.Blockers {
			if blocker.WorkerID == "" {
				blocker.WorkerID = candidate.WorkerID
			}
			blockers = append(blockers, blocker)
		}
	}
	if len(blockers) == 0 {
		t.Fatal("fixture produced no planner blockers")
	}
	for _, want := range blockers {
		found := false
		for _, got := range first.Blockers {
			if got.Code == want.Code && got.Detail == want.Detail && got.WorkerID == want.WorkerID && got.QuotaPoolID == want.QuotaPoolID {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("planner blocker %#v missing from explanation %#v", want, first)
		}
	}
	now = now.Add(30 * time.Second)
	if _, err := planner.Tick(ctx, quota); err != nil {
		t.Fatal(err)
	}
	second := explain()
	if !strings.Contains(second.Summary, "(0s ago)") {
		t.Fatalf("second successful pass did not refresh snapshot: %#v", second)
	}
	now = now.Add(10 * time.Second)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := planner.Tick(canceled, quota); err == nil {
		t.Fatal("canceled planning pass succeeded")
	}
	afterFailure := explain()
	if !strings.Contains(afterFailure.Summary, "(10s ago)") || !reflect.DeepEqual(second.Blockers, afterFailure.Blockers) {
		t.Fatalf("failed pass replaced successful snapshot: before=%#v after=%#v", second, afterFailure)
	}
}
