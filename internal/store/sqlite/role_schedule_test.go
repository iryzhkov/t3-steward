package sqlite

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestScheduledRoleSelectionsPersistAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store := openScheduleTriggerStore(t, path, domain.ScheduleFailureNextCycle, nil)
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{Tasks: []domain.Task{{ID: "role-task", WorkflowID: "workflow-1", Name: "execute", Role: "execute"}}}); err != nil {
		t.Fatal(err)
	}
	request := scheduleTriggerRequest("trigger-role", "run-role", scheduleTriggerTestTime)
	selection := domain.RoleSelection{Role: "execute", Route: "codex/gpt", Effort: "low", PolicyDigest: "first", ResolvedAt: scheduleTriggerTestTime}
	request.RouteSelections = map[string]domain.RoleSelection{"role-task": selection}
	first, err := store.CommitScheduleTrigger(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.WorkflowRun == nil || first.WorkflowRun.RouteSelections["role-task"].PolicyDigest != "first" {
		t.Fatalf("first = %#v", first)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = openMigratedFixture(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	request.RouteSelections["role-task"] = domain.RoleSelection{Role: "execute", Route: "claudeAgent/opus", Effort: "medium", PolicyDigest: "second"}
	request.RoleResolutionError = "new policy unavailable"
	replay, err := store.CommitScheduleTrigger(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Replay || replay.WorkflowRun.RouteSelections["role-task"].PolicyDigest != "first" {
		t.Fatalf("replay = %#v", replay)
	}
	records, err := store.LoadCoordinatorRecordsWithAudit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	run := records.WorkflowRuns[0]
	tasks := domain.TasksForRun(run, records.Tasks)
	if len(tasks) != 1 || len(tasks[0].Routes) != 1 || tasks[0].Routes[0].ProviderInstanceID != "codex" || tasks[0].Routes[0].Options["effort"] != "low" {
		t.Fatalf("persisted overlay = %#v", tasks)
	}
	if len(records.Tasks[0].Routes) != 0 {
		t.Fatal("shared template acquired route")
	}
	graphs, err := store.LoadGraphRevisions(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(graphs) != 1 || len(graphs[0].Tasks[0].Routes) != 1 {
		t.Fatalf("initial graph lacks route: %#v", graphs)
	}
	run.Progress = domain.ProgressSucceeded
	run.Revision++
	if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}}); err != nil {
		t.Fatal(err)
	}
	request.RoleResolutionError = ""
	request.TriggerID = "trigger-second"
	request.WorkflowRunID = "run-second"
	request.NominalAt = request.NominalAt.Add(time.Hour)
	second, err := store.CommitScheduleTrigger(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if second.WorkflowRun == nil || second.WorkflowRun.RouteSelections["role-task"].PolicyDigest != "second" {
		t.Fatalf("second = %#v", second)
	}
}

func TestScheduledRoleWithoutSelectionSuppressesAndAudits(t *testing.T) {
	for _, detail := range []string{"", "policy missing"} {
		t.Run(detail, func(t *testing.T) {
			store := openScheduleTriggerStore(t, filepath.Join(t.TempDir(), "state.db"), domain.ScheduleFailureNextCycle, nil)
			defer store.Close()
			if err := store.SaveCoordinatorRecords(context.Background(), CoordinatorRecords{Tasks: []domain.Task{{ID: "role-task", WorkflowID: "workflow-1", Role: "execute"}}}); err != nil {
				t.Fatal(err)
			}
			request := scheduleTriggerRequest("unresolved", "no-run", scheduleTriggerTestTime)
			request.RoleResolutionError = detail
			result, err := store.CommitScheduleTrigger(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if result.Trigger.State != domain.TriggerSuppressed || result.Trigger.Reason != "role-unresolved" || result.WorkflowRun != nil {
				t.Fatalf("result=%#v", result)
			}
			records, err := store.LoadCoordinatorRecordsWithAudit(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(records.WorkflowRuns) != 0 || len(records.Attempts) != 0 || len(records.AuditEvents) != 1 {
				t.Fatalf("records=%#v", records)
			}
			want := detail
			if want == "" {
				want = "role-task"
			}
			if !strings.Contains(string(records.AuditEvents[0].Detail), want) {
				t.Fatalf("audit missing detail: %s", records.AuditEvents[0].Detail)
			}
		})
	}
}
