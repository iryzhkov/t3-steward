package backlogadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type planningReadAuthorizer struct{}

func (planningReadAuthorizer) Authorize(context.Context, Principal, Action) error { return nil }

func planningExplainFixture() (explainReader, backlog.PlanInput) {
	task := domain.Task{ID: "task", Name: "work", WorkflowID: "wf", Class: domain.TaskClassRequired, Routes: []domain.ProviderRoute{{ProviderInstanceID: "claude", Model: "opus"}}}
	attempt := domain.Attempt{ID: "attempt", WorkflowRunID: "run", TaskID: "task", Number: 1, Revision: 1, Progress: domain.ProgressReady, Control: domain.ControlUnassigned}
	wf := domain.Workflow{ID: "wf", Project: "project", Class: domain.TaskClassRequired}
	run := domain.WorkflowRun{ID: "run", WorkflowID: "wf", Progress: domain.ProgressReady}
	worker := domain.WorkerInventory{ID: "worker", AcceptBacklog: true, Health: domain.WorkerHealthReady, ObservedAt: adminTestNow,
		Projects:  []domain.WorkerProjectInventory{{Name: "project", Available: true, UpdatedAt: adminTestNow}},
		Providers: []domain.WorkerProviderInventory{{InstanceID: "claude", Models: []string{"opus"}, QuotaPoolID: "claude-main", Available: true}}}
	reader := explainReader{records: sqlite.CoordinatorRecords{Workflows: []domain.Workflow{wf}, WorkflowRuns: []domain.WorkflowRun{run}, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}},
		workers: []domain.WorkerSnapshot{{WorkerID: worker.ID, Connected: true, ObservedAt: adminTestNow, ValidUntil: adminTestNow.Add(time.Hour), Inventory: worker}}}
	input := backlog.PlanInput{Now: adminTestNow, MaxWorkerSnapshotAge: time.Hour, Workflows: []backlog.PlanningWorkflow{{Workflow: wf, State: backlog.DAGState{Run: run, Tasks: []domain.Task{task}, Attempts: []domain.Attempt{attempt}}}},
		Workers: []domain.WorkerInventory{worker}, QuotaPools: []domain.QuotaPool{{ID: "claude-main", ProviderInstanceIDs: []string{"claude"}, MaxConcurrent: 4, ActiveAssignments: 4}},
		Ordering: backlog.PlanningOrderingInput{DeadlineRiskWindow: time.Hour, Attempts: map[string]backlog.PlanningAttemptOrdering{"attempt": {ReadySince: adminTestNow}}}}
	reader.records.QuotaPools = input.QuotaPools
	return reader, input
}
func planningExplainService(t *testing.T, reader explainReader, holder *PlanningSnapshotHolder) *Service {
	t.Helper()
	service, err := New(reader, planningReadAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return adminTestNow })
	service.SetPlanningSnapshotHolder(holder)
	return service
}
func planningQuery(t *testing.T, service *Service) Explanation {
	t.Helper()
	response, err := service.Query(context.Background(), Query{Version: Version, Kind: QueryExplanation, WorkflowRunID: "run", TaskID: "task"})
	if err != nil {
		t.Fatal(err)
	}
	return *response.Explanation
}
func TestPlanningExplainRealFullPoolAndV1(t *testing.T) {
	reader, input := planningExplainFixture()
	plan, err := backlog.BuildPlan(input)
	if err != nil {
		t.Fatal(err)
	}
	holder := NewPlanningSnapshotHolder(time.Minute)
	holder.Record(plan, reader.records.Attempts, adminTestNow.Add(-12*time.Second))
	service := planningExplainService(t, reader, holder)
	got := planningQuery(t, service)
	if got.Eligible || !strings.Contains(got.Summary, "not assigned") {
		t.Fatalf("pool-held explanation: %+v", got)
	}
	found := false
	for _, b := range got.Blockers {
		if b.Code == backlog.PlanningBlockerPoolConcurrency {
			found = true
			if b.WorkerID != "worker" || b.QuotaPoolID != "claude-main" || !strings.Contains(b.Detail, "4") {
				t.Fatalf("pool blocker: %+v", b)
			}
		}
	}
	if !found {
		t.Fatalf("no full-pool verdict: %+v", got)
	}
	bytes, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Explanation
	if err = json.Unmarshal(bytes, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, got) {
		t.Fatalf("existing wire fields lost planning explanation: %+v", decoded)
	}
	diagnosis, err := service.Query(context.Background(), Query{Version: Version, Kind: QueryDiagnose, WorkflowRunID: "run"})
	if err != nil {
		t.Fatal(err)
	}
	if len(diagnosis.Diagnosis.Explanations) != 1 || !reflect.DeepEqual(diagnosis.Diagnosis.Explanations[0], got) {
		t.Fatalf("diagnose differs: %+v", diagnosis.Diagnosis)
	}
}

// Mapping deliberately uses arbitrary planner codes: it must not drop newly
// introduced verdicts or silently translate policy semantics in the admin.
func TestPlanningExplainMapsPlanningPolicyCodes(t *testing.T) {
	for _, code := range []string{backlog.PlanningBlockerExecutorCapacity, backlog.PlanningBlockerTaskClass, backlog.PlanningBlockerQuotaCapacity, backlog.PlanningBlockerQuotaWindowMissing, backlog.PlanningBlockerQuotaObservationStale, backlog.PlanningBlockerSurplusWindow, backlog.PlanningBlockerDeadlineRunway, backlog.PlanningBlockerQuotaDrainRunway, backlog.PlanningBlockerQuotaAdmission, backlog.PlanningBlockerRouteWorkerMismatch, backlog.PlanningBlockerProviderUnavailable, backlog.PlanningBlockerModelUnavailable, backlog.PlanningBlockerQuotaPoolUnavailable, backlog.PlanningBlockerNoEligibleWorker, backlog.PlanningBlockerCandidatePolicy, "future-policy"} {
		t.Run(code, func(t *testing.T) {
			reader, _ := planningExplainFixture()
			at := adminTestNow.Add(time.Minute)
			plan := backlog.Plan{Decisions: []backlog.TaskPlanningDecision{{WorkflowRunID: "run", TaskID: "task", AttemptID: "attempt", Progress: domain.ProgressReady,
				Candidates: []backlog.CandidateEvaluation{{WorkerID: "worker", Blockers: []backlog.PlanningBlocker{{Code: code, Detail: "planner's own verdict", QuotaPoolID: "pool", EarliestAt: &at}}}}}}}
			holder := NewPlanningSnapshotHolder(time.Minute)
			holder.Record(plan, reader.records.Attempts, adminTestNow)
			got := planningQuery(t, planningExplainService(t, reader, holder))
			if got.Eligible {
				t.Fatal("blocked candidate eligible")
			}
			found := false
			for _, b := range got.Blockers {
				if b.Code == code {
					found = true
					if b.Detail != "planner's own verdict" || b.WorkerID != "worker" || b.QuotaPoolID != "pool" || b.EarliestAt == nil || !b.EarliestAt.Equal(at) {
						t.Fatalf("mapping: %+v", b)
					}
				}
			}
			if !found {
				t.Fatalf("missing %s: %+v", code, got)
			}
		})
	}
}
func TestPlanningExplainStaleProposedAndUnevaluated(t *testing.T) {
	for _, test := range []struct {
		name     string
		age      time.Duration
		revision int64
		include  bool
		eligible bool
		summary  string
	}{
		{"fresh", time.Second, 1, true, true, "eligible to start"},
		{"boundary", 3 * time.Minute, 1, true, true, "eligible to start"},
		{"old", 3*time.Minute + time.Second, 1, true, false, "stale (181s old)"},
		{"revision", time.Second, 2, true, false, "stale (1s old)"},
		{"missing", time.Second, 1, false, false, "not evaluated"},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader, _ := planningExplainFixture()
			holder := NewPlanningSnapshotHolder(time.Minute)
			plan := backlog.Plan{}
			if test.include {
				plan.Decisions = []backlog.TaskPlanningDecision{{WorkflowRunID: "run", TaskID: "task", AttemptID: "attempt", Progress: domain.ProgressReady, Proposed: true}}
			}
			holder.Record(plan, reader.records.Attempts, adminTestNow.Add(-test.age))
			reader.records.Attempts[0].Revision = test.revision
			got := planningQuery(t, planningExplainService(t, reader, holder))
			if got.Eligible != test.eligible || !strings.Contains(got.Summary, test.summary) {
				t.Fatalf("explanation: %+v", got)
			}
		})
	}
}
func TestPlanningExplainQueueUsesPassOrderAndClasses(t *testing.T) {
	reader, _ := planningExplainFixture()
	holder := NewPlanningSnapshotHolder(time.Minute)
	var decisions []backlog.TaskPlanningDecision
	for i := 0; i < 6; i++ {
		id := fmt.Sprintf("ahead-%d", i)
		run := fmt.Sprintf("run-%d", i)
		class := domain.TaskClassRequired
		if i%2 != 0 {
			class = domain.TaskClassSurplus
		}
		reader.records.Tasks = append(reader.records.Tasks, domain.Task{ID: id, WorkflowID: "wf", Class: class})
		reader.records.Attempts = append(reader.records.Attempts, domain.Attempt{ID: id, WorkflowRunID: run, TaskID: id, Number: 1, Revision: 1, Progress: domain.ProgressReady, Control: domain.ControlUnassigned})
		decisions = append(decisions, backlog.TaskPlanningDecision{WorkflowRunID: run, TaskID: id, AttemptID: id, Progress: domain.ProgressReady, Order: backlog.PlanningOrder{DeadlineRisk: i%2 == 0}})
	}
	decisions = append(decisions, backlog.TaskPlanningDecision{WorkflowRunID: "run", TaskID: "task", AttemptID: "attempt", Progress: domain.ProgressReady})
	holder.Record(backlog.Plan{Decisions: decisions}, reader.records.Attempts, adminTestNow)
	got := planningQuery(t, planningExplainService(t, reader, holder))
	want := "queue position 7 of 7 ready tasks; ahead: 3 required tasks, 3 surplus tasks (run-0, run-1, run-2)"
	if !strings.Contains(strings.Join(got.Details, "\n"), want) {
		t.Fatalf("queue: %+v", got.Details)
	}
}
func TestPlanningExplainCapDedupAndImmutableCopy(t *testing.T) {
	reader, _ := planningExplainFixture()
	at := adminTestNow.Add(time.Hour)
	d := backlog.TaskPlanningDecision{WorkflowRunID: "run", TaskID: "task", AttemptID: "attempt", Progress: domain.ProgressReady}
	for i := 19; i >= 0; i-- {
		b := backlog.PlanningBlocker{Code: backlog.PlanningBlockerPoolConcurrency, Detail: "pool is full", QuotaPoolID: "pool", EarliestAt: &at}
		d.Candidates = append(d.Candidates, backlog.CandidateEvaluation{WorkerID: fmt.Sprintf("worker-%02d", i), Blockers: []backlog.PlanningBlocker{b, b}})
	}
	plan := backlog.Plan{Decisions: []backlog.TaskPlanningDecision{d}}
	holder := NewPlanningSnapshotHolder(time.Minute)
	holder.Record(plan, reader.records.Attempts, adminTestNow)
	plan.Decisions[0].Candidates[0].Blockers[0].Detail = "mutated"
	at = at.Add(time.Hour)
	service := planningExplainService(t, reader, holder)
	got := planningQuery(t, service)
	if len(got.Blockers) != 10 || !strings.Contains(strings.Join(got.Details, "\n"), "and 10 more") {
		t.Fatalf("cap/dedup: %+v", got)
	}
	for i, b := range got.Blockers {
		if b.WorkerID != fmt.Sprintf("worker-%02d", i) || b.Detail != "pool is full" || !b.EarliestAt.Equal(adminTestNow.Add(time.Hour)) {
			t.Fatalf("not immutable/deterministic: %+v", got)
		}
	}
}
func TestPlanningSnapshotHolderConcurrentPublishAndQueries(t *testing.T) {
	reader, _ := planningExplainFixture()
	plan := backlog.Plan{Decisions: []backlog.TaskPlanningDecision{{WorkflowRunID: "run", TaskID: "task", AttemptID: "attempt", Progress: domain.ProgressReady, Proposed: true}}}
	holder := NewPlanningSnapshotHolder(time.Minute)
	holder.Record(plan, reader.records.Attempts, adminTestNow)
	service := planningExplainService(t, reader, holder)
	var wg sync.WaitGroup
	wg.Add(5)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			holder.Record(plan, reader.records.Attempts, adminTestNow)
		}
	}()
	for r := 0; r < 4; r++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				response, err := service.Query(context.Background(), Query{Version: Version, Kind: QueryExplanation, WorkflowRunID: "run", TaskID: "task"})
				if err != nil || response.Explanation == nil || !response.Explanation.Eligible {
					t.Errorf("query: %+v, %v", response, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
