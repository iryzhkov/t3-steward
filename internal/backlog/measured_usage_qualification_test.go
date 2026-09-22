package backlog_test

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/source/providerlog"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

type qualificationFixture struct {
	Version    int                     `json:"version"`
	Disclaimer string                  `json:"disclaimer"`
	Scenarios  []qualificationScenario `json:"scenarios"`
}

type qualificationScenario struct {
	ID           string                    `json:"id"`
	RunID        string                    `json:"runId"`
	RunProgress  domain.ProgressState      `json:"runProgress"`
	SignedReplay bool                      `json:"signedReplay,omitempty"`
	Assignments  []qualificationAssignment `json:"assignments"`
	Evidence     []qualificationEvidence   `json:"evidence"`
	Expected     qualificationObserved     `json:"expected"`
}

type qualificationAssignment struct {
	ID            string               `json:"id"`
	RunID         string               `json:"runId,omitempty"`
	TaskID        string               `json:"taskId"`
	AttemptID     string               `json:"attemptId"`
	AttemptNumber int                  `json:"attemptNumber"`
	Progress      domain.ProgressState `json:"progress"`
	WorkerID      string               `json:"workerId"`
	Provider      string               `json:"provider"`
	Model         string               `json:"model"`
	Thread        string               `json:"thread"`
	Role          domain.ExecutionRole `json:"role"`
}

type qualificationEvidence struct {
	AssignmentID string `json:"assignmentId"`
	Line         string `json:"line"`
	Replay       int    `json:"replay,omitempty"`
}

type qualificationTotals struct {
	Input        int64                    `json:"input"`
	CacheWrite   int64                    `json:"cacheWrite"`
	CacheRead    int64                    `json:"cacheRead"`
	Output       int64                    `json:"output"`
	Cost         float64                  `json:"cost"`
	CostCoverage domain.UsageCostCoverage `json:"costCoverage"`
	Samples      int64                    `json:"samples"`
	Calls        int64                    `json:"calls"`
	Turns        int64                    `json:"turns"`
}

type qualificationCoverage struct {
	State         domain.UsageCoverageState `json:"state"`
	Raw           int64                     `json:"raw"`
	Normalized    int64                     `json:"normalized"`
	Resets        int64                     `json:"resets,omitempty"`
	UnknownModels int64                     `json:"unknownModels,omitempty"`
}

type qualificationAggregate struct {
	Outcome domain.ProgressState `json:"outcome,omitempty"`
	Totals  qualificationTotals  `json:"totals"`
}

type qualificationObserved struct {
	RunProgress               domain.ProgressState              `json:"runProgress"`
	AcceptedOutcomes          int64                             `json:"acceptedOutcomes"`
	CostPerAcceptedOutcomeUSD *float64                          `json:"costPerAcceptedOutcomeUsd,omitempty"`
	Totals                    qualificationTotals               `json:"totals"`
	Coverage                  qualificationCoverage             `json:"coverage"`
	Tasks                     map[string]qualificationAggregate `json:"tasks"`
	Attempts                  map[string]qualificationAggregate `json:"attempts"`
	Roles                     map[string]qualificationAggregate `json:"roles"`
	Models                    map[string]qualificationAggregate `json:"models"`
}

func TestMeasuredUsageQualificationScenarios(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "measured-usage-qualification-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture qualificationFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Version != 1 || !strings.Contains(fixture.Disclaimer, "not subscription quota savings") ||
		!strings.Contains(fixture.Disclaimer, "Synthetic") {
		t.Fatalf("qualification disclaimer/version = %d %q", fixture.Version, fixture.Disclaimer)
	}
	if len(fixture.Scenarios) != 4 {
		t.Fatalf("qualification scenarios = %d, want 4", len(fixture.Scenarios))
	}

	for _, scenario := range fixture.Scenarios {
		scenario := scenario
		t.Run(scenario.ID, func(t *testing.T) {
			got, publicJSON := runQualificationScenario(t, scenario)
			if !reflect.DeepEqual(got, scenario.Expected) {
				gotJSON, _ := json.MarshalIndent(got, "", "  ")
				wantJSON, _ := json.MarshalIndent(scenario.Expected, "", "  ")
				t.Fatalf("frozen public qualification mismatch\ngot:\n%s\nwant:\n%s", gotJSON, wantJSON)
			}
			lower := strings.ToLower(string(publicJSON))
			for _, forbidden := range []string{"prompt", "transcript", "credential", "must-not-survive", "secret"} {
				if strings.Contains(lower, forbidden) {
					t.Fatalf("public qualification report contains forbidden content marker %q: %s", forbidden, publicJSON)
				}
			}
		})
	}
}

func runQualificationScenario(t *testing.T, scenario qualificationScenario) (qualificationObserved, []byte) {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	coordinatorPath := filepath.Join(root, "coordinator.db")
	coordinatorStore, err := sqlite.OpenMigrated(coordinatorPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = coordinatorStore.Close() })

	assignments := make(map[string]qualificationAssignment, len(scenario.Assignments))
	workers := map[string]*signedUsageWorker{}
	workerPaths := map[string]string{}
	workerStores := map[string]*sqlite.Store{}
	records := sqlite.CoordinatorRecords{}
	runs := map[string]bool{}
	tasks := map[string]bool{}
	now := causalUsageNow

	for _, item := range scenario.Assignments {
		assignments[item.ID] = item
		runID := item.RunID
		if runID == "" {
			runID = scenario.RunID
		}
		if !runs[runID] {
			runs[runID] = true
			records.WorkflowRuns = append(records.WorkflowRuns, domain.WorkflowRun{
				ID: runID, WorkflowID: "workflow-" + runID, Progress: domain.ProgressActive,
				Revision: 1, CreatedAt: now, UpdatedAt: now,
			})
		}
		if !tasks[item.TaskID] {
			tasks[item.TaskID] = true
			records.Tasks = append(records.Tasks, domain.Task{
				ID: item.TaskID, RunID: runID, WorkflowID: "workflow-" + runID,
				Name: item.TaskID, Class: domain.TaskClassRequired,
			})
		}
		attempt := domain.Attempt{
			ID: item.AttemptID, WorkflowRunID: runID, TaskID: item.TaskID, Number: item.AttemptNumber,
			AssignmentID: item.ID, Progress: domain.ProgressReady, Control: domain.ControlUnassigned,
			Revision: 1, UpdatedAt: now,
		}
		switch item.Role {
		case domain.ExecutionRoleRepairExecutor:
			activation := domain.Activation{
				ID: "activation-" + item.ID, RunID: runID, Epoch: 1,
				Purpose: domain.RecoveryActivationRepair, State: domain.ActivationPendingDispatch,
				DispatchIdentity: "repair-" + item.ID,
			}
			attempt.SupervisionActivationID, attempt.SupervisionActivationEpoch = activation.ID, activation.Epoch
			records.Activations = append(records.Activations, activation)
		case domain.ExecutionRoleGateReviewer:
			incident := domain.ReviewIncident{
				ID: "incident-" + item.ID, RunID: runID, GateID: "gate-" + item.ID,
				State: domain.IncidentOpen, Revision: 1,
			}
			activation := domain.Activation{
				ID: "activation-" + item.ID, RunID: runID, Epoch: 1, IncidentID: incident.ID,
				State: domain.ActivationPendingDispatch, DispatchIdentity: "review-" + item.ID,
			}
			attempt.SupervisionActivationID, attempt.SupervisionActivationEpoch = activation.ID, activation.Epoch
			records.Incidents = append(records.Incidents, incident)
			records.Activations = append(records.Activations, activation)
		case domain.ExecutionRoleSupervisorActivation:
			activation := domain.Activation{
				ID: "activation-" + item.ID, RunID: runID, Epoch: 1,
				State: domain.ActivationPendingDispatch, DispatchIdentity: "supervision-" + item.ID,
			}
			attempt.SupervisionActivationID, attempt.SupervisionActivationEpoch = activation.ID, activation.Epoch
			records.Activations = append(records.Activations, activation)
		}
		records.Attempts = append(records.Attempts, attempt)
		records.Assignments = append(records.Assignments, domain.Assignment{
			ID: item.ID, AttemptID: item.AttemptID, WorkerID: item.WorkerID, WorkerEpoch: "epoch-" + item.WorkerID,
			Route: domain.ProviderRoute{WorkerID: item.WorkerID, ProviderInstanceID: item.Provider, Model: item.Model, QuotaPoolID: "pool"},
			State: domain.AssignmentOffered, Epoch: 1, LeaseToken: "lease-" + item.ID,
			DispatchToken: "dispatch-" + item.ID, ThreadID: item.Thread,
			LeaseExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
		})
		if _, ok := workerStores[item.WorkerID]; !ok {
			path := filepath.Join(root, item.WorkerID+".db")
			store, openErr := sqlite.OpenMigrated(path)
			if openErr != nil {
				t.Fatal(openErr)
			}
			workerPaths[item.WorkerID], workerStores[item.WorkerID] = path, store
			worker := newSignedUsageWorker(t, item.WorkerID, "epoch-"+item.WorkerID, store)
			workers[item.WorkerID] = worker
		}
		addQualificationModel(workers[item.WorkerID], item.Provider, item.Model)
	}
	t.Cleanup(func() {
		for _, store := range workerStores {
			_ = store.Close()
		}
	})
	if err := coordinatorStore.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}

	for _, evidence := range scenario.Evidence {
		item, ok := assignments[evidence.AssignmentID]
		if !ok {
			t.Fatalf("evidence names unknown assignment %q", evidence.AssignmentID)
		}
		parsed, parseErr := providerlog.ParseUsageEvidenceLine(evidence.Line)
		if parseErr != nil || len(parsed) == 0 {
			t.Fatalf("parse evidence %s: %#v, %v", evidence.AssignmentID, parsed, parseErr)
		}
		replays := evidence.Replay
		if replays < 1 {
			replays = 1
		}
		for i := 0; i < replays; i++ {
			for _, sample := range parsed {
				if err := workerStores[item.WorkerID].RecordUsage(ctx, sample); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	coordinator := backlog.FleetCoordinator{Store: coordinatorStore, Now: func() time.Time { return now }}
	workerIDs := make([]string, 0, len(workers))
	for workerID := range workers {
		workerIDs = append(workerIDs, workerID)
	}
	sort.Strings(workerIDs)
	for _, workerID := range workerIDs {
		worker := workers[workerID]
		if scenario.SignedReplay {
			client := signedUsageClient(t, worker, "lost-"+workerID, &signedLoopback{worker: worker, loseNext: true})
			if _, err := coordinator.ReconcileWorker(ctx, client, causalOfferBuilder{},
				backlog.WorkerAdmissionPolicy{QuotaChecksDisabled: true}, nil, nil, time.Minute, time.Hour); err == nil {
				t.Fatal("lost signed response unexpectedly succeeded")
			}
			if err := workerStores[workerID].Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := sqlite.OpenMigrated(workerPaths[workerID])
			if err != nil {
				t.Fatal(err)
			}
			workerStores[workerID], worker.store = reopened, reopened
			if err := coordinatorStore.Close(); err != nil {
				t.Fatal(err)
			}
			coordinatorStore, err = sqlite.OpenMigrated(coordinatorPath)
			if err != nil {
				t.Fatal(err)
			}
			coordinator.Store = coordinatorStore
		}
		client := signedUsageClient(t, worker, "qualification-"+workerID, &signedLoopback{worker: worker})
		report, err := coordinator.ReconcileWorker(ctx, client, causalOfferBuilder{},
			backlog.WorkerAdmissionPolicy{QuotaChecksDisabled: true}, nil, nil, time.Minute, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		for _, claimed := range report.Claimed {
			want := assignments[claimed.ID].Role
			if claimed.ExecutionRole != want {
				t.Fatalf("claim %s role = %q, want %q", claimed.ID, claimed.ExecutionRole, want)
			}
		}
	}

	completed := now.Add(10 * time.Minute)
	terminal := sqlite.CoordinatorRecords{}
	for runID := range runs {
		progress := scenario.RunProgress
		if runID != scenario.RunID {
			progress = domain.ProgressSucceeded
		}
		terminal.WorkflowRuns = append(terminal.WorkflowRuns, domain.WorkflowRun{
			ID: runID, WorkflowID: "workflow-" + runID, Progress: progress, Revision: 2,
			CreatedAt: now, UpdatedAt: completed, CompletedAt: &completed,
		})
	}
	for _, item := range scenario.Assignments {
		runID := item.RunID
		if runID == "" {
			runID = scenario.RunID
		}
		terminal.Attempts = append(terminal.Attempts, domain.Attempt{
			ID: item.AttemptID, WorkflowRunID: runID, TaskID: item.TaskID, Number: item.AttemptNumber,
			AssignmentID: item.ID, ThreadID: item.Thread, Progress: item.Progress, Control: domain.ControlStopped,
			Revision: 3, UpdatedAt: completed, CompletedAt: &completed,
		})
	}
	if err := coordinatorStore.SaveCoordinatorRecords(ctx, terminal); err != nil {
		t.Fatal(err)
	}

	admin, err := backlogadmin.New(coordinatorStore, causalAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	key, err := coordinatorStore.CoordinatorUsageCursorKey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.SetUsageCursorKey(key); err != nil {
		t.Fatal(err)
	}
	admin.SetClock(func() time.Time { return completed })
	response, err := admin.Query(ctx, backlogadmin.Query{
		Version: backlogadmin.Version, Kind: backlogadmin.QueryUsage, WorkflowRunID: scenario.RunID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.UsageReport == nil {
		t.Fatal("public usage query omitted report")
	}
	publicJSON, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	return qualificationObservation(*response.UsageReport), publicJSON
}

func addQualificationModel(worker *signedUsageWorker, provider, model string) {
	for i := range worker.snapshot.Inventory.Providers {
		entry := &worker.snapshot.Inventory.Providers[i]
		if entry.InstanceID != provider {
			continue
		}
		for _, existing := range entry.Models {
			if existing == model {
				return
			}
		}
		entry.Models = append(entry.Models, model)
		return
	}
	worker.snapshot.Inventory.Providers = append(worker.snapshot.Inventory.Providers,
		domain.WorkerProviderInventory{InstanceID: provider, Models: []string{model}, Available: true})
}

func qualificationObservation(report domain.UsageReport) qualificationObserved {
	got := qualificationObserved{
		RunProgress: report.RunProgress, AcceptedOutcomes: report.AcceptedOutcomeCount,
		CostPerAcceptedOutcomeUSD: roundedCostPointer(report.MeasuredCostPerAcceptedOutcomeUSD),
		Totals:                    qualificationTotalsFromDomain(report.Totals),
		Coverage: qualificationCoverage{
			State: report.Coverage.State, Raw: report.Coverage.RawSampleCount,
			Normalized: report.Coverage.NormalizedSampleCount, Resets: report.Coverage.ResetCount,
			UnknownModels: report.Coverage.UnknownModelCount,
		},
		Tasks: make(map[string]qualificationAggregate), Attempts: make(map[string]qualificationAggregate),
		Roles: make(map[string]qualificationAggregate), Models: make(map[string]qualificationAggregate),
	}
	copyAggregates := func(target map[string]qualificationAggregate, source []domain.UsageAggregate) {
		for _, aggregate := range source {
			target[aggregate.Key] = qualificationAggregate{
				Outcome: aggregate.Outcome, Totals: qualificationTotalsFromDomain(aggregate.Totals),
			}
		}
	}
	copyAggregates(got.Tasks, report.ByTask)
	copyAggregates(got.Attempts, report.ByAttempt)
	copyAggregates(got.Roles, report.ByRole)
	copyAggregates(got.Models, report.ByModel)
	return got
}

func qualificationTotalsFromDomain(totals domain.UsageTotals) qualificationTotals {
	return qualificationTotals{
		Input: totals.UncachedInputTokens, CacheWrite: totals.CacheWriteTokens,
		CacheRead: totals.CacheReadTokens, Output: totals.OutputTokens,
		Cost:         math.Round(totals.ProviderCostUSD*1e6) / 1e6,
		CostCoverage: totals.ProviderCostCoverage, Samples: totals.NormalizedSamples,
		Calls: totals.Calls, Turns: totals.Turns,
	}
}

func roundedCostPointer(value *float64) *float64 {
	if value == nil {
		return nil
	}
	rounded := math.Round(*value*1e6) / 1e6
	return &rounded
}
