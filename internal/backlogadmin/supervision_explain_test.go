package backlogadmin

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

var explainTestNow = time.Date(2026, 9, 16, 9, 0, 0, 0, time.UTC)

// explainReader is a Reader that also answers supervision readiness snapshots,
// which is how a coordinator store reaches an explanation.
type explainReader struct {
	records  sqlite.CoordinatorRecords
	workers  []domain.WorkerSnapshot
	snapshot domain.SupervisionSnapshot
}

func (r explainReader) LoadCoordinatorRecords(context.Context) (sqlite.CoordinatorRecords, error) {
	return r.records, nil
}

func (r explainReader) LoadWorkerSnapshots(context.Context) ([]domain.WorkerSnapshot, error) {
	return r.workers, nil
}

func (r explainReader) LoadQuotaAdmissions(context.Context) ([]domain.QuotaAdmissionRecord, error) {
	return nil, nil
}

func (r explainReader) LoadSupervisionSnapshot(_ context.Context, runID string) (domain.SupervisionSnapshot, error) {
	if runID != r.snapshot.RunID {
		return domain.SupervisionSnapshot{RunID: runID}, nil
	}
	return r.snapshot, nil
}

// explainFixture is one supervised run whose gate is ready for review and whose
// protected task the planner is therefore withholding.
func explainFixture(supervisorCapable bool) explainReader {
	route := domain.ProviderRoute{ProviderInstanceID: "instance-a", Model: "model-b"}
	run := domain.WorkflowRun{
		ID: "run-1", WorkflowID: "wf-1", GraphRevision: 12,
		Progress: domain.ProgressActive,
		Supervision: &domain.SupervisionRecord{
			RunID: "run-1", Config: domain.SupervisionConfig{Route: route}, ActivationEpoch: 1,
		},
	}
	worker := domain.WorkerSnapshot{
		WorkerID: "worker-a",
		Inventory: domain.WorkerInventory{
			ID:        "worker-a",
			Providers: []domain.WorkerProviderInventory{{InstanceID: "instance-a", Models: []string{"model-b"}}},
		},
	}
	if supervisorCapable {
		worker.Inventory.Capabilities = []string{workerproto.CapabilityCampaignSupervision}
	}
	return explainReader{
		records: sqlite.CoordinatorRecords{
			Workflows:    []domain.Workflow{{ID: "wf-1", Project: "t3-steward"}},
			WorkflowRuns: []domain.WorkflowRun{run},
			Tasks: []domain.Task{
				{ID: "task-publish", Name: "publish", WorkflowID: "wf-1"},
			},
		},
		workers: []domain.WorkerSnapshot{worker},
		snapshot: domain.SupervisionSnapshot{
			RunID: "run-1", GraphRevision: 12, Supervised: true,
			// The store always reports this true; the service is what answers it
			// honestly, so leaving it true here proves the service overrides it.
			RouteAvailable: true,
			Gates: []domain.Gate{{
				Definition: domain.GateDefinition{
					ID: "gate-1", Name: "analysis_review",
					ObservedTaskIDs:  []string{"task-analyse"},
					ProtectedTaskIDs: []string{"task-publish"},
				},
				RunID: "run-1", State: domain.GateReadyForReview,
				GraphRevision: 12, EvidenceSnapshotID: "snapshot-1", Revision: 3,
			}},
		},
	}
}

func explainService(t *testing.T, reader explainReader, supervisorClientConfigured bool) *Service {
	t.Helper()
	service, err := New(reader, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return explainTestNow })
	service.SetSupervisorClientConfigured(supervisorClientConfigured)
	return service
}

func explainTask(t *testing.T, service *Service, taskID string) Explanation {
	t.Helper()
	response, err := service.Query(context.Background(), Query{
		Version: Version, Kind: QueryExplanation,
		Principal:     Principal{ID: "local:1000", Roles: []string{LocalAdminRole}},
		WorkflowRunID: "run-1", TaskID: taskID,
	})
	if err != nil {
		t.Fatalf("explain %s: %v", taskID, err)
	}
	if response.Explanation == nil {
		t.Fatalf("explain %s returned no explanation", taskID)
	}
	return *response.Explanation
}

func blockerWithCode(explanation Explanation, code domain.SupervisionBlockerCode) (Blocker, bool) {
	for _, blocker := range explanation.Blockers {
		if blocker.SupervisionCode == code {
			return blocker, true
		}
	}
	return Blocker{}, false
}

// A gate-protected task is withheld by the planner, and explain has to say so.
// It used to report "task is eligible to start" with no blockers at all, which
// is the opposite of what the coordinator was doing with the same task.
func TestExplainReportsTheSupervisionGateThePlannerApplies(t *testing.T) {
	service := explainService(t, explainFixture(true), true)
	explanation := explainTask(t, service, "publish")
	if explanation.Eligible {
		t.Fatalf("a gate-protected task was reported eligible: %#v", explanation)
	}
	blocker, found := blockerWithCode(explanation, domain.SupervisionBlockerGateAwaitingReview)
	if !found {
		t.Fatalf("explanation names no awaiting-review gate: %#v", explanation.Blockers)
	}
	if blocker.GateID != "gate-1" {
		t.Fatalf("blocker does not name the gate to decide: %#v", blocker)
	}
	if blocker.Code != "supervision-gate" {
		t.Fatalf("blocker planning code = %q, want supervision-gate", blocker.Code)
	}
	if explanation.Summary == "task is eligible to start" {
		t.Fatalf("summary contradicts the blockers: %q", explanation.Summary)
	}
	// The route is available here, so the reason the gate waits is the review
	// itself and explain must not also blame the route.
	if _, found := blockerWithCode(explanation, domain.SupervisionBlockerRouteUnavailable); found {
		t.Fatalf("explanation blamed the route on a capable fleet: %#v", explanation.Blockers)
	}
}

// With no supervisor admin client the gate is not merely waiting for a review:
// nothing in the deployment will ever dispatch one, and explain reports the
// route as unavailable and names the configuration that is missing.
func TestExplainReportsAMissingSupervisorClientAsAnUnavailableRoute(t *testing.T) {
	service := explainService(t, explainFixture(true), false)
	explanation := explainTask(t, service, "publish")
	blocker, found := blockerWithCode(explanation, domain.SupervisionBlockerRouteUnavailable)
	if !found {
		t.Fatalf("explanation does not report the route as unavailable: %#v", explanation.Blockers)
	}
	if !strings.Contains(blocker.Detail, "no supervisor client configured") {
		t.Fatalf("blocker does not name the missing supervisor client: %q", blocker.Detail)
	}
}

// A fleet with a supervisor client but no capable worker reports the route as
// unavailable too, and says the other cause.
func TestExplainReportsAnUnadmittedRouteWhenNoWorkerIsCapable(t *testing.T) {
	service := explainService(t, explainFixture(false), true)
	explanation := explainTask(t, service, "publish")
	blocker, found := blockerWithCode(explanation, domain.SupervisionBlockerRouteUnavailable)
	if !found {
		t.Fatalf("explanation does not report the route as unavailable: %#v", explanation.Blockers)
	}
	if !strings.Contains(blocker.Detail, "cannot be admitted") {
		t.Fatalf("blocker does not name the unadmitted route: %q", blocker.Detail)
	}
}
