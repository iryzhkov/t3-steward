package main

// What a coordinator with no supervisor admin client does with a gate that is
// ready for review.
//
// Before this it did nothing at all: coordinatorSupervisorClient returned no
// principal, activation dispatch was disabled, and no warning, no incident and
// no planner blocker said so. The protected task was simply never offered, and
// the only evidence anything was wrong was work that never started.

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

const (
	missingSupervisorRun      = "run-unsupervised-client"
	missingSupervisorWorker   = "worker-capable"
	missingSupervisorGate     = "gate-analysis-review"
	missingSupervisorIncident = "incident-analysis-review"
)

var missingSupervisorTime = time.Date(2026, time.September, 16, 10, 0, 0, 0, time.UTC)

type missingSupervisorFixture struct {
	store       *sqlite.Store
	supervision backlog.CoordinatorSupervisionStore
	coordinator coordinatorSupervision
	logs        *bytes.Buffer
	now         time.Time
}

// newMissingSupervisorFixture is one supervised run whose gate is ready for
// review, on a fleet whose worker could host the overseer route, with no
// supervisor admin client configured.
func newMissingSupervisorFixture(t *testing.T, supervisorClient string) *missingSupervisorFixture {
	t.Helper()
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	fixture := &missingSupervisorFixture{store: store, logs: &bytes.Buffer{}, now: missingSupervisorTime}
	store.SetClock(func() time.Time { return fixture.now })
	fixture.supervision = backlog.CoordinatorSupervisionStore{Store: store}
	fixture.coordinator = coordinatorSupervision{
		store:  fixture.supervision,
		logger: slog.New(slog.NewTextHandler(fixture.logs, nil)),
		now:    func() time.Time { return fixture.now },
		workers: func(ctx context.Context) ([]domain.WorkerSnapshot, error) {
			return store.LoadWorkerSnapshots(ctx)
		},
		activations: backlog.SupervisionActivationService{
			Store: fixture.supervision, Now: func() time.Time { return fixture.now },
		},
		warnedNoSupervisorClient: make(map[string]bool),
		settings: coordinatorActivationSettings{
			CoordinatorID: "coordinator-1", CoordinatorEpoch: 1, SupervisorClient: supervisorClient,
		},
	}
	config := domain.SupervisionConfig{
		Route:                 domain.ProviderRoute{ProviderInstanceID: "claudeAgent", Model: "claude-fable-5-1"},
		PromptArtifactID:      "artifact-overseer",
		MaxActivations:        5,
		MaxTurnsPerActivation: 4,
		ActivationDeadline:    time.Hour,
	}
	run := domain.WorkflowRun{
		ID: missingSupervisorRun, WorkflowID: "workflow-1", GraphRevision: 1,
		Progress: domain.ProgressActive, Revision: 1,
		CreatedAt: fixture.now, UpdatedAt: fixture.now,
		// The run record is what every boundary reads to decide a run is
		// supervised at all, so the fixture states it there as well as in the
		// supervision tables.
		Supervision: &domain.SupervisionRecord{RunID: missingSupervisorRun, Config: config},
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: "workflow-1", Version: 1, Name: "workflow", Class: domain.TaskClassRequired,
			TaskIDs: []string{"task-analyse", "task-synthesis"}, CreatedAt: fixture.now,
		}},
		WorkflowRuns: []domain.WorkflowRun{run},
		Tasks: []domain.Task{{
			ID: "task-analyse", WorkflowID: "workflow-1", Name: "analyse", Class: domain.TaskClassRequired,
		}, {
			ID: "task-synthesis", WorkflowID: "workflow-1", Name: "synthesis", Class: domain.TaskClassRequired,
			Needs: []string{"task-analyse"},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveWorkerSnapshot(ctx, domain.WorkerSnapshot{
		WorkerID: missingSupervisorWorker, WorkerEpoch: "worker-epoch-1", CoordinatorEpoch: 1, Sequence: 1,
		Connected: true, ObservedAt: fixture.now, ValidUntil: fixture.now.Add(time.Hour),
		Inventory: domain.WorkerInventory{
			ID: missingSupervisorWorker, AcceptBacklog: true, Health: domain.WorkerHealthReady,
			Capabilities: []string{workerproto.CapabilityCampaignSupervision}, ObservedAt: fixture.now,
			Providers: []domain.WorkerProviderInventory{{
				InstanceID: "claudeAgent", Models: []string{"claude-fable-5-1"},
			}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutSupervision(ctx, sqlite.SupervisionMaterialization{
		Record: domain.SupervisionRecord{RunID: missingSupervisorRun, Config: config},
		Gates: []domain.Gate{{
			RunID: missingSupervisorRun, State: domain.GateReadyForReview, GraphRevision: 1,
			Definition: domain.GateDefinition{
				ID: missingSupervisorGate, Name: "analysis_review",
				ObservedTaskIDs:  []string{"task-analyse"},
				ProtectedTaskIDs: []string{"task-synthesis"},
			},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	// The open review incident the live coordinator already had: the gate became
	// reviewable on an earlier boundary, so this pass observes a standing block
	// rather than a new one.
	if _, err := fixture.supervision.OpenReviewIncident(ctx, sqlite.IncidentRequest{
		RunID: missingSupervisorRun, IncidentID: missingSupervisorIncident, RequestID: missingSupervisorIncident,
		Actor:         domain.Actor{Kind: domain.ActorOperator, Principal: coordinatorSupervisionPrincipal},
		SourceEventID: "event-" + missingSupervisorIncident, GateID: missingSupervisorGate,
		RequiredDisposition: domain.DispositionGateDecision,
		Reason:              "gate analysis_review is ready for review", OpenedAt: fixture.now,
	}); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func (f *missingSupervisorFixture) incident(t *testing.T) domain.ReviewIncident {
	t.Helper()
	projection, err := f.supervision.SupervisionProjection(context.Background(), missingSupervisorRun)
	if err != nil {
		t.Fatal(err)
	}
	for _, incident := range projection.Incidents {
		if incident.ID == missingSupervisorIncident {
			return incident
		}
	}
	t.Fatalf("the review incident is gone: %#v", projection.Incidents)
	return domain.ReviewIncident{}
}

// Route availability is the planner's input, and it must be false when no
// overseer can be dispatched, so the blocker reads supervision-route-unavailable
// rather than a gate that looks merely pending.
func TestPlanningRouteIsUnavailableWithoutASupervisorClient(t *testing.T) {
	fixture := newMissingSupervisorFixture(t, "")
	ctx := context.Background()
	records, err := fixture.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	workers, err := fixture.store.LoadWorkerSnapshots(ctx)
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := coordinatorSupervisionSnapshots(ctx, fixture.store, records.WorkflowRuns, workers, false)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, supervised := blocked[missingSupervisorRun]
	if !supervised {
		t.Fatal("the supervised run has no snapshot")
	}
	if snapshot.RouteAvailable {
		t.Fatal("route reported available with no supervisor client configured")
	}
	verdict := domain.SupervisionAdmits(snapshot, domain.SupervisionQuery{TaskID: "task-synthesis"})
	if verdict.Admitted {
		t.Fatal("the protected task was admitted")
	}
	var unavailable bool
	for _, blocker := range verdict.Blockers {
		if blocker.Code == domain.SupervisionBlockerRouteUnavailable {
			unavailable = true
		}
	}
	if !unavailable {
		t.Fatalf("blockers = %#v, want supervision-route-unavailable", verdict.Blockers)
	}

	// The same fleet with a supervisor client configured reports the route as
	// available, so this test is about the client and not about the workers.
	configured, err := coordinatorSupervisionSnapshots(ctx, fixture.store, records.WorkflowRuns, workers, true)
	if err != nil {
		t.Fatal(err)
	}
	if !configured[missingSupervisorRun].RouteAvailable {
		t.Fatal("route reported unavailable on a capable fleet with a supervisor client")
	}
}

// The boundary says so once per run, not once per tick, and escalates the open
// review incident with the reason an operator can act on.
func TestSupervisionBoundaryWarnsOnceAndEscalatesWithoutASupervisorClient(t *testing.T) {
	fixture := newMissingSupervisorFixture(t, "")
	ctx := context.Background()
	fixture.coordinator.Tick(ctx)
	fixture.now = missingSupervisorTime.Add(time.Minute)
	fixture.coordinator.Tick(ctx)

	const warning = "no supervisor client configured"
	if count := strings.Count(fixture.logs.String(), warning); count != 1 {
		t.Fatalf("warned %d times over two boundaries, want exactly once: %s", count, fixture.logs.String())
	}
	incident := fixture.incident(t)
	if incident.State != domain.IncidentEscalated {
		t.Fatalf("incident state = %q, want escalated", incident.State)
	}
	// The incident was opened because the gate became reviewable, and is
	// escalated because nobody will review it. The reason an operator reads is
	// the second one.
	if !strings.Contains(incident.Reason, warning) {
		t.Fatalf("escalation reason does not name the missing supervisor client: %q", incident.Reason)
	}
}

// A coordinator that has a supervisor client neither warns nor escalates for
// this reason: the gate is waiting for a review that will actually arrive.
func TestSupervisionBoundaryIsSilentWhenASupervisorClientExists(t *testing.T) {
	fixture := newMissingSupervisorFixture(t, "campaign-supervisor")
	fixture.coordinator.Tick(context.Background())
	if strings.Contains(fixture.logs.String(), "no supervisor client configured") {
		t.Fatalf("warned about a configured supervisor client: %s", fixture.logs.String())
	}
	if state := fixture.incident(t).State; state == domain.IncidentEscalated {
		t.Fatal("escalated a review a configured overseer can perform")
	}
}
