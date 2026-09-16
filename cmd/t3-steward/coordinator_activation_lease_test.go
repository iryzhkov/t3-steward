package main

// Lease renewal, read from the durable records an activation leaves behind.
//
// The coordinator does not renew an overseer's lease on a timer. It renews it
// when the records say the overseer is doing what it was woken for: the worker
// claimed the assignment, or a decision of this activation's own epoch is in
// the store. Everything else about a live activation -- including an overseer
// that stopped deciding -- lets the lease run out, and an expired lease revokes
// decision authority immediately rather than after a grace period.

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

var activationLeaseTime = time.Date(2026, time.September, 16, 9, 0, 0, 0, time.UTC)

const (
	activationLeaseRun      = "run-lease"
	activationLeaseWorker   = "worker-lease"
	activationLeaseGate     = "gate-review"
	activationLeaseEvidence = "evidence-lease"
	activationLeaseClient   = "campaign-supervisor"
)

// activationLeaseFixture is one supervised run with a reviewable gate, its
// worker, and the coordinator boundary that reads them.
type activationLeaseFixture struct {
	store       *sqlite.Store
	supervision backlog.CoordinatorSupervisionStore
	coordinator coordinatorSupervision
	now         time.Time
}

func newActivationLeaseFixture(t *testing.T) *activationLeaseFixture {
	t.Helper()
	ctx := context.Background()
	store, err := sqlite.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	fixture := &activationLeaseFixture{store: store, now: activationLeaseTime}
	store.SetClock(func() time.Time { return fixture.now })
	fixture.supervision = backlog.CoordinatorSupervisionStore{Store: store}
	fixture.coordinator = coordinatorSupervision{
		store:       fixture.supervision,
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:         func() time.Time { return fixture.now },
		activations: backlog.SupervisionActivationService{Store: fixture.supervision, Now: func() time.Time { return fixture.now }},
		settings: coordinatorActivationSettings{
			CoordinatorID: "coordinator-1", CoordinatorEpoch: 1, SupervisorClient: activationLeaseClient,
		},
	}
	run := domain.WorkflowRun{
		ID: activationLeaseRun, WorkflowID: "workflow-lease", GraphRevision: 1,
		Progress: domain.ProgressActive, Revision: 1,
		CreatedAt: fixture.now, UpdatedAt: fixture.now,
	}
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Workflows: []domain.Workflow{{
			ID: "workflow-lease", Version: 1, Name: "workflow", Class: domain.TaskClassRequired,
			TaskIDs: []string{"task-producer", "task-protected"}, CreatedAt: fixture.now,
		}},
		WorkflowRuns: []domain.WorkflowRun{run},
		Tasks: []domain.Task{{
			ID: "task-producer", WorkflowID: "workflow-lease", Name: "producer", Class: domain.TaskClassRequired,
		}, {
			ID: "task-protected", WorkflowID: "workflow-lease", Name: "protected", Class: domain.TaskClassRequired,
			Needs: []string{"task-producer"},
		}},
		Attempts: []domain.Attempt{{
			ID: "attempt-producer", WorkflowRunID: activationLeaseRun, TaskID: "task-producer", Number: 1,
			Progress: domain.ProgressSucceeded, Control: domain.ControlStopped,
			Revision: 7, UpdatedAt: fixture.now,
		}, {
			ID: "attempt-protected", WorkflowRunID: activationLeaseRun, TaskID: "task-protected", Number: 1,
			Progress: domain.ProgressBlocked, Control: domain.ControlUnassigned,
			Revision: 1, UpdatedAt: fixture.now,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveWorkerSnapshot(ctx, domain.WorkerSnapshot{
		WorkerID: activationLeaseWorker, WorkerEpoch: "worker-epoch-1", CoordinatorEpoch: 1, Sequence: 1,
		Connected: true, ObservedAt: fixture.now, ValidUntil: fixture.now.Add(time.Hour),
		Inventory: domain.WorkerInventory{
			ID: activationLeaseWorker, AcceptBacklog: true, Health: domain.WorkerHealthReady,
			Capabilities: []string{workerproto.CapabilityCampaignSupervision}, ObservedAt: fixture.now,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutSupervision(ctx, sqlite.SupervisionMaterialization{
		Record: domain.SupervisionRecord{RunID: activationLeaseRun, Config: domain.SupervisionConfig{
			Route:                 domain.ProviderRoute{ProviderInstanceID: "claudeAgent", Model: "claude-fable-5-1"},
			PromptArtifactID:      "artifact-overseer",
			MaxActivations:        5,
			MaxTurnsPerActivation: 4,
			ActivationDeadline:    time.Hour,
		}},
		Gates: []domain.Gate{{
			RunID: activationLeaseRun, State: domain.GateReadyForReview, GraphRevision: 1,
			Definition: domain.GateDefinition{
				ID: activationLeaseGate, Name: "review", ObservedTaskIDs: []string{"task-producer"},
				ProtectedTaskIDs: []string{"task-protected"},
			},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.supervision.AppendSupervisionEvents(ctx, activationLeaseRun,
		[]backlog.SupervisionEvent{{
			ID: "event-review-ready", RunID: activationLeaseRun, Kind: backlog.TriggerGateReviewReady,
			Reason: "the review gate is ready", GateID: activationLeaseGate, OccurredAt: fixture.now,
		}}); err != nil {
		t.Fatal(err)
	}
	return fixture
}

// activate dispatches one activation and takes it to active, which is the state
// both tests below start from: claimed by its worker, holding a live lease.
func (f *activationLeaseFixture) activate(t *testing.T) domain.Activation {
	t.Helper()
	ctx := context.Background()
	plan, err := f.coordinator.activations.Advance(ctx, activationLeaseRun, f.signal(t, domain.ActivationEventTriggerFired))
	if err != nil {
		t.Fatalf("wake the overseer: %v", err)
	}
	if plan.Dispatch == nil {
		t.Fatal("the trigger planned no dispatch")
	}
	attempt, assignment, err := backlog.ActivationAssignment(plan.Activation, *plan.Dispatch,
		backlog.ActivationPlacement{
			WorkerID: activationLeaseWorker, WorkerEpoch: "worker-epoch-1", SnapshotSequence: 1,
			Route: domain.ProviderRoute{
				WorkerID: activationLeaseWorker, ProviderInstanceID: "claudeAgent",
				Model: "claude-fable-5-1", QuotaPoolID: "claude-main",
			},
		}, 4, f.now)
	if err != nil {
		t.Fatalf("build the activation assignment: %v", err)
	}
	// The durable rows the worker's claim would leave, written directly: what is
	// under test is what the coordinator reads back from them, not the claim.
	attempt.AssignmentID = assignment.ID
	attempt.Progress = domain.ProgressActive
	attempt.Control = domain.ControlRunning
	attempt.Revision = 1
	assignment.State = domain.AssignmentClaimed
	assignment.WorkerEpoch = "worker-epoch-1"
	assignment.CreatedAt, assignment.UpdatedAt = f.now, f.now
	if err := f.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		Attempts: []domain.Attempt{attempt}, Assignments: []domain.Assignment{assignment},
	}); err != nil {
		t.Fatal(err)
	}
	f.now = activationLeaseTime.Add(time.Minute)
	confirmed := f.advance(t)
	if confirmed.Activation.State != domain.ActivationActive {
		t.Fatalf("activation = %#v, want active once its worker claimed it", confirmed.Activation)
	}
	return confirmed.Activation
}

func (f *activationLeaseFixture) signal(t *testing.T, event domain.ActivationEvent) backlog.ActivationSignal {
	t.Helper()
	state, err := f.supervision.LoadSupervisionActivationState(context.Background(), activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	return backlog.ActivationSignal{
		Event:                  event,
		Actor:                  domain.Actor{Kind: domain.ActorOperator, Principal: coordinatorSupervisionPrincipal},
		ExpectedEpoch:          state.Activation.Epoch,
		ExpectedRecordRevision: state.Record.Revision,
		Principal:              f.coordinator.settings.principalID(),
		Reason:                 string(event),
	}
}

// advance derives the lifecycle event this boundary carries and commits it,
// which is exactly what the coordinator's activation pass does.
func (f *activationLeaseFixture) advance(t *testing.T) backlog.ActivationPlan {
	t.Helper()
	ctx := context.Background()
	records, err := f.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	state, err := f.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	signal, wanted, err := f.coordinator.activationLifecycleSignal(ctx, records, state)
	if err != nil {
		t.Fatalf("derive the lifecycle signal: %v", err)
	}
	if !wanted {
		t.Fatal("the activation's durable records imply no lifecycle event")
	}
	plan, err := f.coordinator.activations.Advance(ctx, activationLeaseRun, signal)
	if err != nil {
		t.Fatalf("advance the activation with %s: %v", signal.Event, err)
	}
	return plan
}

// decide records one gate decision by the live overseer, through the supervisor
// principal and at the activation's own epoch.
func (f *activationLeaseFixture) decide(t *testing.T, epoch int64, requestID string) {
	t.Helper()
	state, err := f.store.LoadSupervisionAdminState(context.Background(), activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	var gate sqlite.SupervisionGateFacts
	for _, candidate := range state.Gates {
		if candidate.Gate.Definition.ID == activationLeaseGate {
			gate = candidate
		}
	}
	if _, err := f.store.DecideGate(context.Background(), sqlite.GateDecisionRequest{
		RunID: activationLeaseRun, GateID: activationLeaseGate, RequestID: requestID,
		Actor: domain.Actor{
			Kind: domain.ActorOverseer, Principal: f.coordinator.settings.principalID(), ActivationEpoch: epoch,
		},
		ExpectedGraphRevision: gate.Gate.GraphRevision, ExpectedGateRevision: gate.Gate.Revision,
		Evidence: domain.EvidenceSnapshot{
			ID: activationLeaseEvidence, GraphRevision: 1, TakenAt: f.now,
			Producers: []domain.ProducerEvidence{{
				TaskID: "task-producer", AttemptID: "attempt-producer", ResultRevision: 7,
			}},
		},
		Outcome: domain.GateDecisionAccept, Reason: "the producer meets the rubric", DecidedAt: f.now,
	}); err != nil {
		t.Fatalf("record the overseer's decision: %v", err)
	}
}

func TestActivationLeaseExpiredBeforeRenewalIsRevoked(t *testing.T) {
	fixture := newActivationLeaseFixture(t)
	activation := fixture.activate(t)
	expiry := activation.LeaseExpiresAt
	if expiry == nil {
		t.Fatal("a confirmed dispatch issued no lease")
	}

	// Nothing renews it: no decision is recorded and the assignment stays
	// claimed. One second past expiry the coordinator revokes the activation's
	// authority, without waiting for the worker to say anything.
	fixture.now = expiry.Add(time.Second)
	revoked := fixture.advance(t)
	if revoked.Activation.State != domain.ActivationRevoked ||
		revoked.Activation.Outcome != domain.ActivationOutcomeRevoked {
		t.Fatalf("activation = %#v, want it revoked once its lease expired", revoked.Activation)
	}
	if revoked.Activation.LeaseToken != "" || revoked.Activation.LeaseExpiresAt != nil {
		t.Fatalf("a revoked activation still holds lease %#v", revoked.Activation)
	}
	if revoked.CursorAdvanced {
		t.Fatal("a revoked activation consumed the inbox it never reviewed")
	}

	// Its decisions are refused from that moment, which is what "revoked" has to
	// mean for an overseer that is still running somewhere.
	state, err := fixture.supervision.LoadSupervisionActivationState(context.Background(), activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if err := backlog.AuthorizeActivationDecision(state, domain.Actor{
		Kind: domain.ActorOverseer, Principal: fixture.coordinator.settings.principalID(),
		ActivationEpoch: activation.Epoch,
	}, activation.Epoch, state.Record.Revision, fixture.now); err == nil {
		t.Fatal("a revoked activation was still authorized to decide")
	}
}

func TestActivationLeaseRenewedWithinTheDeadlineStaysActive(t *testing.T) {
	fixture := newActivationLeaseFixture(t)
	activation := fixture.activate(t)
	claimExpiry := *activation.LeaseExpiresAt
	deadline := activation.Deadline
	if deadline == nil {
		t.Fatal("the activation has no maximum elapsed time")
	}

	// The overseer decides a gate well inside its lease. The coordinator reads
	// its own decision row, renews the lease from that moment and leaves the
	// activation active with one turn spent.
	fixture.now = claimExpiry.Add(-5 * time.Minute)
	fixture.decide(t, activation.Epoch, "decide-accept")
	renewed := fixture.advance(t)
	if renewed.Result.State != domain.ActivationActive || renewed.Activation.State != domain.ActivationActive {
		t.Fatalf("activation = %#v, want it still active after a recorded decision", renewed.Activation)
	}
	if renewed.Activation.TurnsUsed != 1 {
		t.Fatalf("turns used = %d, want the recorded decision counted once", renewed.Activation.TurnsUsed)
	}
	if renewed.Activation.LeaseExpiresAt == nil || !renewed.Activation.LeaseExpiresAt.After(claimExpiry) {
		t.Fatalf("lease expiry = %v, want it renewed past the claim's %v",
			renewed.Activation.LeaseExpiresAt, claimExpiry)
	}
	if !backlog.ActivationLeaseValid(renewed.Activation, fixture.now) {
		t.Fatal("the renewed lease is not live")
	}
	// A renewal extends the coordinator's belief in this overseer, never the
	// review's own maximum elapsed time.
	if renewed.Activation.Deadline == nil || !renewed.Activation.Deadline.Equal(*deadline) {
		t.Fatalf("deadline = %v, want the activation's original %v", renewed.Activation.Deadline, deadline)
	}

	// The renewed lease is authority: the same overseer's next decision passes
	// the guard the expired one failed.
	state, err := fixture.supervision.LoadSupervisionActivationState(context.Background(), activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if err := backlog.AuthorizeActivationDecision(state, domain.Actor{
		Kind: domain.ActorOverseer, Principal: fixture.coordinator.settings.principalID(),
		ActivationEpoch: activation.Epoch,
	}, activation.Epoch, state.Record.Revision, fixture.now); err != nil {
		t.Fatalf("the renewed activation was refused its next decision: %v", err)
	}
}
