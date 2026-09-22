package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

var supervisionTestTime = time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC)

func openSupervisionStore(t *testing.T, path string) *Store {
	t.Helper()
	store, err := OpenMigrated(path)
	if err != nil {
		t.Fatal(err)
	}
	store.SetClock(func() time.Time { return supervisionTestTime })
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func supervisionTestConfig() domain.SupervisionConfig {
	return domain.SupervisionConfig{
		Route:                 domain.ProviderRoute{ProviderInstanceID: "codex", Model: "gpt-5.6-sol"},
		PromptArtifactID:      "artifact-overseer",
		MaxActivations:        5,
		MaxTurnsPerActivation: 4,
		ActivationDeadline:    time.Hour,
	}
}

func supervisionOperator() domain.Actor {
	return domain.Actor{Kind: domain.ActorOperator, Principal: "operator-1"}
}

// seedSupervisedRun writes a two-task run whose second task is the one every
// enforcement test asks about, plus a ready attempt and a healthy worker.
func seedSupervisedRun(t *testing.T, store *Store, gates []domain.Gate) domain.WorkflowRun {
	t.Helper()
	ctx := context.Background()
	now := supervisionTestTime
	run := domain.WorkflowRun{
		ID: "run-1", WorkflowID: "workflow-1", GraphRevision: 1,
		Progress: domain.ProgressActive, Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	tasks := []domain.Task{
		{ID: "task-0", WorkflowID: "workflow-1", Name: "producer", Class: domain.TaskClassRequired},
		{ID: "task-1", WorkflowID: "workflow-1", Name: "protected", Class: domain.TaskClassRequired, Needs: []string{"task-0"}},
	}
	attempt := domain.Attempt{
		ID: "attempt-1", WorkflowRunID: run.ID, TaskID: "task-1", Number: 1,
		Progress: domain.ProgressReady, Control: domain.ControlUnassigned,
		Revision: 1, UpdatedAt: now,
	}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "workflow-1", Version: 1, Name: "workflow", Class: domain.TaskClassRequired, TaskIDs: []string{"task-0", "task-1"}, CreatedAt: now}},
		WorkflowRuns: []domain.WorkflowRun{run},
		Tasks:        tasks,
		Attempts:     []domain.Attempt{attempt},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveWorkerSnapshot(ctx, supervisionWorkerSnapshot()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutSupervision(ctx, SupervisionMaterialization{
		Record: domain.SupervisionRecord{RunID: run.ID, Config: supervisionTestConfig()},
		Gates:  gates,
	}); err != nil {
		t.Fatal(err)
	}
	return run
}

func supervisionWorkerSnapshot() domain.WorkerSnapshot {
	return domain.WorkerSnapshot{
		WorkerID: "normandy", WorkerEpoch: "worker-epoch-1", CoordinatorEpoch: 1, Sequence: 1,
		Connected: true, ObservedAt: supervisionTestTime, ValidUntil: supervisionTestTime.Add(time.Hour),
		Inventory: domain.WorkerInventory{
			ID: "normandy", AcceptBacklog: true, Health: domain.WorkerHealthReady,
			ObservedAt: supervisionTestTime,
		},
	}
}

func supervisionPlanCommit() domain.AssignmentPlanCommit {
	return domain.AssignmentPlanCommit{
		CoordinatorEpoch: 1, CommittedAt: supervisionTestTime,
		Items: []domain.AssignmentPlanItem{{
			ExpectedAttemptRevision: 1, WorkerEpoch: "worker-epoch-1", WorkerSnapshotSequence: 1,
			Assignment: domain.Assignment{
				ID: "assignment-1", AttemptID: "attempt-1", WorkerID: "normandy",
				Route:    domain.ProviderRoute{ProviderInstanceID: "codex", Model: "gpt-5.6-sol"},
				Estimate: &domain.TaskAdmissionEstimate{RemainingCost: 10, ExpectedRuntime: time.Hour, CheckpointMargin: time.Minute},
				State:    domain.AssignmentOffered, Epoch: 1,
				LeaseToken: "lease-token-1", DispatchToken: "dispatch-token-1", ThreadID: "thread-1",
			},
		}},
	}
}

func supervisionClaim() domain.AssignmentClaimRequest {
	return domain.AssignmentClaimRequest{
		CoordinatorEpoch: 1, WorkerID: "normandy", WorkerEpoch: "worker-epoch-1",
		AssignmentID: "assignment-1", AssignmentEpoch: 1, LeaseToken: "lease-token-1",
		ClaimedAt: supervisionTestTime.Add(time.Second), LeaseExpiresAt: supervisionTestTime.Add(time.Hour),
	}
}

func supervisionRunHold(id, requestID, reason string) HoldRequest {
	return HoldRequest{
		RunID: "run-1", HoldID: id, RequestID: requestID, Actor: supervisionOperator(),
		Scope: domain.HoldScope{Kind: domain.HoldScopeRun}, ExpectedGraphRevision: 1,
		Reason: reason, PlacedAt: supervisionTestTime,
	}
}

func liveAssignments(t *testing.T, store *Store) []domain.Assignment {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return records.Assignments
}

func TestSupervisionSchemaMigratesForwardAndRefusesNewerDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store := openSupervisionStore(t, path)
	var version int
	if err := store.db.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != currentSchemaVersion || currentSchemaVersion != 26 {
		t.Fatalf("schema version = %d, current = %d", version, currentSchemaVersion)
	}
	for _, table := range []string{
		"coordinator_supervision",
		"coordinator_supervision_activations",
		"coordinator_supervision_gates",
		"coordinator_supervision_decisions",
		"coordinator_supervision_holds",
		"coordinator_supervision_incidents",
		"coordinator_supervision_outbox",
		"coordinator_supervision_inbox",
		"coordinator_supervision_inbox_ack",
		"coordinator_supervision_receipts",
	} {
		var name string
		if err := store.db.QueryRow(
			`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name); err != nil {
			t.Fatalf("table %q missing after migration: %v", table, err)
		}
	}
	// Forward migration is idempotent: re-running it changes nothing and
	// records no second row for the same version.
	if err := store.Migrate(); err != nil {
		t.Fatalf("repeat migration: %v", err)
	}
	var rows int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM schema_version WHERE version = 18`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("schema version 18 recorded %d times", rows)
	}
	// An existing run gets no supervision row: absence is the unsupervised
	// case and no empty record is ever created.
	snapshot, err := store.LoadSupervisionSnapshot(context.Background(), "run-absent")
	if err != nil || snapshot.Supervised {
		t.Fatalf("absent run snapshot = %#v, err = %v", snapshot, err)
	}

	// An older binary refuses a newer database rather than opening it.
	if _, err := store.db.Exec(`INSERT INTO schema_version(version) VALUES (?)`, currentSchemaVersion+1); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(); err == nil || !strings.Contains(err.Error(), "newer than supported") {
		t.Fatalf("newer schema error = %v", err)
	}
}

func TestSupervisionOfferRacesHoldWithOneWinner(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	seedSupervisedRun(t, store, nil)
	ctx := context.Background()

	var wg sync.WaitGroup
	var offerErr, holdErr error
	var offered []domain.Assignment
	var decision SupervisionDecision
	wg.Add(2)
	go func() {
		defer wg.Done()
		offered, offerErr = store.CommitAssignmentPlan(ctx, supervisionPlanCommit())
	}()
	go func() {
		defer wg.Done()
		decision, holdErr = store.PlaceHold(ctx, supervisionRunHold("hold-1", "request-hold-1", "pause the run"))
	}()
	wg.Wait()
	if offerErr != nil {
		t.Fatalf("offer: %v", offerErr)
	}
	if holdErr != nil {
		t.Fatalf("hold: %v", holdErr)
	}
	if decision.Hold == nil || decision.Hold.State != domain.HoldActive {
		t.Fatalf("hold decision = %#v", decision)
	}
	// Whichever transaction committed first, no offer survives the hold: the
	// offer was refused by the predicate, or the hold released it in the same
	// transaction that placed it.
	for _, assignment := range liveAssignments(t, store) {
		if assignment.State == domain.AssignmentOffered {
			t.Fatalf("offer survived the hold: %#v (offered=%#v, released=%v)",
				assignment, offered, decision.ReleasedAssignmentIDs)
		}
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if records.Attempts[0].AssignmentID != "" {
		t.Fatalf("held attempt retains an assignment: %#v", records.Attempts[0])
	}
}

func TestSupervisionClaimRacesHoldWithOneWinner(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	seedSupervisedRun(t, store, nil)
	ctx := context.Background()
	if _, err := store.CommitAssignmentPlan(ctx, supervisionPlanCommit()); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var claimErr, holdErr error
	var claimed domain.Assignment
	var decision SupervisionDecision
	wg.Add(2)
	go func() {
		defer wg.Done()
		claimed, claimErr = store.ClaimAssignment(ctx, supervisionClaim())
	}()
	go func() {
		defer wg.Done()
		decision, holdErr = store.PlaceHold(ctx, supervisionRunHold("hold-1", "request-hold-1", "pause the run"))
	}()
	wg.Wait()
	if holdErr != nil {
		t.Fatalf("hold: %v", holdErr)
	}
	switch {
	case claimErr == nil:
		// The claim won the compare-and-set. It is past the start boundary, so
		// the hold reports it and never rolls it back.
		if claimed.State != domain.AssignmentClaimed {
			t.Fatalf("claimed assignment = %#v", claimed)
		}
		if len(decision.ReleasedAssignmentIDs) != 0 {
			t.Fatalf("hold released a claimed assignment: %#v", decision)
		}
		if len(decision.ClaimedTaskIDs) != 1 || decision.ClaimedTaskIDs[0] != "task-1" {
			t.Fatalf("hold did not report the started work: %#v", decision)
		}
	case errors.Is(claimErr, ErrAssignmentClaim):
		// The hold won. The offer is released and the worker was told no.
		if !strings.Contains(claimErr.Error(), string(domain.SupervisionBlockerRunHold)) &&
			len(decision.ReleasedAssignmentIDs) != 1 {
			t.Fatalf("claim refusal = %v, hold decision = %#v", claimErr, decision)
		}
		for _, assignment := range liveAssignments(t, store) {
			if assignment.State == domain.AssignmentOffered || assignment.State == domain.AssignmentClaimed {
				t.Fatalf("held assignment is still live: %#v", assignment)
			}
		}
	default:
		t.Fatalf("unexpected claim error: %v", claimErr)
	}
}

func TestSupervisionHoldRefusesOfferAndReleaseRestoresIt(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	seedSupervisedRun(t, store, nil)
	ctx := context.Background()
	if _, err := store.PlaceHold(ctx, supervisionRunHold("hold-1", "request-hold-1", "pause the run")); err != nil {
		t.Fatal(err)
	}
	offered, err := store.CommitAssignmentPlan(ctx, supervisionPlanCommit())
	if err != nil || len(offered) != 0 {
		t.Fatalf("held offer: assignments=%#v err=%v", offered, err)
	}
	released, err := store.ReleaseHold(ctx, HoldReleaseRequest{
		RunID: "run-1", HoldID: "hold-1", RequestID: "request-release-1",
		Actor: supervisionOperator(), Reason: "work may resume", ReleasedAt: supervisionTestTime,
	})
	if err != nil || released.Hold.State != domain.HoldReleased {
		t.Fatalf("release: %#v %v", released, err)
	}
	offered, err = store.CommitAssignmentPlan(ctx, supervisionPlanCommit())
	if err != nil || len(offered) != 1 {
		t.Fatalf("offer after release: assignments=%#v err=%v", offered, err)
	}
	if _, err := store.ClaimAssignment(ctx, supervisionClaim()); err != nil {
		t.Fatalf("claim after release: %v", err)
	}
}

func TestSupervisionGateRejectionReleasesOutstandingOffer(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	gate := domain.Gate{
		RunID: "run-1", State: domain.GateAccepted, GraphRevision: 1,
		Definition: domain.GateDefinition{
			ID: "gate-1", Name: "review", ObservedTaskIDs: []string{"task-0"}, ProtectedTaskIDs: []string{"task-1"},
		},
	}
	seedSupervisedRun(t, store, []domain.Gate{gate})
	ctx := context.Background()
	offered, err := store.CommitAssignmentPlan(ctx, supervisionPlanCommit())
	if err != nil || len(offered) != 1 {
		t.Fatalf("accepted gate must admit the offer: %#v %v", offered, err)
	}
	// An accepted gate is moved back to pending evidence by a replaced
	// producer, which narrows readiness and must release the outstanding offer
	// in the same transaction.
	decision, err := store.PlaceHold(ctx, HoldRequest{
		RunID: "run-1", HoldID: "hold-branch", RequestID: "request-branch-hold",
		Actor:                 supervisionOperator(),
		Scope:                 domain.HoldScope{Kind: domain.HoldScopeBranch, BranchRootTaskID: "task-0"},
		ExpectedGraphRevision: 1, Reason: "producer is being replaced", PlacedAt: supervisionTestTime,
	})
	if err != nil {
		t.Fatalf("branch hold: %v", err)
	}
	if decision.Hold == nil || !reflect.DeepEqual(decision.Hold.ResolvedTaskIDs, []string{"task-0", "task-1"}) {
		t.Fatalf("branch closure = %#v", decision.Hold)
	}
	if len(decision.ReleasedAssignmentIDs) != 1 || decision.ReleasedAssignmentIDs[0] != "assignment-1" {
		t.Fatalf("branch hold released = %#v", decision.ReleasedAssignmentIDs)
	}
	if decision.AttemptRevision == 0 {
		t.Fatalf("the hold recorded no attempt revision boundary: %#v", decision)
	}
	for _, assignment := range liveAssignments(t, store) {
		if assignment.State != domain.AssignmentReleased {
			t.Fatalf("assignment = %#v", assignment)
		}
	}
}

func TestSupervisionDecisionInvalidatesConcurrentSettlementProjection(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	ctx := context.Background()
	now := supervisionTestTime
	run := domain.WorkflowRun{
		ID: "run-1", WorkflowID: "workflow-1", GraphRevision: 1,
		Progress: domain.ProgressActive, Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	tasks := []domain.Task{{ID: "task-1", WorkflowID: "workflow-1", Name: "only", Class: domain.TaskClassRequired}}
	bound, err := domain.BindRunSink(run, tasks)
	if err != nil {
		t.Fatal(err)
	}
	attempts := []domain.Attempt{{
		ID: "attempt-1", WorkflowRunID: run.ID, TaskID: "task-1", Number: 1, Revision: 1,
		Progress: domain.ProgressFailed, Control: domain.ControlStopped, UpdatedAt: now,
	}}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{bound}, Tasks: tasks, Attempts: attempts,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutSupervision(ctx, SupervisionMaterialization{
		Record: domain.SupervisionRecord{RunID: run.ID, Config: supervisionTestConfig()},
	}); err != nil {
		t.Fatal(err)
	}
	before := loadSupervisionProjectionFixture(t, store, run.ID)
	if before.Supervision == nil || before.Supervision.Record == nil {
		t.Fatalf("supervision is not part of the fenced read set: %#v", before.Supervision)
	}
	projected, err := domain.ProjectRunSink(before.Run, before.Tasks, before.Attempts, before.Assignments, now)
	if err != nil {
		t.Fatal(err)
	}
	projected.Revision++
	projected.UpdatedAt = now

	if _, err := store.OpenReviewIncident(ctx, IncidentRequest{
		RunID: run.ID, IncidentID: "incident-1", RequestID: "request-incident-1",
		Actor: supervisionOperator(), SourceEventID: "event-1", SourceTaskID: "task-1",
		SourceAttemptID: "attempt-1", RequiredDisposition: domain.DispositionConcludeFailure,
		Reason: "task failed and needs a disposition", OpenedAt: now,
	}); err != nil {
		t.Fatalf("open incident: %v", err)
	}
	if err := store.CommitWorkflowProjection(ctx, before, projected, before.Attempts, now); !errors.Is(err, ErrStaleWorkflowProjection) {
		t.Fatalf("settlement raced an incident without refusal: %v", err)
	}

	// Once the incident is resolved the settlement projection is recomputed
	// from the new read set and publishes normally.
	if _, err := store.ResolveReviewIncident(ctx, IncidentResolutionRequest{
		RunID: run.ID, IncidentID: "incident-1", RequestID: "request-resolve-1",
		Actor: supervisionOperator(), Event: domain.IncidentEventConcludeFailure,
		ExpectedRevision: 1, Outcome: domain.IncidentOutcomeConcludeFailure,
		Reason: "acknowledged terminal failure for settlement", ResolvedAt: now,
	}); err != nil {
		t.Fatalf("resolve incident: %v", err)
	}
	after := loadSupervisionProjectionFixture(t, store, run.ID)
	settled, err := domain.ProjectRunSink(after.Run, after.Tasks, after.Attempts, after.Assignments, now)
	if err != nil {
		t.Fatal(err)
	}
	settled.Revision++
	settled.UpdatedAt = now
	if err := store.CommitWorkflowProjection(ctx, after, settled, after.Attempts, now); err != nil {
		t.Fatalf("settlement after resolution: %v", err)
	}
}

func loadSupervisionProjectionFixture(t *testing.T, store *Store, runID string) WorkflowProjectionSnapshot {
	t.Helper()
	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	snapshot, err := loadWorkflowProjectionTx(context.Background(), tx, runID)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestSupervisionReceiptsReplayAndRefuseChangedPayload(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	seedSupervisedRun(t, store, nil)
	ctx := context.Background()
	request := supervisionRunHold("hold-1", "request-hold-1", "pause the run")
	first, err := store.PlaceHold(ctx, request)
	if err != nil || first.Replay {
		t.Fatalf("first hold: %#v %v", first, err)
	}
	replay, err := store.PlaceHold(ctx, request)
	if err != nil {
		t.Fatalf("replay hold: %v", err)
	}
	if !replay.Replay || replay.SupervisionRevision != first.SupervisionRevision ||
		!reflect.DeepEqual(replay.Hold, first.Hold) {
		t.Fatalf("replay = %#v, first = %#v", replay, first)
	}
	changed := request
	changed.Reason = "a different reason"
	if _, err := store.PlaceHold(ctx, changed); !errors.Is(err, ErrSupervisionRequestConflict) {
		t.Fatalf("changed payload error = %v", err)
	}
	// The refused request wrote nothing: exactly one hold exists.
	projection, err := store.LoadSupervisionProjection(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(projection.Holds) != 1 || projection.Holds[0].Reason != "pause the run" {
		t.Fatalf("holds = %#v", projection.Holds)
	}
}

func TestSupervisionStateReconstructsAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store := openSupervisionStore(t, path)
	gate := domain.Gate{
		RunID: "run-1", State: domain.GateReadyForReview, GraphRevision: 1,
		Definition: domain.GateDefinition{
			ID: "gate-1", Name: "review", ObservedTaskIDs: []string{"task-0"}, ProtectedTaskIDs: []string{"task-1"},
		},
	}
	seedSupervisedRun(t, store, []domain.Gate{gate})
	ctx := context.Background()
	if _, err := store.PlaceHold(ctx, supervisionRunHold("hold-1", "request-hold-1", "pause the run")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenReviewIncident(ctx, IncidentRequest{
		RunID: "run-1", IncidentID: "incident-1", RequestID: "request-incident-1",
		Actor: supervisionOperator(), SourceEventID: "event-1", SourceTaskID: "task-1",
		GateID: "gate-1", RequiredDisposition: domain.DispositionGateDecision,
		Reason: "gate is ready for review", OpenedAt: supervisionTestTime,
	}); err != nil {
		t.Fatal(err)
	}
	before, err := store.LoadSupervisionSnapshot(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	beforeProjection, err := store.LoadSupervisionProjection(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openSupervisionStore(t, path)
	after, err := reopened.LoadSupervisionSnapshot(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("snapshot changed across restart:\nbefore = %#v\nafter  = %#v", before, after)
	}
	afterProjection, err := reopened.LoadSupervisionProjection(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(beforeProjection, afterProjection) {
		t.Fatalf("projection changed across restart:\nbefore = %#v\nafter  = %#v", beforeProjection, afterProjection)
	}
	// The reconstructed state still refuses dispatch.
	offered, err := reopened.CommitAssignmentPlan(ctx, supervisionPlanCommit())
	if err != nil || len(offered) != 0 {
		t.Fatalf("reconstructed hold admitted an offer: %#v %v", offered, err)
	}
}

func TestSupervisionGateDecisionBindsEvidenceAndFencesRevisions(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	gate := domain.Gate{
		RunID: "run-1", State: domain.GateReadyForReview, GraphRevision: 1,
		Definition: domain.GateDefinition{
			ID: "gate-1", Name: "review", ObservedTaskIDs: []string{"task-0"}, ProtectedTaskIDs: []string{"task-1"},
		},
	}
	seedSupervisedRun(t, store, []domain.Gate{gate})
	ctx := context.Background()
	producer := domain.Attempt{
		ID: "attempt-0", WorkflowRunID: "run-1", TaskID: "task-0", Number: 1,
		Progress: domain.ProgressSucceeded, Control: domain.ControlStopped,
		Revision: 7, UpdatedAt: supervisionTestTime,
	}
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{Attempts: []domain.Attempt{producer}}); err != nil {
		t.Fatal(err)
	}
	evidence := domain.EvidenceSnapshot{
		ID: "evidence-1", GraphRevision: 1, TakenAt: supervisionTestTime,
		Producers: []domain.ProducerEvidence{{TaskID: "task-0", AttemptID: "attempt-0", ResultRevision: 7}},
	}
	base := GateDecisionRequest{
		RunID: "run-1", GateID: "gate-1", RequestID: "request-accept-1",
		Actor: supervisionOperator(), ExpectedGraphRevision: 1, ExpectedGateRevision: 1,
		Evidence: evidence, Outcome: domain.GateDecisionAccept,
		Reason: "outputs match the rubric", DecidedAt: supervisionTestTime,
	}

	stale := base
	stale.RequestID, stale.ExpectedGateRevision = "request-stale", 99
	if _, err := store.DecideGate(ctx, stale); !errors.Is(err, domain.ErrSupervisionStaleRevision) {
		t.Fatalf("stale gate revision error = %v", err)
	}
	retried := base
	retried.RequestID = "request-retried"
	retried.Evidence.Producers[0].ResultRevision = 6
	if _, err := store.DecideGate(ctx, retried); !errors.Is(err, domain.ErrSupervisionStaleRevision) {
		t.Fatalf("retried producer evidence error = %v", err)
	}
	// Evidence is rebuilt because the refused request must not have mutated it.
	base.Evidence = domain.EvidenceSnapshot{
		ID: "evidence-1", GraphRevision: 1, TakenAt: supervisionTestTime,
		Producers: []domain.ProducerEvidence{{TaskID: "task-0", AttemptID: "attempt-0", ResultRevision: 7}},
	}
	accepted, err := store.DecideGate(ctx, base)
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	if accepted.Gate.State != domain.GateAccepted || accepted.Gate.Revision != 2 ||
		accepted.GateDecision.Actor != supervisionOperator() {
		t.Fatalf("accepted gate = %#v", accepted)
	}
	offered, err := store.CommitAssignmentPlan(ctx, supervisionPlanCommit())
	if err != nil || len(offered) != 1 {
		t.Fatalf("accepted gate must admit the protected task: %#v %v", offered, err)
	}
	// History survives: the decision row is append-only.
	projection, err := store.LoadSupervisionProjection(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(projection.Decisions) != 1 || projection.Decisions[0].Outcome != domain.GateDecisionAccept {
		t.Fatalf("decisions = %#v", projection.Decisions)
	}
}

func TestSupervisionAdminStartRefusedByHeldGate(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	gate := domain.Gate{
		RunID: "run-1", State: domain.GateHeld, GraphRevision: 1,
		Definition: domain.GateDefinition{
			ID: "gate-1", Name: "review", ObservedTaskIDs: []string{"task-0"}, ProtectedTaskIDs: []string{"task-1"},
		},
	}
	seedSupervisedRun(t, store, []domain.Gate{gate})
	ctx := context.Background()
	now := supervisionTestTime

	command := domain.AdminCommand{
		ID: "start-1", Kind: domain.AdminCommandStart, TargetType: domain.AdminTargetAttempt,
		TargetID: "attempt-1", ExpectedRevision: 1, Reason: "operator wants it started now",
		RequestedBy: "operator-1", State: domain.AdminCommandPending, CreatedAt: now,
	}
	if _, err := store.SubmitAdminCommand(ctx, command); err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	workers := []domain.WorkerSnapshot{supervisionWorkerSnapshot()}
	fingerprint, err := domain.AdminSafetyFingerprint(domain.AdminSafetyState{
		Tasks: records.Tasks, Attempts: records.Attempts, Assignments: records.Assignments, Workers: workers,
	})
	if err != nil {
		t.Fatal(err)
	}
	validUntil, ok := domain.AdminWorkerSafetyValidUntil(workers, now)
	if !ok {
		t.Fatal("worker safety validity is not available")
	}
	var started domain.Attempt
	for _, attempt := range records.Attempts {
		if attempt.ID == "attempt-1" {
			started = attempt
		}
	}
	started.Progress, started.Revision, started.UpdatedAt = domain.ProgressReady, 2, now
	decision, err := store.ApplyAdminCommand(ctx, domain.AdminCommandApplication{
		CommandID: command.ID, ExpectedCommandState: domain.AdminCommandPending, ExpectedTargetRevision: 1,
		SafetyFingerprint: fingerprint, SafetyValidUntil: &validUntil,
		Attempt: &started, State: domain.AdminCommandApplied, AppliedAt: now,
	})
	if err != nil {
		t.Fatalf("apply admin start: %v", err)
	}
	if decision.Command.State != domain.AdminCommandRejected {
		t.Fatalf("admin start bypassed a held gate: %#v", decision.Command)
	}
	if !strings.Contains(decision.Command.Failure, string(domain.SupervisionBlockerGateHeld)) {
		t.Fatalf("admin start failure = %q", decision.Command.Failure)
	}
	loaded, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range loaded.Attempts {
		if attempt.ID == "attempt-1" && attempt.Revision != 1 {
			t.Fatalf("refused start mutated the attempt: %#v", attempt)
		}
	}
}

func TestSupervisionActivationTransitionsAreBudgetedAndFenced(t *testing.T) {
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	seedSupervisedRun(t, store, nil)
	ctx := context.Background()
	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO coordinator_supervision_inbox(id, run_id, sequence, consumed, record)
		VALUES ('event-1', 'run-1', 1, 0, '{"id":"event-1"}')`); err != nil {
		t.Fatal(err)
	}
	overseer := domain.Actor{Kind: domain.ActorOverseer, Principal: "overseer:run-1", ActivationEpoch: 1}
	dispatch, err := store.RecordActivationTransition(ctx, ActivationRequest{
		RunID: "run-1", ActivationID: "activation-1", RequestID: "request-trigger-1",
		Actor: overseer, Event: domain.ActivationEventTriggerFired, ExpectedEpoch: 1,
		ExpectedSupervisionRevision: 1, DispatchIdentity: "supervision:run-1:1",
		TransitionedAt: supervisionTestTime,
	})
	if err != nil {
		t.Fatalf("trigger: %v", err)
	}
	if dispatch.Activation.State != domain.ActivationPendingDispatch ||
		dispatch.Activation.DispatchIdentity != "supervision:run-1:1" {
		t.Fatalf("activation = %#v", dispatch.Activation)
	}
	if dispatch.Record.ActivationsUsed != 0 {
		t.Fatalf("pending dispatch spent budget: %#v", dispatch.Record)
	}
	expiry := supervisionTestTime.Add(time.Hour)
	confirmed, err := store.RecordActivationTransition(ctx, ActivationRequest{
		RunID: "run-1", ActivationID: "activation-1", RequestID: "request-confirm-1",
		Actor: overseer, Event: domain.ActivationEventDispatchConfirmed, ExpectedEpoch: 1,
		ExpectedSupervisionRevision: dispatch.SupervisionRevision,
		Observations:                ActivationObservations{LeaseValid: true},
		LeaseToken:                  "lease-overseer-1", LeaseExpiresAt: &expiry,
		TransitionedAt: supervisionTestTime,
	})
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if confirmed.Activation.State != domain.ActivationActive || confirmed.Record.ActivationsUsed != 1 {
		t.Fatalf("confirmed = %#v record = %#v", confirmed.Activation, confirmed.Record)
	}
	// A decision from a stale epoch is fenced out even when it arrives later.
	_, err = store.RecordActivationTransition(ctx, ActivationRequest{
		RunID: "run-1", ActivationID: "activation-1", RequestID: "request-stale-epoch",
		Actor: overseer, Event: domain.ActivationEventDecisionRecorded, ExpectedEpoch: 99,
		ExpectedSupervisionRevision: confirmed.SupervisionRevision,
		Observations:                ActivationObservations{LeaseValid: true},
		TransitionedAt:              supervisionTestTime,
	})
	if !errors.Is(err, domain.ErrSupervisionStaleRevision) {
		t.Fatalf("stale epoch error = %v", err)
	}
	// A second activation cannot be valid at the same time.
	_, err = store.RecordActivationTransition(ctx, ActivationRequest{
		RunID: "run-1", ActivationID: "activation-2", RequestID: "request-second",
		Actor: overseer, Event: domain.ActivationEventTriggerFired,
		ExpectedSupervisionRevision: confirmed.SupervisionRevision,
		TransitionedAt:              supervisionTestTime,
	})
	if !errors.Is(err, domain.ErrSupervisionPrerequisite) {
		t.Fatalf("second activation error = %v", err)
	}
}
