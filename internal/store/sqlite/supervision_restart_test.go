package sqlite

// Restart at every supervision transition.
//
// The plan requires that a restart reconstructs gates, holds, inbox, epochs and
// reservations before scheduling. The dangerous moment is not a restart at rest:
// it is a restart in the gap between a decision's transaction and the dispatch
// that decision permits, because that is the only window in which the
// coordinator's intention exists nowhere but in the process that is about to
// die. Every test here closes and reopens the database in exactly that gap, and
// then asks two questions: is the reconstructed state the same state, and does
// the dispatch the decision permitted happen exactly once rather than zero or
// twice.

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// supervisionRestartCheck captures everything a restart must preserve, so a
// single comparison covers the snapshot, the decision history, the activation
// and the inbox rather than four partial ones.
type supervisionRestartCheck struct {
	Snapshot   domain.SupervisionSnapshot
	Projection SupervisionProjection
	Activation domain.Activation
	Record     domain.SupervisionRecord
	Inbox      []SupervisionInboxRow
}

func supervisionRestartState(t *testing.T, store *Store, runID string) supervisionRestartCheck {
	t.Helper()
	ctx := context.Background()
	snapshot, err := store.LoadSupervisionSnapshot(ctx, runID)
	if err != nil {
		t.Fatalf("load supervision snapshot of %q: %v", runID, err)
	}
	projection, err := store.LoadSupervisionProjection(ctx, runID)
	if err != nil {
		t.Fatalf("load supervision projection of %q: %v", runID, err)
	}
	rows, err := store.LoadSupervisionActivationRows(ctx, runID)
	if err != nil {
		t.Fatalf("load activation rows of %q: %v", runID, err)
	}
	return supervisionRestartCheck{
		Snapshot: snapshot, Projection: projection,
		Activation: rows.Activation, Record: rows.Record, Inbox: rows.Inbox,
	}
}

// restartSupervisionStore closes the store and opens the same file again, which
// is the coordinator restarting between a decision and its consequence.
func restartSupervisionStore(t *testing.T, store *Store, path, runID string) (*Store, supervisionRestartCheck) {
	t.Helper()
	before := supervisionRestartState(t, store, runID)
	if err := store.Close(); err != nil {
		t.Fatalf("close the store: %v", err)
	}
	reopened := openSupervisionStore(t, path)
	after := supervisionRestartState(t, reopened, runID)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("supervision state changed across restart:\nbefore = %#v\nafter  = %#v", before, after)
	}
	return reopened, after
}

// offeredAssignmentsOfAttempt counts the live offers of one attempt, which is
// how "dispatch resumed exactly once" is counted.
func offeredAssignmentsOfAttempt(t *testing.T, store *Store, attemptID string) []domain.Assignment {
	t.Helper()
	records, err := store.LoadCoordinatorRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var offers []domain.Assignment
	for _, assignment := range records.Assignments {
		if assignment.AttemptID == attemptID && assignment.State == domain.AssignmentOffered {
			offers = append(offers, assignment)
		}
	}
	return offers
}

// A gate accepted and then a restart: the acceptance survives, and the dispatch
// it permitted happens once.
func TestSupervisionRestartAfterGateAcceptanceResumesDispatchOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store := openSupervisionStore(t, path)
	ctx := context.Background()
	seedSupervisedRun(t, store, []domain.Gate{supervisionReadyGate()})
	seedSucceededProducer(t, store, 7)

	accepted, err := store.DecideGate(ctx, supervisionAcceptRequest("request-accept", 1, 7))
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	if accepted.Gate.State != domain.GateAccepted {
		t.Fatalf("accepted gate = %q", accepted.Gate.State)
	}

	// The restart lands between the acceptance and the offer it permits.
	store, after := restartSupervisionStore(t, store, path, "run-1")
	if after.Snapshot.Gates[0].State != domain.GateAccepted {
		t.Fatalf("reconstructed gate = %q, want the acceptance preserved", after.Snapshot.Gates[0].State)
	}
	if len(after.Projection.Decisions) != 1 {
		t.Fatalf("reconstructed decisions = %#v, want the acceptance on the record", after.Projection.Decisions)
	}

	offered, err := store.CommitAssignmentPlan(ctx, supervisionPlanCommit())
	if err != nil || len(offered) != 1 {
		t.Fatalf("dispatch after restart: assignments=%#v err=%v", offered, err)
	}
	// A second pass of the same plan, which is what a coordinator that is not
	// sure whether it already committed would do, adds no second offer.
	_, _ = store.CommitAssignmentPlan(ctx, supervisionPlanCommit())
	if offers := offeredAssignmentsOfAttempt(t, store, "attempt-1"); len(offers) != 1 {
		t.Fatalf("live offers of the protected attempt = %d, want exactly one", len(offers))
	}

	// The replayed acceptance is still the same answer after a restart: the
	// receipt is durable, not process state.
	replay, err := store.DecideGate(ctx, supervisionAcceptRequest("request-accept", 1, 7))
	if err != nil {
		t.Fatalf("replay the acceptance after restart: %v", err)
	}
	if !replay.Replay || replay.SupervisionRevision != accepted.SupervisionRevision {
		t.Fatalf("replay = %#v, want the first answer replayed", replay)
	}
}

// A hold placed and then a restart: the reconstructed hold still refuses the
// offer, and the release the operator issues after the restart admits it once.
func TestSupervisionRestartAfterHoldPreservesReservationsAndReleasesOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store := openSupervisionStore(t, path)
	ctx := context.Background()
	seedSupervisedRun(t, store, nil)

	hold, err := store.PlaceHold(ctx, supervisionRunHold("hold-1", "request-hold-1", "pause the run"))
	if err != nil {
		t.Fatalf("hold: %v", err)
	}
	if len(hold.ReleasedAssignmentIDs) != 0 || len(hold.ClaimedTaskIDs) != 0 {
		t.Fatalf("hold receipt = %#v, want an empty start boundary before anything was offered", hold)
	}

	store, after := restartSupervisionStore(t, store, path, "run-1")
	if len(after.Snapshot.Holds) != 1 || after.Snapshot.Holds[0].State != domain.HoldActive {
		t.Fatalf("reconstructed holds = %#v, want the active hold", after.Snapshot.Holds)
	}
	offered, err := store.CommitAssignmentPlan(ctx, supervisionPlanCommit())
	if err != nil || len(offered) != 0 {
		t.Fatalf("the reconstructed hold admitted an offer: %#v %v", offered, err)
	}

	released, err := store.ReleaseHold(ctx, HoldReleaseRequest{
		RunID: "run-1", HoldID: "hold-1", RequestID: "request-release-1",
		Actor: supervisionOperator(), Reason: "work may resume", ReleasedAt: supervisionTestTime,
	})
	if err != nil || released.Hold.State != domain.HoldReleased {
		t.Fatalf("release: %#v %v", released, err)
	}

	// A second restart, this time between the release and the dispatch it
	// permits.
	store, afterRelease := restartSupervisionStore(t, store, path, "run-1")
	if len(afterRelease.Snapshot.Holds) != 0 {
		t.Fatalf("reconstructed holds after release = %#v, want none active", afterRelease.Snapshot.Holds)
	}
	if offered, err = store.CommitAssignmentPlan(ctx, supervisionPlanCommit()); err != nil || len(offered) != 1 {
		t.Fatalf("dispatch after the released hold: assignments=%#v err=%v", offered, err)
	}
	_, _ = store.CommitAssignmentPlan(ctx, supervisionPlanCommit())
	if offers := offeredAssignmentsOfAttempt(t, store, "attempt-1"); len(offers) != 1 {
		t.Fatalf("live offers after the released hold = %d, want exactly one", len(offers))
	}
}

// An activation planned and then a restart: the epoch, the dispatch identity
// and the unconsumed inbox are all reconstructed, and the dispatch is confirmed
// once rather than spending the run's budget twice.
func TestSupervisionRestartBetweenActivationPlanAndDispatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store := openSupervisionStore(t, path)
	ctx := context.Background()
	seedSupervisedRun(t, store, nil)
	seedSupervisionInboxEvent(t, store, "event-1", 1)
	overseer := domain.Actor{Kind: domain.ActorOverseer, Principal: "overseer:run-1", ActivationEpoch: 1}

	planned, err := store.RecordActivationTransition(ctx, ActivationRequest{
		RunID: "run-1", ActivationID: "activation-1", RequestID: "request-trigger",
		Actor: overseer, Event: domain.ActivationEventTriggerFired, ExpectedEpoch: 1,
		ExpectedSupervisionRevision: 1, DispatchIdentity: "supervision:run-1:1",
		TransitionedAt: supervisionTestTime,
	})
	if err != nil {
		t.Fatalf("trigger: %v", err)
	}

	// The restart lands between the planned activation and the dispatch it
	// asks for, which is the window in which a second coordinator could wake a
	// second overseer.
	store, after := restartSupervisionStore(t, store, path, "run-1")
	if after.Activation.State != domain.ActivationPendingDispatch ||
		after.Activation.DispatchIdentity != "supervision:run-1:1" ||
		after.Activation.Epoch != 1 || after.Record.ActivationEpoch != 1 {
		t.Fatalf("reconstructed activation = %#v (record epoch %d)", after.Activation, after.Record.ActivationEpoch)
	}
	if after.Record.ActivationsUsed != 0 {
		t.Fatalf("a restart spent activation budget: %#v", after.Record)
	}
	if pending := pendingSupervisionEventIDs(t, store, "run-1"); len(pending) != 1 || pending[0] != "event-1" {
		t.Fatalf("reconstructed inbox = %v, want the event that has not been reviewed", pending)
	}

	// The retry of the undelivered dispatch after the restart reuses the
	// original identity, so the resumed dispatch is the same dispatch.
	retried, err := store.RecordActivationTransition(ctx, ActivationRequest{
		RunID: "run-1", ActivationID: "activation-1", RequestID: "request-undelivered",
		Actor: overseer, Event: domain.ActivationEventDispatchUndelivered, ExpectedEpoch: 1,
		ExpectedSupervisionRevision: planned.SupervisionRevision,
		TransitionedAt:              supervisionTestTime,
	})
	if err != nil {
		t.Fatalf("retry after restart: %v", err)
	}
	if retried.Activation.DispatchIdentity != planned.Activation.DispatchIdentity ||
		retried.Record.ActivationsUsed != 0 {
		t.Fatalf("resumed dispatch = %#v (used %d), want the original identity and no budget spent",
			retried.Activation, retried.Record.ActivationsUsed)
	}

	expiry := supervisionTestTime.Add(time.Hour)
	confirm := ActivationRequest{
		RunID: "run-1", ActivationID: "activation-1", RequestID: "request-confirm",
		Actor: overseer, Event: domain.ActivationEventDispatchConfirmed, ExpectedEpoch: 1,
		ExpectedSupervisionRevision: retried.SupervisionRevision,
		Observations:                ActivationObservations{LeaseValid: true},
		LeaseToken:                  "lease-1", LeaseExpiresAt: &expiry,
		TransitionedAt: supervisionTestTime,
	}
	confirmed, err := store.RecordActivationTransition(ctx, confirm)
	if err != nil {
		t.Fatalf("confirm after restart: %v", err)
	}
	if confirmed.Record.ActivationsUsed != 1 {
		t.Fatalf("activations used = %d, want one", confirmed.Record.ActivationsUsed)
	}

	// A restart between the confirmation and whatever the overseer does next
	// does not spend a second activation when the confirmation is redelivered.
	store, _ = restartSupervisionStore(t, store, path, "run-1")
	replay, err := store.RecordActivationTransition(ctx, confirm)
	if err != nil {
		t.Fatalf("redelivered confirmation after restart: %v", err)
	}
	if !replay.Replay || replay.Record.ActivationsUsed != 1 {
		t.Fatalf("redelivered confirmation = %#v, want the first answer replayed once", replay)
	}
}

// An incident opened and then a restart: it is still open, it still withholds
// settlement, and resolving it after the restart settles the run once.
func TestSupervisionRestartAfterIncidentKeepsTheSettlementBarrier(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store := openSupervisionStore(t, path)
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
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		WorkflowRuns: []domain.WorkflowRun{bound}, Tasks: tasks,
		Attempts: []domain.Attempt{{
			ID: "attempt-1", WorkflowRunID: run.ID, TaskID: "task-1", Number: 1, Revision: 1,
			Progress: domain.ProgressFailed, Control: domain.ControlStopped, UpdatedAt: now,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutSupervision(ctx, SupervisionMaterialization{
		Record: domain.SupervisionRecord{RunID: run.ID, Config: supervisionTestConfig()},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenReviewIncident(ctx, IncidentRequest{
		RunID: run.ID, IncidentID: "incident-1", RequestID: "request-incident-1",
		Actor: supervisionOperator(), SourceEventID: "event-1", SourceTaskID: "task-1",
		SourceAttemptID: "attempt-1", RequiredDisposition: domain.DispositionConcludeFailure,
		Reason: "the task failed and needs a disposition", OpenedAt: now,
	}); err != nil {
		t.Fatalf("open incident: %v", err)
	}

	// The restart lands between the incident and the settlement it withholds.
	store, after := restartSupervisionStore(t, store, path, run.ID)
	if len(after.Projection.Incidents) != 1 || after.Projection.Incidents[0].State != domain.IncidentOpen {
		t.Fatalf("reconstructed incidents = %#v, want one still open", after.Projection.Incidents)
	}
	verdict := domain.SupervisionSettlementBarrier(domain.SupervisionBarrier{
		Supervised: after.Snapshot.Supervised,
		Gates:      after.Snapshot.Gates,
		Incidents:  after.Projection.Incidents,
	}, false)
	if verdict.Settles || verdict.Reason != domain.SinkBarrierUnresolvedIncident ||
		verdict.IncidentID != "incident-1" {
		t.Fatalf("settlement barrier after restart = %#v, want the open incident withholding", verdict)
	}

	if _, err := store.ResolveReviewIncident(ctx, IncidentResolutionRequest{
		RunID: run.ID, IncidentID: "incident-1", RequestID: "request-resolve-1",
		Actor: supervisionOperator(), Event: domain.IncidentEventConcludeFailure,
		ExpectedRevision: 1, Outcome: domain.IncidentOutcomeConcludeFailure,
		Reason: "acknowledged terminal failure for settlement", ResolvedAt: now,
	}); err != nil {
		t.Fatalf("resolve incident: %v", err)
	}

	// A second restart, between the resolution and the settlement it permits.
	store, resolved := restartSupervisionStore(t, store, path, run.ID)
	if resolved.Projection.Incidents[0].State != domain.IncidentResolved {
		t.Fatalf("reconstructed incident = %q, want resolved", resolved.Projection.Incidents[0].State)
	}
	settleFrom := loadSupervisionProjectionFixture(t, store, run.ID)
	settled, err := domain.ProjectRunSink(settleFrom.Run, settleFrom.Tasks,
		settleFrom.Attempts, settleFrom.Assignments, now)
	if err != nil {
		t.Fatal(err)
	}
	settled.Revision++
	settled.UpdatedAt = now
	if err := store.CommitWorkflowProjection(ctx, settleFrom, settled, settleFrom.Attempts, now); err != nil {
		t.Fatalf("settlement after the resolved incident: %v", err)
	}
}
