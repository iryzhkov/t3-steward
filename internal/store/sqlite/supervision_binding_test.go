package sqlite

// The bindings Lane B7 added to this store: the inbox, the outbox, the whole
// activation plan, the admin read side, and the branch closure. Each one is the
// durable half of an interface another package declares, so each test asserts
// the fence or the idempotency that interface promises rather than the shape of
// a row.

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestSupervisionInboxAssignsSequencesAndIgnoresRepeats(t *testing.T) {
	ctx := context.Background()
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	seedSupervisedRun(t, store, nil)

	first := []SupervisionInboxRow{
		{ID: "event-1", Record: json.RawMessage(`{"id":"event-1"}`)},
		{ID: "event-2", Record: json.RawMessage(`{"id":"event-2"}`)},
	}
	written, err := store.AppendSupervisionInbox(ctx, "run-1", first)
	if err != nil || written != 2 {
		t.Fatalf("first append wrote %d rows (err %v), want 2", written, err)
	}
	// At-least-once delivery: the same event arriving again adds nothing and is
	// not resequenced, so no activation is woken twice for it.
	repeat, err := store.AppendSupervisionInbox(ctx, "run-1", append(first,
		SupervisionInboxRow{ID: "event-3", Record: json.RawMessage(`{"id":"event-3"}`)}))
	if err != nil || repeat != 1 {
		t.Fatalf("repeated append wrote %d rows (err %v), want only the new one", repeat, err)
	}
	rows, err := store.ListSupervisionInbox(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	var sequences []int64
	for _, row := range rows {
		if row.Consumed {
			t.Fatalf("event %q is consumed before any activation ran", row.ID)
		}
		sequences = append(sequences, row.Sequence)
	}
	if !reflect.DeepEqual(sequences, []int64{1, 2, 3}) {
		t.Fatalf("sequences = %v, want a dense run-local order", sequences)
	}
}

func TestSupervisionActivationCommitFencesOnTheRecordRevision(t *testing.T) {
	ctx := context.Background()
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	seedSupervisedRun(t, store, nil)
	if _, err := store.AppendSupervisionInbox(ctx, "run-1", []SupervisionInboxRow{
		{ID: "event-1", Record: json.RawMessage(`{"id":"event-1"}`)},
	}); err != nil {
		t.Fatal(err)
	}
	state, err := store.LoadSupervisionActivationRows(ctx, "run-1")
	if err != nil || !state.Supervised {
		t.Fatalf("load activation rows: %#v (err %v)", state, err)
	}
	if len(state.Inbox) != 1 || state.OtherValidActivation {
		t.Fatalf("activation state = %#v", state)
	}
	record := state.Record
	record.ActivationsUsed++
	liveLease := supervisionTestTime.Add(time.Hour)
	commit := SupervisionActivationRowCommit{
		RunID: "run-1", ExpectedRecordRevision: state.Record.Revision, Record: record,
		Activation: domain.Activation{
			ID: "activation-1", RunID: "run-1", Epoch: state.Record.ActivationEpoch,
			DispatchIdentity: "dispatch-1", State: domain.ActivationActive, Principal: "overseer-1",
			// Scope follows the lease, so the activation carries one. An
			// activation without a live lease resolves to no run at all, which
			// is asserted below.
			LeaseToken: "lease-1", LeaseExpiresAt: &liveLease,
		},
		ConsumedThrough: 1, CursorAdvanced: true,
		Outbox: []SupervisionOutboxRow{{
			ID: "wake-1", ActivationID: "activation-1", Delivery: "pending",
			Record: json.RawMessage(`{"id":"wake-1","kind":"wake"}`),
		}},
		CommittedAt: supervisionTestTime,
	}
	if err := store.CommitSupervisionActivationRows(ctx, commit); err != nil {
		t.Fatalf("commit the plan: %v", err)
	}
	// Replaying the same plan under the revision it read is refused: the record
	// moved, so the plan is about a state that no longer exists.
	err = store.CommitSupervisionActivationRows(ctx, commit)
	if !errors.Is(err, domain.ErrSupervisionStaleRevision) {
		t.Fatalf("stale commit error = %v, want a stale revision refusal", err)
	}
	after, err := store.LoadSupervisionActivationRows(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if after.Activation.ID != "activation-1" || after.Activation.Principal != "overseer-1" {
		t.Fatalf("committed activation = %#v", after.Activation)
	}
	if len(after.Inbox) != 1 || !after.Inbox[0].Consumed {
		t.Fatalf("inbox after the commit = %#v, want the high-water mark consumed", after.Inbox)
	}
	if len(after.Outbox) != 1 || after.Outbox[0].Delivery != "pending" {
		t.Fatalf("outbox after the commit = %#v", after.Outbox)
	}
	// The activation's principal is what binds a supervisor capability to one
	// run and one epoch, and it is read from the coordinator's own record.
	runID, epoch, err := store.SupervisorScopeForPrincipal(ctx, "overseer-1")
	if err != nil || runID != "run-1" || epoch != after.Activation.Epoch {
		t.Fatalf("supervisor scope = %q at epoch %d (err %v)", runID, epoch, err)
	}
	if runID, _, err := store.SupervisorScopeForPrincipal(ctx, "someone-else"); err != nil || runID != "" {
		t.Fatalf("an unrelated principal resolved to run %q (err %v)", runID, err)
	}
	// An expired lease revokes the capability immediately, the read half
	// included: the activation row is still active, and it still resolves to no
	// run at all.
	expired := after.Activation
	expiredAt := supervisionTestTime.Add(-time.Minute)
	expired.LeaseExpiresAt = &expiredAt
	stale := SupervisionActivationRowCommit{
		RunID: "run-1", ExpectedRecordRevision: after.Record.Revision, Record: after.Record,
		Activation: expired, CommittedAt: supervisionTestTime,
	}
	if err := store.CommitSupervisionActivationRows(ctx, stale); err != nil {
		t.Fatalf("commit the expired lease: %v", err)
	}
	if runID, epoch, err := store.SupervisorScopeForPrincipal(ctx, "overseer-1"); err != nil || runID != "" || epoch != 0 {
		t.Fatalf("an expired lease still resolved to run %q at epoch %d (err %v)", runID, epoch, err)
	}
}

func TestSupervisionOutboxDeduplicatesAndFencesDelivery(t *testing.T) {
	ctx := context.Background()
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	seedSupervisedRun(t, store, nil)
	entry := SupervisionOutboxRow{
		ID: "escalation-1", Delivery: "pending",
		Record: json.RawMessage(`{"id":"escalation-1","kind":"escalation","incidentId":"incident-1","threadId":"thread-1","reason":"needs a human"}`),
	}
	if written, err := store.AppendSupervisionOutboxRows(ctx, "run-1", []SupervisionOutboxRow{entry}); err != nil || written != 1 {
		t.Fatalf("first append wrote %d (err %v)", written, err)
	}
	// Re-escalating the same incident is the same intent, so it is not sent a
	// second time.
	if written, err := store.AppendSupervisionOutboxRows(ctx, "run-1", []SupervisionOutboxRow{entry}); err != nil || written != 0 {
		t.Fatalf("repeated append wrote %d (err %v), want none", written, err)
	}
	pending, err := store.PendingSupervisionEscalations(ctx)
	if err != nil || len(pending) != 1 || pending[0].ThreadID != "thread-1" ||
		pending[0].IncidentID != "incident-1" {
		t.Fatalf("pending escalations = %#v (err %v)", pending, err)
	}
	claimed, err := store.TransitionSupervisionOutboxRow(ctx, "escalation-1", "pending", "sending", supervisionTestTime)
	if err != nil || !claimed {
		t.Fatalf("claim = %v (err %v)", claimed, err)
	}
	// A second claimant loses: delivery ownership is a compare and set on the
	// state the claimant read, exactly as a node wake is.
	if again, err := store.TransitionSupervisionOutboxRow(ctx, "escalation-1", "pending", "sending", supervisionTestTime); err != nil || again {
		t.Fatalf("second claim = %v (err %v), want a loss", again, err)
	}
	if _, err := store.TransitionSupervisionOutboxRow(ctx, "escalation-1", "sending", "delivered", supervisionTestTime); err != nil {
		t.Fatal(err)
	}
	delivered, err := store.PendingSupervisionEscalations(ctx)
	if err != nil || len(delivered) != 0 {
		t.Fatalf("delivered escalation is still pending: %#v (err %v)", delivered, err)
	}
}

func TestSupervisionAdminStateBindsEvidenceAndBranchClosure(t *testing.T) {
	ctx := context.Background()
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	gate := domain.Gate{
		Definition: domain.GateDefinition{
			ID: "gate-1", Name: "review",
			ObservedTaskIDs: []string{"task-0"}, ProtectedTaskIDs: []string{"task-1"},
		},
		RunID: "run-1", State: domain.GatePendingEvidence, GraphRevision: 1,
	}
	seedSupervisedRun(t, store, []domain.Gate{gate})

	// A producer that has not succeeded binds no evidence, so the gate cannot
	// become reviewable and nothing can be accepted through it.
	state, err := store.LoadSupervisionAdminState(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Gates) != 1 || state.Gates[0].ProducersVerified || state.Gates[0].Evidence != nil {
		t.Fatalf("gate facts before any producer = %#v", state.Gates)
	}
	if advanced, err := store.AdvanceSupervisionGates(ctx, "run-1", supervisionTestTime); err != nil || len(advanced) != 0 {
		t.Fatalf("advanced %d gates with no succeeded producer (err %v)", len(advanced), err)
	}

	succeedSupervisionProducer(t, store)
	advanced, err := store.AdvanceSupervisionGates(ctx, "run-1", supervisionTestTime.Add(time.Minute))
	if err != nil || len(advanced) != 1 {
		t.Fatalf("advanced = %#v (err %v)", advanced, err)
	}
	if advanced[0].Gate.State != domain.GateReadyForReview || advanced[0].Evidence.ID == "" {
		t.Fatalf("advanced gate = %#v", advanced[0])
	}
	state, err = store.LoadSupervisionAdminState(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if state.Gates[0].Evidence == nil || state.Gates[0].Evidence.ID != advanced[0].Evidence.ID {
		t.Fatalf("admin state evidence = %#v, want the identity the advance bound", state.Gates[0].Evidence)
	}
	if state.Gates[0].SuccessorOfferedOrStarted {
		t.Fatal("a protected task with no assignment is reported as offered")
	}

	// A branch root resolves by task ID and by the declared task name, and its
	// closure covers the descendants a hold must not let escape.
	for _, root := range []string{"task-0", "producer"} {
		exists, revision, resolved, err := store.SupervisionBranchClosure(ctx, "run-1", root)
		if err != nil || !exists || revision != 1 {
			t.Fatalf("closure of %q: exists %v revision %d (err %v)", root, exists, revision, err)
		}
		if !reflect.DeepEqual(resolved, []string{"task-0", "task-1"}) {
			t.Fatalf("closure of %q = %v, want the root and its descendant", root, resolved)
		}
	}
	if exists, _, _, err := store.SupervisionBranchClosure(ctx, "run-1", "no-such-task"); err != nil || exists {
		t.Fatalf("an unknown branch root resolved (exists %v, err %v)", exists, err)
	}
}

func TestIncidentResolutionMatchesByGate(t *testing.T) {
	ctx := context.Background()
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	seedSupervisedRun(t, store, nil)
	if _, err := store.OpenReviewIncident(ctx, IncidentRequest{
		RunID: "run-1", IncidentID: "incident-1", RequestID: "open-1", Actor: supervisionOperator(),
		SourceEventID: "event-1", GateID: "gate-1",
		RequiredDisposition: domain.DispositionGateDecision,
		Reason:              "gate review is ready", OpenedAt: supervisionTestTime,
	}); err != nil {
		t.Fatalf("open the incident: %v", err)
	}
	// Another gate's acceptance closes nothing here. Matching by the mere
	// presence of a gate would let one review dismiss another's.
	_, err := store.ResolveReviewIncident(ctx, IncidentResolutionRequest{
		RunID: "run-1", IncidentID: "incident-1", RequestID: "close-wrong", Actor: supervisionOperator(),
		Event: domain.IncidentEventGateAccepted, GateID: "gate-2", ExpectedRevision: 1,
		Reason: "a different gate was accepted", ResolvedAt: supervisionTestTime,
	})
	if !errors.Is(err, domain.ErrSupervisionPrerequisite) {
		t.Fatalf("cross-gate closure error = %v, want an unmet prerequisite", err)
	}
	decision, err := store.ResolveReviewIncident(ctx, IncidentResolutionRequest{
		RunID: "run-1", IncidentID: "incident-1", RequestID: "close-right", Actor: supervisionOperator(),
		Event: domain.IncidentEventGateAccepted, GateID: "gate-1", ExpectedRevision: 1,
		Outcome: domain.IncidentOutcomeGateAccepted,
		Reason:  "its own gate was accepted", ResolvedAt: supervisionTestTime,
	})
	if err != nil {
		t.Fatalf("close the matching incident: %v", err)
	}
	if decision.Incident == nil || decision.Incident.State != domain.IncidentResolved {
		t.Fatalf("resolved incident = %#v", decision.Incident)
	}
}

// succeedSupervisionProducer marks the seeded run's producer task succeeded with
// one output artifact, which is what makes a gate's evidence verifiable.
func succeedSupervisionProducer(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()
	if err := store.SaveCoordinatorRecords(ctx, CoordinatorRecords{
		Attempts: []domain.Attempt{{
			ID: "attempt-0", WorkflowRunID: "run-1", TaskID: "task-0", Number: 1,
			Progress: domain.ProgressSucceeded, Control: domain.ControlStopped,
			Revision: 2, UpdatedAt: supervisionTestTime,
		}},
		Artifacts: []domain.Artifact{{
			ID: "artifact-produced", WorkflowRunID: "run-1", TaskID: "task-0", AttemptID: "attempt-0",
			Kind: domain.ArtifactOutput, Name: "result.md", SHA256: "digest-1",
			StoragePath: "result.md", Producer: "worker", CreatedAt: supervisionTestTime,
		}},
	}); err != nil {
		t.Fatal(err)
	}
}
