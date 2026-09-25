package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A continuation writes the replacement activation as a new row at the raised
// epoch and leaves the old row as it was, so the old row can still say
// pending-dispatch at its own epoch. That row has no authority left, the same
// judgement ReleaseDeadActivationOffers makes of its offer (S13), and counting
// it as another valid activation refused every trigger of the replacement, so
// the run could never wake its overseer again. An activation at the current
// epoch still fences a second one.
func TestSupersededEpochActivationIsNotAnotherValidActivation(t *testing.T) {
	ctx := context.Background()
	store := openSupervisionStore(t, filepath.Join(t.TempDir(), "state.db"))
	seedSupervisedRun(t, store, nil)
	seedSupervisionInboxEvent(t, store, "event-1", 1)

	if _, err := store.RecordActivationTransition(ctx, ActivationRequest{
		RunID: "run-1", ActivationID: "activation-1", RequestID: "request-trigger-1",
		Actor: domain.Actor{Kind: domain.ActorOverseer, Principal: "overseer:run-1", ActivationEpoch: 1},
		Event: domain.ActivationEventTriggerFired, ExpectedEpoch: 1,
		ExpectedSupervisionRevision: 1, DispatchIdentity: "supervision:run-1:1",
		TransitionedAt: supervisionTestTime,
	}); err != nil {
		t.Fatalf("trigger the first activation: %v", err)
	}
	state, err := store.LoadSupervisionActivationRows(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.State != domain.ActivationPendingDispatch {
		t.Fatalf("first activation = %#v, want pending dispatch", state.Activation)
	}
	// The continuation's shape: the record moves to epoch 2 with a new, idle
	// activation, and the epoch-1 row is not rewritten.
	record := state.Record
	record.ActivationEpoch = 2
	if err := store.CommitSupervisionActivationRows(ctx, SupervisionActivationRowCommit{
		RunID: "run-1", ExpectedRecordRevision: state.Record.Revision, Record: record,
		Activation:  domain.Activation{ID: "activation-2", RunID: "run-1", Epoch: 2, State: domain.ActivationIdle},
		RequestID:   "continuation",
		CommittedAt: supervisionTestTime,
	}); err != nil {
		t.Fatalf("commit the continuation: %v", err)
	}

	rows, err := store.LoadSupervisionActivationRows(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if rows.Activation.ID != "activation-2" {
		t.Fatalf("current activation = %#v, want the epoch-2 replacement", rows.Activation)
	}
	if rows.OtherValidActivation {
		t.Fatal("the superseded epoch-1 pending dispatch was counted as another valid activation")
	}
	triggered, err := store.RecordActivationTransition(ctx, ActivationRequest{
		RunID: "run-1", ActivationID: "activation-2", RequestID: "request-trigger-2",
		Actor: domain.Actor{Kind: domain.ActorOverseer, Principal: "overseer:run-1", ActivationEpoch: 2},
		Event: domain.ActivationEventTriggerFired, ExpectedEpoch: 2,
		ExpectedSupervisionRevision: rows.Record.Revision, DispatchIdentity: "supervision:run-1:2",
		TransitionedAt: supervisionTestTime,
	})
	if err != nil {
		t.Fatalf("the replacement could not be triggered: %v", err)
	}
	if triggered.Activation.State != domain.ActivationPendingDispatch || triggered.Activation.Epoch != 2 {
		t.Fatalf("replacement = %#v, want pending dispatch at epoch 2", triggered.Activation)
	}

	// The replacement is now valid at the current epoch, so a third activation
	// is still refused.
	_, err = store.RecordActivationTransition(ctx, ActivationRequest{
		RunID: "run-1", ActivationID: "activation-3", RequestID: "request-trigger-3",
		Actor: domain.Actor{Kind: domain.ActorOverseer, Principal: "overseer:run-1", ActivationEpoch: 2},
		Event: domain.ActivationEventTriggerFired, ExpectedEpoch: 2,
		ExpectedSupervisionRevision: triggered.SupervisionRevision, DispatchIdentity: "supervision:run-1:2b",
		TransitionedAt: supervisionTestTime,
	})
	if !errors.Is(err, domain.ErrSupervisionPrerequisite) {
		t.Fatalf("a second activation at the current epoch: err = %v, want a prerequisite refusal", err)
	}
}
