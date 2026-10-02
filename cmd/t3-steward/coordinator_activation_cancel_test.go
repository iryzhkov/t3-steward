package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// A whole-run cancel closes the run's supervision but may leave the run
// waiting a few boundaries for a cancelled worker to stop before its sink
// settles. The review event that was pending when the operator cancelled must
// not wake an overseer in that window: the activation would reopen what the
// cancel resolved.
func TestNoOverseerIsWokenForARunCancelledWhole(t *testing.T) {
	ctx := context.Background()
	fixture := newActivationLeaseFixture(t)
	fixture.superviseRun(t)
	if err := fixture.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{
		AdminCommands: []domain.AdminCommand{{
			ID: "cancel-whole-run", Kind: domain.AdminCommandCancel,
			TargetType: domain.AdminTargetAttempt, TargetID: "attempt-protected", ExpectedRevision: 1,
			Reason: "obsolete", RequestedBy: "operator", Payload: json.RawMessage(`{"scope":"run"}`),
			State: domain.AdminCommandApplied, CreatedAt: fixture.now, AppliedAt: &fixture.now,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	fixture.coordinator.DispatchActivations(ctx, admittingQuota())
	state, err := fixture.supervision.LoadSupervisionActivationState(ctx, activationLeaseRun)
	if err != nil {
		t.Fatal(err)
	}
	if state.Activation.State == domain.ActivationPendingDispatch || state.Activation.State == domain.ActivationActive {
		t.Fatalf("an overseer was woken for a run cancelled whole: %+v", state.Activation)
	}
}
