package backlog

import (
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestActivationPurposeSelectsOnlyOwnedInterleavedEvents(t *testing.T) {
	review := supervisionTestEvent("review", 2, TriggerGateReviewReady)
	repair := supervisionTestEvent("repair", 1, TriggerTaskJudgmentRequired)
	state := SupervisionActivationState{
		Record:  supervisionTestRecord(),
		Pending: []SupervisionEvent{repair, review},
	}
	reviewer, err := PlanActivation(state, ActivationSignal{
		Event: domain.ActivationEventTriggerFired,
	}, supervisionTestTime())
	if err != nil {
		t.Fatal(err)
	}
	if got := reviewer.Inbox.EventIDs(); len(got) != 1 || got[0] != "review" {
		t.Fatalf("reviewer inbox = %v, want only review", got)
	}
	if reviewer.Activation.Purpose != "" || reviewer.Activation.ConsumedEventCursor != review.Sequence {
		t.Fatalf("reviewer activation = %#v", reviewer.Activation)
	}

	repairPlan, err := PlanActivation(state, ActivationSignal{
		Event: domain.ActivationEventTriggerFired, Purpose: domain.RecoveryActivationRepair,
	}, supervisionTestTime())
	if err != nil {
		t.Fatal(err)
	}
	if got := repairPlan.Inbox.EventIDs(); len(got) != 1 || got[0] != "repair" {
		t.Fatalf("repair inbox = %v, want only repair", got)
	}
	if repairPlan.Activation.Purpose != domain.RecoveryActivationRepair ||
		repairPlan.Activation.ConsumedEventCursor != repair.Sequence {
		t.Fatalf("repair activation = %#v", repairPlan.Activation)
	}

	repair.AcknowledgedPurposes = []domain.RecoveryActivationPurpose{domain.RecoveryActivationRepair}
	state.Pending = []SupervisionEvent{repair, review}
	if selected := supervisionEventsForPurpose(state.Pending, domain.RecoveryActivationRepair); len(selected) != 0 {
		t.Fatalf("acknowledged repair replayed: %#v", selected)
	}
	if selected := supervisionEventsForPurpose(state.Pending, ""); len(selected) != 1 || selected[0].ID != "review" {
		t.Fatalf("repair acknowledgement affected reviewer inbox: %#v", selected)
	}
}
