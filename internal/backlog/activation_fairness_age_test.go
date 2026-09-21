package backlog

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
	"time"
)

func TestFreshSameIncidentActivationUsesPostCursorAge(t *testing.T) {
	old := supervisionTestEvent("old", 1, TriggerGateReviewReady)
	old.IncidentID = "incident"
	old.OccurredAt = supervisionTestTime().Add(-time.Hour)
	fresh := supervisionTestEvent("fresh", 2, TriggerGateReviewReady)
	fresh.IncidentID = "incident"
	fresh.OccurredAt = supervisionTestTime()
	r := supervisionTestRecord()
	r.EventCursor = 1
	p, err := PlanActivation(SupervisionActivationState{Record: r, Pending: []SupervisionEvent{old, fresh}}, ActivationSignal{Event: domain.ActivationEventTriggerFired, IncidentID: "incident"}, supervisionTestTime())
	if err != nil {
		t.Fatal(err)
	}
	if !p.Activation.ReadyAt.Equal(fresh.OccurredAt) || p.Activation.ReadyTieID != "fresh" {
		t.Fatalf("age=%v/%s want fresh", p.Activation.ReadyAt, p.Activation.ReadyTieID)
	}
}
func TestUndeliveredRetryRetainsDurableAgeAcrossReload(t *testing.T) {
	ready := supervisionTestTime().Add(-time.Hour)
	a := domain.Activation{ID: ActivationID("run-1", 1), RunID: "run-1", Epoch: 1, DispatchIdentity: ActivationDispatchIdentity("run-1", 1), State: domain.ActivationPendingDispatch, ReadyAt: ready, ReadyTieID: "trigger"}
	p, err := PlanActivation(SupervisionActivationState{Record: supervisionTestRecord(), Activation: a}, ActivationSignal{Event: domain.ActivationEventDispatchUndelivered}, supervisionTestTime())
	if err != nil {
		t.Fatal(err)
	}
	if !p.Activation.ReadyAt.Equal(ready) || p.Activation.ReadyTieID != "trigger" || p.Dispatch == nil || !p.Dispatch.Retry {
		t.Fatalf("retry lost durable age: %#v", p)
	}
}
