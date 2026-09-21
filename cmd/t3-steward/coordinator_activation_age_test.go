package main

import (
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestActivationDispatchAgeFreshTriggerIgnoresPriorEpochAge(t *testing.T) {
	old := time.Date(2026, 9, 20, 8, 0, 0, 0, time.UTC)
	fresh := old.Add(time.Hour)
	state := backlog.SupervisionActivationState{
		Record: domain.SupervisionRecord{RunID: "run", EventCursor: 4},
		Activation: domain.Activation{
			DispatchIdentity: "dispatch-old", ReadyAt: old, ReadyTieID: "event-old",
			State: domain.ActivationIdle,
		},
		Pending: []backlog.SupervisionEvent{{
			ID: "event-fresh", RunID: "run", Sequence: 5, OccurredAt: fresh,
			Kind: backlog.TriggerOperatorReassessment,
		}},
	}
	at, id, err := activationDispatchAge(state, backlog.ActivationSignal{Event: domain.ActivationEventTriggerFired})
	if err != nil {
		t.Fatal(err)
	}
	if !at.Equal(fresh) || id != "event-fresh" {
		t.Fatalf("fresh trigger ordering=(%s,%q), want (%s,%q)", at, id, fresh, "event-fresh")
	}
}

func TestActivationDispatchAgeLegacyRetryUsesStableDeadlineEvidence(t *testing.T) {
	planned := time.Date(2026, 9, 20, 8, 0, 0, 0, time.UTC)
	deadline := planned.Add(2 * time.Hour)
	state := backlog.SupervisionActivationState{
		Record: domain.SupervisionRecord{
			RunID: "run", EventCursor: 4,
			Config: domain.SupervisionConfig{ActivationDeadline: 2 * time.Hour},
		},
		Activation: domain.Activation{
			DispatchIdentity: "dispatch-legacy", State: domain.ActivationPendingDispatch,
			Deadline: &deadline,
		},
	}
	at, id, err := activationDispatchAge(state, backlog.ActivationSignal{Event: domain.ActivationEventDispatchUndelivered})
	if err != nil {
		t.Fatal(err)
	}
	if !at.Equal(planned) || id != "dispatch-legacy" {
		t.Fatalf("legacy retry ordering=(%s,%q), want (%s,%q)", at, id, planned, "dispatch-legacy")
	}
}
