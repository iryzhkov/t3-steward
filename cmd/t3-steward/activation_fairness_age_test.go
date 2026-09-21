package main

import (
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
	"time"
)

func TestActivationDispatchAgeUsesPersistedEpochAcrossRestart(t *testing.T) {
	ready := time.Date(2026, 9, 20, 8, 0, 0, 0, time.UTC)
	state := backlog.SupervisionActivationState{Record: domain.SupervisionRecord{RunID: "run", EventCursor: 2}, Activation: domain.Activation{DispatchIdentity: "dispatch", ReadyAt: ready, ReadyTieID: "selected"}, Pending: []backlog.SupervisionEvent{{ID: "consumed", RunID: "run", Sequence: 1, OccurredAt: ready.Add(-time.Hour)}}}
	got, id, err := activationDispatchAge(state, backlog.ActivationSignal{Event: domain.ActivationEventDispatchUndelivered})
	if err != nil || !got.Equal(ready) || id != "selected" {
		t.Fatalf("age=%v id=%q err=%v", got, id, err)
	}
}
