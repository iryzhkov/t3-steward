package domain

import (
	"errors"
	"testing"
	"time"
)

func TestRepairActivationHasNoReviewerAuthority(t *testing.T) {
	now := time.Date(2026, 9, 21, 15, 0, 0, 0, time.UTC)
	expires := now.Add(time.Hour)
	deadline := now.Add(2 * time.Hour)
	record := SupervisionRecord{RunID: "run", ActivationEpoch: 2}
	activation := Activation{ID: "repair", RunID: "run", Epoch: 2, Purpose: RecoveryActivationRepair,
		State: ActivationActive, LeaseToken: "lease", LeaseExpiresAt: &expires, Deadline: &deadline}
	actor := Actor{Kind: ActorOverseer, Principal: "repairer", ActivationEpoch: 2}
	if err := AuthorizeSupervisionActor(record, activation, actor, now); !errors.Is(err, ErrSupervisionUnauthorizedActor) {
		t.Fatalf("repair activation reviewer authority error=%v", err)
	}
	activation.Purpose = ""
	if err := AuthorizeSupervisionActor(record, activation, actor, now); err != nil {
		t.Fatalf("legacy reviewer activation rejected: %v", err)
	}
}
