package domain

// One answer to "may this actor decide this run's supervision right now".
//
// The question is asked in two places that cannot import each other: the store,
// inside the transaction that commits a decision, and the activation lifecycle,
// before it records one. Two implementations of an authority rule is how an
// expired lease keeps deciding in one of them, so the rule lives here and both
// call it.
//
// An epoch alone is not authority. A revoked activation, an expired lease and a
// review past its maximum elapsed time all leave the epoch exactly where it
// was, so a predicate that compares only epochs agrees with every one of them.

import (
	"fmt"
	"strings"
	"time"
)

// ActivationLeaseLive reports whether the coordinator-issued lease is still
// live. Expiry revokes decision authority immediately: it is not a grace period
// and not a warning.
func ActivationLeaseLive(activation Activation, now time.Time) bool {
	if strings.TrimSpace(activation.LeaseToken) == "" || activation.LeaseExpiresAt == nil {
		return false
	}
	return now.UTC().Before(activation.LeaseExpiresAt.UTC())
}

// ActivationPastDeadline reports whether the activation exceeded its maximum
// elapsed time. A renewed lease extends how long the coordinator believes in
// the overseer, never how long the overseer may take.
func ActivationPastDeadline(activation Activation, now time.Time) bool {
	if activation.Deadline == nil {
		return false
	}
	return !now.UTC().Before(activation.Deadline.UTC())
}

// AuthorizeSupervisionActor reports whether one actor may act on one run's
// supervision at this moment.
//
// An operator is authenticated by the admin transport and is not bound to an
// activation; it is still fenced by whatever expected revision the operation
// itself names, which is the caller's check rather than this one.
//
// An overseer must hold the run's current activation: the epoch it names must be
// the record's and the activation's, that activation must be active, its lease
// must be live and it must not have passed its deadline.
func AuthorizeSupervisionActor(
	record SupervisionRecord,
	activation Activation,
	actor Actor,
	now time.Time,
) error {
	if err := actor.Validate(); err != nil {
		return err
	}
	if actor.Kind == ActorOperator {
		return nil
	}
	if actor.Kind != ActorOverseer {
		return fmt.Errorf("%w: actor kind %q may not act on supervision", ErrSupervisionUnauthorizedActor, actor.Kind)
	}
	if actor.ActivationEpoch != record.ActivationEpoch {
		return fmt.Errorf("%w: actor names activation epoch %d, run %q is at %d",
			ErrSupervisionStaleRevision, actor.ActivationEpoch, record.RunID, record.ActivationEpoch)
	}
	if activation.RunID != record.RunID || activation.Epoch != record.ActivationEpoch {
		return fmt.Errorf("%w: run %q has no activation at epoch %d",
			ErrSupervisionUnauthorizedActor, record.RunID, record.ActivationEpoch)
	}
	if activation.State != ActivationActive {
		return fmt.Errorf("%w: the activation is %s, not active", ErrSupervisionPrerequisite, activation.State)
	}
	if activation.Purpose == RecoveryActivationRepair {
		return fmt.Errorf("%w: a repair activation has no reviewer or hold authority", ErrSupervisionUnauthorizedActor)
	}
	if !ActivationLeaseLive(activation, now) {
		return fmt.Errorf("%w: the activation lease expired, so its decision authority is revoked",
			ErrSupervisionPrerequisite)
	}
	if ActivationPastDeadline(activation, now) {
		return fmt.Errorf("%w: the activation passed its maximum elapsed time", ErrSupervisionPrerequisite)
	}
	return nil
}
