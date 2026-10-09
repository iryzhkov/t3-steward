package workerruntime

import (
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// CollectionDeferral is the durable account of an attempt whose turn ended
// and whose collection keeps deferring: since when, and why it last did.
// The worker reports it in the journal excerpt's turn-end note, so that
// triage can list an attempt that has been deferring for long instead of it
// reading as active running.
type CollectionDeferral struct {
	Since  time.Time `json:"since"`
	Reason string    `json:"reason"`
}

// recordCollectionDeferral records reason, already redacted, as why the
// collection of attempt id last deferred. Since is the first deferral of this
// collection and is kept. Only an attempt the journal still names collecting
// for the same assignment epoch is touched, and nothing is written when the
// reason is unchanged, so a pass that defers again costs no journal write.
func (r *Runtime) recordCollectionDeferral(id string, record AttemptRecord, reason string) error {
	reason = truncateText(reason, maxTurnEndNote)
	return r.journal.update(func(state *journalState) error {
		current, ok := state.Attempts[id]
		// A quota pause defers collection on purpose, and triage lists the
		// held pool instead.
		if !ok || current.Phase != PhaseCollecting || current.LocalThrottle != nil ||
			current.Assignment.ID != record.Assignment.ID || current.Assignment.Epoch != record.Assignment.Epoch {
			return nil
		}
		if current.CollectionDeferred != nil && current.CollectionDeferred.Reason == reason {
			return nil
		}
		deferral := CollectionDeferral{Since: r.now().UTC(), Reason: reason}
		if current.CollectionDeferred != nil {
			deferral.Since = current.CollectionDeferred.Since
		}
		current.CollectionDeferred = &deferral
		state.Attempts[id] = current
		state.Sequence++
		return nil
	})
}

// collectionDeferredNote is the turn-end note for record's deferred
// collection, or empty when it is not collecting or has not deferred.
func collectionDeferredNote(record AttemptRecord) string {
	if record.Phase != PhaseCollecting || record.CollectionDeferred == nil {
		return ""
	}
	return truncateText(domain.CollectionDeferredNote(record.CollectionDeferred.Since, record.CollectionDeferred.Reason), maxTurnEndNote)
}
