package workerruntime

import (
	"context"
	"fmt"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// This prefix is an internal durable intent in the existing journal Failure
// field, not an error classifier. Only the typed publishing rejection writes it.
const permanentCollectionFailurePrefix = "permanent collection size failure: "

type permanentCollectionFailure struct {
	size *workerproto.ArtifactSizeError
}

func (e *permanentCollectionFailure) Error() string { return e.size.Error() }
func (e *permanentCollectionFailure) Unwrap() error { return e.size }

func permanentCollectionIntent(failure string) bool {
	return strings.HasPrefix(failure, permanentCollectionFailurePrefix)
}

// failCollection keeps the finished flight registered until the same-current
// failed intent is durable. Quiescence must be proven before the transition.
// Journal or quiescence failures retain the flight; they never rerun finalization.
func (r *Runtime) failCollection(id string, record AttemptRecord, flight *collectionFlight, failure *permanentCollectionFailure) error {
	claimed, err := r.collectionClaimed(id, record)
	if err != nil {
		return err
	}
	if !claimed {
		releaseCollection(r.collectionFlightKey(record), flight)
		return nil
	}
	ctx, cancel := context.WithTimeout(r.config.Lifetime, r.finalizationTimeout(record))
	defer cancel()
	if err := r.stopPreparation(ctx, record.Package.Package); err != nil {
		return err
	}
	err = r.journal.update(func(state *journalState) error {
		current, ok := state.Attempts[id]
		if !ok || !sameCollection(current, record) || hasCommandRequest(current, domain.WorkerCommandStop) {
			return nil
		}
		current.Phase = PhaseFailed
		current.Failure = permanentCollectionFailurePrefix + failure.Error() +
			"; raw outputs, thread archive and capture retained in worker workspace/custody for this assignment; recover using the journal workspace path"
		current.UpdatedAt = r.now()
		state.Attempts[id] = current
		state.Sequence++
		return nil
	})
	if err != nil {
		return err
	}
	releaseCollection(r.collectionFlightKey(record), flight)
	return fmt.Errorf("collection failed permanently; bounded failure custody pending: %w", failure)
}
