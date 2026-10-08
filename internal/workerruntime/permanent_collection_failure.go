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
const permanentSecretFailurePrefix = "permanent collection secret failure: "

type permanentCollectionFailure struct {
	size   *workerproto.ArtifactSizeError
	secret *SecretScanError
}

func (e *permanentCollectionFailure) Error() string { return e.Unwrap().Error() }
func (e *permanentCollectionFailure) Unwrap() error {
	if e.secret != nil {
		return e.secret
	}
	return e.size
}

func permanentCollectionIntent(failure string) bool {
	return strings.HasPrefix(failure, permanentCollectionFailurePrefix) || strings.HasPrefix(failure, permanentSecretFailurePrefix)
}

// failCollection keeps the finished flight registered until the same-current
// failed intent is durable. Quiescence must be proven before the transition.
// Journal or quiescence failures retain the flight; they never rerun finalization.
func (r *Runtime) failCollection(id string, record AttemptRecord, flight *collectionFlight, failure *permanentCollectionFailure) error {
	prefix := permanentCollectionFailurePrefix
	if failure.secret != nil {
		prefix = permanentSecretFailurePrefix
	}
	claimed, err := r.failFinishedCollection(id, record, flight, prefix+failure.Error()+
		"; raw outputs, thread archive and capture retained in worker workspace/custody for this assignment; recover using the journal workspace path")
	if err != nil || !claimed {
		return err
	}
	return fmt.Errorf("collection failed permanently; bounded failure custody pending: %w", failure)
}

// failFinishedCollection records failure as the outcome of a finished
// collection that cannot succeed, once quiescence is proven. It reports false
// when the collection no longer decides the attempt.
func (r *Runtime) failFinishedCollection(id string, record AttemptRecord, flight *collectionFlight, failure string) (bool, error) {
	claimed, err := r.collectionClaimed(id, record)
	if err != nil {
		return false, err
	}
	if !claimed {
		releaseCollection(r.collectionFlightKey(record), flight)
		return false, nil
	}
	ctx, cancel := context.WithTimeout(r.config.Lifetime, r.finalizationTimeout(record))
	defer cancel()
	if err := r.stopPreparation(ctx, record.Package.Package); err != nil {
		return false, err
	}
	err = r.journal.update(func(state *journalState) error {
		current, ok := state.Attempts[id]
		if !ok || !sameCollection(current, record) || hasCommandRequest(current, domain.WorkerCommandStop) {
			return nil
		}
		current.Phase = PhaseFailed
		current.Failure = failure
		current.UpdatedAt = r.now()
		state.Attempts[id] = current
		state.Sequence++
		return nil
	})
	if err != nil {
		return false, err
	}
	releaseCollection(r.collectionFlightKey(record), flight)
	return true, nil
}
