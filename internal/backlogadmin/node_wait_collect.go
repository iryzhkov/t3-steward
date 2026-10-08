package backlogadmin

import (
	"context"
	"errors"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// RunCollectAction records that a thread has acted on one finished run, and
// RunCollectionListAction lists a thread's records. They ride the node-wait
// operation because ownership is a node wait of the thread on the run: the
// same privilege, over the same records, and an envelope every coordinator
// since rc.70 already decodes. Both carry only request.threadId and
// request.target.runId.
const (
	RunCollectAction        = "collect-run"
	RunCollectionListAction = "list-collections"
)

// olderCoordinatorNodeWaitAnswer is the whole answer a coordinator gives to a
// node-wait action it does not know, which is what a coordinator older than
// run collection answers to both actions above.
const olderCoordinatorNodeWaitAnswer = "unknown native wait action"

type runCollectionStore interface {
	CollectRun(ctx context.Context, threadID, runID, actor string, now time.Time) (domain.RunCollection, bool, error)
	ListRunCollections(ctx context.Context, threadID string) ([]domain.RunCollection, error)
}

// runCollection answers the two run-collection actions. The coordinator, not
// the client, reads the run's progress and completion time, and the store
// checks ownership in the same transaction that writes the record.
func (s *Service) runCollection(ctx context.Context, principal Principal, op NodeWaitOperation) (NodeWaitResponse, error) {
	var result NodeWaitResponse
	store, ok := s.reader.(runCollectionStore)
	if !ok {
		return result, errors.New("run collection unavailable")
	}
	thread := op.Request.ThreadID
	if err := domain.ValidateRunCollectionID("thread", thread); err != nil {
		return result, err
	}
	if op.Action == RunCollectionListAction {
		collections, err := store.ListRunCollections(ctx, thread)
		if err != nil {
			return result, err
		}
		result.Collections = collections
		return result, nil
	}
	run := op.Request.Target.RunID
	if err := domain.ValidateRunCollectionID("run", run); err != nil {
		return result, err
	}
	collection, changed, err := store.CollectRun(ctx, thread, run, principal.ID, s.now())
	if err != nil {
		return result, err
	}
	result.Collections = []domain.RunCollection{collection}
	result.Changed = changed
	return result, nil
}

// RunCollectionUnsupported reports whether err is a coordinator's answer that
// it does not know the run-collection actions: exactly the answer to an
// unknown node-wait action, either as the whole refusal or as the cause the
// SSH carrier wraps with the session's stderr. Any other error, including one
// that merely contains the same words, is not.
func RunCollectionUnsupported(err error) bool {
	if err == nil {
		return false
	}
	answer := err
	var transport *TransportError
	if errors.As(err, &transport) && transport.Err != nil {
		answer = transport.Err
	}
	if answer.Error() == olderCoordinatorNodeWaitAnswer {
		return true
	}
	cause := errors.Unwrap(answer)
	return cause != nil && cause.Error() == olderCoordinatorNodeWaitAnswer
}
