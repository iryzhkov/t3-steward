package workerruntime

import (
	"context"
	"errors"
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// errAttemptFailed reports that observing an execution found a definite
// failure and the attempt has already been recorded as failed. The caller
// must not move the attempt to any other phase.
var errAttemptFailed = errors.New("attempt failed while observing its execution")

// observeThread is how every reconciliation path observes a dispatched
// execution. A contained run that systemd ended with a known cause, such as the
// memory limit killing it, can never be observed again; treating it as an
// unavailable observation kept the attempt in its phase for ever. It fails the
// attempt instead, through markFailed, which proves custody with a forced
// quiesce before the failure becomes durable and keeps the workspace for the
// failed result. Until custody is proven the attempt stays where it was and
// the next pass tries again.
func (r *Runtime) observeThread(ctx context.Context, id string, pkg workerproto.ExecutionPackage) (backlog.DispatchThreadState, error) {
	state, err := r.driver.ObserveThread(ctx, pkg)
	var ended *ContainedRunEndedError
	if !errors.As(err, &ended) {
		return state, err
	}
	if failErr := r.markFailed(ctx, id, ended.Failure); isJournalError(failErr) {
		return "", failErr
	} else if failErr != nil {
		return "", fmt.Errorf("%w; failing the attempt deferred: %v", err, failErr)
	}
	return "", fmt.Errorf("%w: %w", errAttemptFailed, err)
}
