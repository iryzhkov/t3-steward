package backlog

import (
	"errors"
	"fmt"
)

// ErrResultImportSuperseded reports a worker upload for an attempt that already
// reached a different terminal outcome (for example it was cancelled while the
// worker was still collecting). The upload carries no new information and can
// be acknowledged without import.
var ErrResultImportSuperseded = errors.New("result import superseded")

// ErrTurnOutcomeWaiting reports a finished-turn marker, or a whole worker
// result, that arrived while the attempt still held a live task-bound wait.
//
// The turn ended because the task parked, not because it finished. Its declared
// outputs are not written yet, so importing the result would verify the task
// against files it has not produced. The upload carries no usable information
// and is discarded rather than retried: after the wake, the resumed turn
// produces its own result.
//
// It wraps ErrResultImportSuperseded so the worker client acknowledges and
// drops the upload instead of holding every other result that worker owns.
var ErrTurnOutcomeWaiting = fmt.Errorf("%w: refused while a task-bound wait is live", ErrResultImportSuperseded)
