package wait

import (
	"context"
	"errors"
	"log/slog"
)

// logFailure reports one failed step of a tick. A step that failed because the
// runner's context was cancelled is the daemon shutting down, not an
// operational fault: every store call in flight returns "context canceled" at
// once, and logging that burst at ERROR buried the errors an operator has to
// read. Those are reported at INFO as shutdown; every other failure keeps its
// severity. The coordinator's boundary cycle applies the same rule.
func logFailure(ctx context.Context, log *slog.Logger, msg string, err error, attrs ...any) {
	// The runner's own context is the authoritative signal. The error is also
	// checked because a store call that observed the cancellation returns it
	// wrapped, sometimes before ctx.Err() is visible to this goroutine.
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		log.Info("shutting down: "+msg, attrs...)
		return
	}
	log.Error(msg, attrs...)
}
