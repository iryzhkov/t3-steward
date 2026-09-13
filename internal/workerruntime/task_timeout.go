package workerruntime

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"time"
)

func taskTimeoutExpired(record AttemptRecord, now time.Time) bool {
	pkg := record.Package.Package
	switch record.Phase {
	case PhaseCompleted, PhaseFailed, PhaseCollecting:
		return false
	}
	return pkg.Timeout > 0 && !now.Before(pkg.CreatedAt.Add(pkg.Timeout))
}

// An elapsed budget is not proof that execution stopped. Keep the attempt in
// stopping until T3 observation proves containment, then publish a failed result.
func (r *Runtime) expireTask(ctx context.Context, id string, record AttemptRecord) error {
	switch record.Phase {
	case PhaseClaimed, PhasePreparing, PhasePrepared:
		return r.markFailed(id, "task timeout expired before dispatch")
	}
	pkg := record.Package.Package
	if record.Phase != PhaseStopping {
		if err := r.markPhase(id, PhaseStopping, "task timeout expired", record.WorkspacePath, record.ThreadID); err != nil {
			return err
		}
	}
	if err := r.driver.StopThread(ctx, pkg); err != nil {
		r.log.Warn("task timeout stop remains unproven", "assignment", id, "error", err)
		return nil
	}
	observed, err := r.driver.ObserveThread(ctx, pkg)
	if err != nil || (observed != backlog.DispatchThreadStopped && observed != backlog.DispatchThreadMissing) {
		return nil
	}
	return r.markFailed(id, "task timeout expired; execution stopped")
}
