package wait

import (
	"context"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// TaskWaitStore is the coordinator's task-bound wait surface. A store that does
// not implement it holds no task-bound waits, and the runner behaves exactly as
// it did for interactive waits alone.
type TaskWaitStore interface {
	SettleTaskWait(context.Context, string, domain.TaskWaitResult, time.Time) (domain.TaskWait, error)
	ExpireTaskWaits(context.Context, time.Time) ([]domain.TaskWait, error)
	WakeTaskWaits(context.Context, time.Time) ([]domain.TaskWaitWakeContext, error)
	PendingTaskWakes(context.Context) ([]domain.TaskWaitWakeContext, error)
	MarkTaskWaitDelivered(context.Context, string, time.Time) error
}

// tickTaskWaits carries settled checks into the coordinator, enforces every
// wait's maximum duration, and delivers the wakes the coordinator committed.
//
// The order matters. A check result becomes an immutable coordinator outcome
// before anything acts on it; expiry then settles whatever the checks did not,
// so a held directory binding cannot block the fleet forever; and only then are
// resumed attempts told what happened. Resumption is committed durably before
// the message is sent, so a lost response retries the message and never the
// resumption: one wake, one resumed turn, one verification.
func (r *Runner) tickTaskWaits(ctx context.Context, waits []Wait) {
	store, ok := r.store.(TaskWaitStore)
	if !ok {
		return
	}
	now := r.now()
	for _, w := range waits {
		if w.TaskWaitID == "" || !w.Settled() {
			continue
		}
		if _, err := store.SettleTaskWait(ctx, w.TaskWaitID, taskWaitResult(w, now), now); err != nil {
			r.log.Error("settle task-bound wait", "wait", w.TaskWaitID, "err", err)
		}
	}
	if expired, err := store.ExpireTaskWaits(ctx, now); err != nil {
		r.log.Error("expire task-bound waits", "err", err)
	} else if len(expired) != 0 {
		r.log.Warn("task-bound waits exceeded their maximum duration", "waits", len(expired))
	}
	if _, err := store.WakeTaskWaits(ctx, now); err != nil {
		r.log.Error("resume parked attempts", "err", err)
		return
	}
	pending, err := store.PendingTaskWakes(ctx)
	if err != nil {
		r.log.Error("list pending task wakes", "err", err)
		return
	}
	for _, wake := range pending {
		r.deliverTaskWake(ctx, store, wake, now)
	}
}

func (r *Runner) deliverTaskWake(ctx context.Context, store TaskWaitStore, wake domain.TaskWaitWakeContext, now time.Time) {
	log := r.log.With("thread", wake.ThreadID, "attempt", wake.AttemptID)
	thread, err := r.control.GetThread(ctx, wake.ThreadID)
	if err != nil {
		log.Warn("cannot read thread for a task wake", "err", err)
		return
	}
	if thread == nil || thread.ArchivedAt != nil {
		// The attempt is already resumed and its thread is gone. Marking the
		// wake delivered stops an unreachable message being retried forever;
		// the attempt's own turn will end with no outputs and fail honestly.
		log.Warn("task wake thread is gone; the resumed attempt will fail on its own evidence")
		r.markTaskWakeDelivered(ctx, store, wake, now)
		return
	}
	if ok, why := r.healthy(*thread); !ok {
		log.Debug("task wake held", "reason", why)
		return
	}
	if r.DryRun {
		log.Info("dry-run: would resume a parked task", "waits", len(wake.Waits))
		return
	}
	if err := r.control.ResumeThread(ctx, *thread, wake.Prompt()); err != nil {
		log.Error("resume parked task", "err", err)
		return
	}
	log.Info("parked task resumed", "waits", len(wake.Waits))
	r.markTaskWakeDelivered(ctx, store, wake, now)
}

func (r *Runner) markTaskWakeDelivered(ctx context.Context, store TaskWaitStore, wake domain.TaskWaitWakeContext, now time.Time) {
	for _, w := range wake.Waits {
		if err := store.MarkTaskWaitDelivered(ctx, w.ID, now); err != nil {
			r.log.Error("record task wake delivery", "wait", w.ID, "err", err)
		}
	}
}

// taskWaitResult translates a settled check into the structured evidence the
// woken task is given. Silence is not an outcome: which wait, which condition,
// which exit status and how long it ran all travel with it.
func taskWaitResult(w Wait, now time.Time) domain.TaskWaitResult {
	outcome := domain.TaskWaitFailed
	switch w.Status {
	case StatusMet:
		outcome = domain.TaskWaitMet
	case StatusTimedOut:
		outcome = domain.TaskWaitTimedOut
	case StatusCancelled:
		outcome = domain.TaskWaitCancelled
	}
	observed := now
	if w.SettledAt != nil {
		observed = *w.SettledAt
	}
	return domain.TaskWaitResult{
		Outcome: outcome, ExitCode: w.LastExit, Reason: w.Reason,
		Output: w.LastOutput, RanFor: observed.Sub(w.CreatedAt), ObservedAt: observed,
	}
}
