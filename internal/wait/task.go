package wait

import (
	"context"
	"fmt"
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
	TaskWakesAwaitingDelivery(context.Context, time.Time) ([]domain.TaskWaitWakeContext, error)
	TransitionTaskWake(context.Context, string, string, string, time.Time) (bool, error)
}

// TaskWaitLister is the optional list surface of the coordinator. A store
// that offers it lets the worker notice a bound wait that was settled
// without its check (a cancellation, the coordinator's own settlement pass)
// and stop polling for it.
type TaskWaitLister interface {
	ListTaskWaits(context.Context) ([]domain.TaskWait, error)
}

// taskWaitReconcileEvery bounds how often a worker asks the coordinator
// about its live bound rows.
const taskWaitReconcileEvery = time.Minute

// reconcileBoundChecks cancels the local rows whose coordinator wait has
// settled without them. It asks the coordinator at most once a minute, and
// only while a bound row is still waiting.
func (r *Runner) reconcileBoundChecks(ctx context.Context, store TaskWaitStore, waits []Wait, now time.Time) {
	lister, ok := store.(TaskWaitLister)
	if !ok {
		return
	}
	bound := map[string]*Wait{}
	for i := range waits {
		if waits[i].TaskWaitID != "" && waits[i].Status == StatusWaiting {
			bound[waits[i].TaskWaitID] = &waits[i]
		}
	}
	if len(bound) == 0 || (!r.lastBoundReconcile.IsZero() && now.Sub(r.lastBoundReconcile) < taskWaitReconcileEvery) {
		return
	}
	r.lastBoundReconcile = now
	records, err := lister.ListTaskWaits(ctx)
	if err != nil {
		logFailure(ctx, r.log, "list task-bound waits for reconcile", err, "err", err)
		return
	}
	for _, record := range records {
		row, ok := bound[record.ID]
		if !ok || !record.Settled() {
			continue
		}
		outcome := "settled"
		if record.Result != nil {
			outcome = string(record.Result.Outcome)
		}
		row.Status = StatusCancelled
		row.Reason = fmt.Sprintf("the coordinator settled wait %s as %s without this check; the check is cancelled", record.ID, outcome)
		t := now
		row.SettledAt = &t
		r.log.Info("bound check cancelled after coordinator settlement", "wait", row.ID, "task_wait", record.ID, "outcome", outcome)
		r.save(ctx, *row)
	}
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
	if r.DisableTaskWaitRuntime {
		return
	}
	store := r.TaskStore
	if store == nil {
		store, _ = r.store.(TaskWaitStore)
	}
	if store == nil {
		return
	}
	now := r.now()
	for _, w := range waits {
		if w.TaskWaitID == "" || !w.Settled() {
			continue
		}
		if _, err := store.SettleTaskWait(ctx, w.TaskWaitID, taskWaitResult(w, now), now); err != nil {
			logFailure(ctx, r.log, "settle task-bound wait", err, "wait", w.TaskWaitID, "err", err)
		}
	}
	r.reconcileBoundChecks(ctx, store, waits, now)
	if expired, err := store.ExpireTaskWaits(ctx, now); err != nil {
		logFailure(ctx, r.log, "expire task-bound waits", err, "err", err)
	} else if len(expired) != 0 {
		r.log.Warn("task-bound waits exceeded their maximum duration", "waits", len(expired))
	}
	if r.DryRun {
		// Resuming an attempt is a workflow mutation, so a dry run holds the
		// intent rather than performing it. Waking here and reporting nothing
		// would un-park every task on the fleet the first time someone set the
		// flag, which is the opposite of what a dry run is for.
		r.log.Debug("dry-run: parked attempts are not resumed")
		return
	}
	if _, err := store.WakeTaskWaits(ctx, now); err != nil {
		logFailure(ctx, r.log, "resume parked attempts", err, "err", err)
		return
	}
	pending, err := store.TaskWakesAwaitingDelivery(ctx, now)
	if err != nil {
		logFailure(ctx, r.log, "list task wakes awaiting delivery", err, "err", err)
		return
	}
	control, ok := r.control.(NodeControl)
	if !ok {
		r.log.Error("task wakes cannot be delivered without observable message identity")
		return
	}
	for _, wake := range pending {
		if r.AssignedTaskWakesOnly && (wake.WorkerID == "" || wake.WorkerID != r.TaskWorkerID) {
			continue
		}
		r.deliverTaskWake(ctx, store, control, wake, now)
	}
}

// deliverTaskWake sends one wake at most once.
//
// It borrows the node-wait delivery seam rather than inventing a second one:
// the message carries the wait's own stable delivery ID, sending is durable
// before the send, and an uncertain outcome is resolved only by observing that
// ID in the thread. Absence is not proof of non-delivery, so it never
// authorizes a second send that would start a second turn.
func (r *Runner) deliverTaskWake(ctx context.Context, store TaskWaitStore, control NodeControl, wake domain.TaskWaitWakeContext, now time.Time) {
	log := r.log.With("thread", wake.ThreadID, "attempt", wake.AttemptID)
	seen := map[string]bool{}
	for _, wait := range wake.Waits {
		if wait.DeliveryID == "" {
			log.Error("task wake lacks durable delivery identity", "wait", wait.ID)
			continue
		}
		if seen[wait.DeliveryID] {
			continue
		}
		seen[wait.DeliveryID] = true
		if wait.Delivery == "manual-recovery-required" {
			log.Error("task wake requires manual recovery: grouped payload coverage is unknown", "delivery", wait.DeliveryID)
			continue
		}
		if wait.Delivery == "sending" || wait.Delivery == "recovery-required" {
			found, err := control.ObserveNodeWake(ctx, wake.ThreadID, wait.DeliveryID)
			if err != nil {
				continue
			}
			to := "recovery-required"
			if found {
				to = "delivered"
			}
			if to != wait.Delivery {
				if _, err := store.TransitionTaskWake(ctx, wait.ID, wait.Delivery, to, now); err != nil {
					log.Error("record task wake delivery", "wait", wait.ID, "err", err)
				}
			}
			continue
		}
		thread, err := r.control.GetThread(ctx, wake.ThreadID)
		if err != nil {
			log.Warn("cannot read thread for a task wake", "err", err)
			return
		}
		if thread == nil || thread.ArchivedAt != nil {
			// The attempt is already resumed and its thread is gone. The wake is
			// abandoned rather than retried forever; the attempt's own turn ends
			// with no outputs and fails on that evidence.
			log.Warn("task wake thread is gone; the resumed attempt will fail on its own evidence")
			if _, err := store.TransitionTaskWake(ctx, wait.ID, wait.Delivery, "abandoned", now); err != nil {
				log.Error("abandon task wake", "wait", wait.ID, "err", err)
			}
			continue
		}
		if ok, why := r.healthy(*thread); !ok {
			log.Debug("task wake held", "reason", why)
			return
		}
		claimed, err := store.TransitionTaskWake(ctx, wait.ID, wait.Delivery, "sending", now)
		if err != nil || !claimed {
			continue
		}
		if err := control.SendNodeWake(ctx, *thread, wait.DeliveryID, taskWakeMessage(wake, wait)); err != nil {
			log.Error("deliver task wake", "wait", wait.ID, "err", err)
			if _, err := store.TransitionTaskWake(ctx, wait.ID, "sending", "recovery-required", now); err != nil {
				log.Error("record uncertain task wake", "wait", wait.ID, "err", err)
			}
			continue
		}
		log.Info("task wake delivered", "wait", wait.ID, "resumption", wait.Resumption)
		if _, err := store.TransitionTaskWake(ctx, wait.ID, "sending", "delivered", now); err != nil {
			log.Error("record task wake delivery", "wait", wait.ID, "err", err)
		}
	}
}

// taskWakeMessage renders every outcome in one durable wake delivery.
// A wake that resumed a parked attempt starts the next turn; one that did not
// is evidence arriving mid-turn, and says so, because an agent acts on the two
// differently.
func taskWakeMessage(wake domain.TaskWaitWakeContext, wait domain.TaskWait) string {
	group := domain.TaskWaitWakeContext{
		AttemptID: wake.AttemptID, ThreadID: wake.ThreadID,
		AttemptRevision: wake.AttemptRevision,
	}
	for _, member := range wake.Waits {
		if member.DeliveryID == wait.DeliveryID {
			group.Waits = append(group.Waits, member)
		}
	}
	// The trailer names the wait this delivery is for; a grouped delivery
	// lists the other members in the prose and says how many there are.
	line := taskTrailer(wait)
	if len(group.Waits) > 1 {
		line += " count=" + fmt.Sprint(len(group.Waits))
	}
	return line + "\n\n" + group.Prompt()
}

// taskWaitResult translates a settled check into the structured evidence the
// woken task is given. Silence is not an outcome: which wait, which condition,
// which exit status and how long it ran all travel with it.
func taskWaitResult(w Wait, now time.Time) domain.TaskWaitResult {
	outcome := domain.TaskWaitFailed
	switch w.Status {
	case StatusMet:
		outcome = domain.TaskWaitMet
	case StatusGaveUp:
		outcome = domain.TaskWaitGaveUp
	case StatusTimedOut:
		outcome = domain.TaskWaitTimedOut
	case StatusCancelled:
		outcome = domain.TaskWaitCancelled
	default:
		if w.Outcome != "" {
			// The row records its outcome at settlement; a status that has
			// moved on since (woken) does not change what was observed.
			outcome = domain.TaskWaitOutcome(w.Outcome)
		}
	}
	observed := now
	if w.SettledAt != nil {
		observed = *w.SettledAt
	}
	return domain.TaskWaitResult{
		Outcome: outcome, ExitCode: w.LastExit, Reason: w.Reason, Fields: w.Fields,
		Output: w.LastOutput, RanFor: observed.Sub(w.CreatedAt), ObservedAt: observed,
	}
}
