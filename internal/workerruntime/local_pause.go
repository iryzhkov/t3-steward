package workerruntime

import (
	"context"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// The local quota pause: the worker runtime's own drain and stop of a thread
// it owns, applied when the host watchdog's bucket for the attempt's route is
// draining or stopped. The watchdog leaves owned threads alone (see
// daemon.ThreadOwnership), so this is the only quota stop an owned thread
// receives, and it is a pause the attempt survives: nothing is collected, the
// attempt reports ControlPaused with the bucket as its reason, and the same
// worker resumes the thread once the bucket has recovered and the attempt is
// still live according to the last assignment state it holds.

// pauseForQuota asks the guard whether the running attempt's route must
// pause, and applies the pause through the throttle path when it must. The
// caller has just observed the thread active, so the pause always meets a
// thread mid-work: a draining bucket sends the drain notice once and then
// observes; a stopped bucket also sends the drain notice first, because a
// session that checkpoints and ends its turn on request loses nothing while
// an interrupted one loses its subagents, and escalates to the driver's stop
// only when the thread is still working Config.PauseEscalation after the
// notice. record is updated in place so the caller sees the request that is
// still waiting for the thread to end.
func (r *Runtime) pauseForQuota(ctx context.Context, id string, record *AttemptRecord) error {
	if r.config.Quota == nil || record.Package.Package.Identity.ThreadID == "" {
		return nil
	}
	pause, required, err := r.config.Quota.PauseRequired(ctx, record.Package.Package.Route)
	if err != nil {
		r.log.Warn("host quota state unavailable; attempt keeps running", "assignment", id, "error", err)
		return nil
	}
	if !required {
		if record.LocalThrottle != nil {
			// A drain was requested but the bucket recovered while the thread
			// kept working; the request is moot.
			r.log.Info("quota recovered before the drain took effect; the pause request is withdrawn", "assignment", id)
			if err := r.withdrawLocalPause(id); err != nil {
				return err
			}
			record.LocalThrottle = nil
		}
		return nil
	}
	now := r.now()
	kind := domain.ThrottleCommandDrain
	if pause.Phase == domain.PhaseStopped {
		kind = domain.ThrottleCommandHardStop
	}
	if record.LocalThrottle != nil && record.LocalThrottle.Kind == domain.ThrottleCommandDrain {
		if kind == domain.ThrottleCommandDrain {
			// The drain notice was sent; the thread is expected to checkpoint
			// and end its turn on its own. Sending it again every reconcile
			// would spam the session, so the running branch just observes.
			return nil
		}
		if elapsed := now.Sub(record.LocalThrottle.RequestedAt); elapsed < r.config.PauseEscalation {
			// The bucket is stopped, but the notice is still within the
			// window the daemon gives a stop to take effect.
			r.log.Debug("drain notice stands; the stop follows if the thread keeps working",
				"assignment", id, "escalates_in", (r.config.PauseEscalation - elapsed).Round(time.Second))
			return nil
		}
		r.log.Warn("drain notice not honoured in time; escalating to the stop", "assignment", id,
			"thread", record.Package.Package.Identity.ThreadID, "notice_age", now.Sub(record.LocalThrottle.RequestedAt).Round(time.Second))
	} else if kind == domain.ThrottleCommandHardStop && record.LocalThrottle == nil {
		// First contact with a stopped bucket while the thread is mid-work:
		// the drain form goes first, the stop only after the escalation
		// window. The request keeps the bucket's stopped phase so the notice
		// and the reported pause say what the bucket is.
		kind = domain.ThrottleCommandDrain
	}
	request := LocalThrottleRequest{
		Kind: kind, Bucket: pause.Bucket, Phase: pause.Phase, UsedPercent: pause.UsedPercent,
		LimitName: pause.LimitName, ResetsAt: pause.ResetsAt, ObservedAt: pause.ObservedAt,
		Reason: pause.Summary(), RequestedAt: now,
	}
	if record.LocalThrottle != nil {
		// A hard stop after a drain request keeps the earlier request time,
		// as the watchdog keeps the earliest stop time on its intents.
		request.RequestedAt = record.LocalThrottle.RequestedAt
	}
	// The intent is durable before the effect, so a worker that restarts
	// inside the driver call comes back knowing the stop was its own.
	if err := r.journal.update(func(state *journalState) error {
		current, ok := state.Attempts[id]
		if !ok {
			return fmt.Errorf("worker journal: unknown assignment %q", id)
		}
		current.LocalThrottle = &request
		current.UpdatedAt = now
		state.Attempts[id] = current
		state.Sequence++
		return nil
	}); err != nil {
		return err
	}
	record.LocalThrottle = &request
	pkg := record.Package.Package
	command := r.localThrottleCommand(*record, request)
	r.log.Warn("quota watchdog pauses an owned attempt", "assignment", id, "thread", pkg.Identity.ThreadID,
		"kind", string(kind), "bucket", pause.Bucket.String(), "phase", string(pause.Phase), "used", fmt.Sprintf("%.0f%%", pause.UsedPercent))
	switch kind {
	case domain.ThrottleCommandDrain:
		checkpoint, err := r.driver.Checkpoint(ctx, pkg, command)
		if err != nil {
			// The notice went out but the thread has not ended its turn yet.
			// It keeps running; the next reconcile observes it, and the stop
			// follows once the bucket is stopped and the escalation window
			// has passed.
			r.log.Warn("drain requested; thread has not stopped yet", "assignment", id, "error", err)
			return nil
		}
		return r.markLocalPauseStopped(id, checkpoint)
	default:
		if err := r.driver.StopThread(ctx, pkg); err != nil {
			r.log.Warn("quota stop outcome is unproven; retrying next reconcile", "assignment", id, "error", err)
			return nil
		}
		return r.markLocalPauseStopped(id, nil)
	}
}

// localThrottleCommand shapes the worker's own pause as the throttle command
// the driver already understands, so the drain notice, the stop and the
// resume go through exactly the path a coordinator-delivered command takes.
func (r *Runtime) localThrottleCommand(record AttemptRecord, request LocalThrottleRequest) domain.ThrottleCommand {
	pkg := record.Package.Package
	reason := fmt.Sprintf("quota watchdog on %s: %s is %s at %.0f%%", r.config.WorkerID, request.Bucket, request.Phase, request.UsedPercent)
	if request.ResetsAt != nil {
		reason += ", resets at " + request.ResetsAt.UTC().Format(time.RFC3339)
	}
	return domain.ThrottleCommand{
		ID:        fmt.Sprintf("local-%s-%s-%d", request.Kind, pkg.Identity.AttemptID, request.RequestedAt.UnixNano()),
		AttemptID: pkg.Identity.AttemptID, AssignmentID: record.Assignment.ID, AssignmentEpoch: record.Assignment.Epoch,
		WorkerID: r.config.WorkerID, ThreadID: pkg.Identity.ThreadID, WorkspacePath: record.WorkspacePath,
		Route: pkg.Route, Kind: request.Kind, QuotaPoolID: pkg.Route.QuotaPoolID,
		BucketEpochs: []domain.QuotaBucketEpoch{{Bucket: request.Bucket, Epoch: domain.EpochFor(request.ResetsAt)}},
		Reason:       reason, Checkpoint: request.Checkpoint, CreatedAt: request.RequestedAt,
	}
}

// markLocalPauseStopped records that the paused thread has stopped. The
// phase becomes stopped; collection is withheld by LocalThrottle.
func (r *Runtime) markLocalPauseStopped(id string, checkpoint *domain.CheckpointMetadata) error {
	now := r.now()
	return r.journal.update(func(state *journalState) error {
		current, ok := state.Attempts[id]
		if !ok {
			return fmt.Errorf("worker journal: unknown assignment %q", id)
		}
		if current.LocalThrottle == nil {
			return nil
		}
		request := *current.LocalThrottle
		if request.StoppedAt == nil {
			request.StoppedAt = &now
		}
		if checkpoint != nil {
			request.Checkpoint = checkpoint
		}
		current.LocalThrottle = &request
		current.Phase = PhaseStopped
		current.Failure = ""
		current.ThreadID = current.Package.Package.Identity.ThreadID
		current.ObservedThreadState = string(backlog.DispatchThreadStopped)
		current.UpdatedAt = now
		state.Attempts[id] = current
		state.Sequence++
		return nil
	})
}

// withdrawLocalPause forgets a pause request that never took effect.
func (r *Runtime) withdrawLocalPause(id string) error {
	return r.journal.update(func(state *journalState) error {
		current, ok := state.Attempts[id]
		if !ok || current.LocalThrottle == nil {
			return nil
		}
		current.LocalThrottle = nil
		current.UpdatedAt = r.now()
		state.Attempts[id] = current
		state.Sequence++
		return nil
	})
}

// attemptLive reports whether the attempt is still live according to the
// last assignment state this worker holds: claimed, under a lease that has
// not expired, with no coordinator stop command. A cancelled attempt fails
// this and is never resumed; its thread, if still live, is stopped through
// the ordinary stop path.
func attemptLive(record AttemptRecord, now time.Time) (bool, string) {
	switch {
	case hasCommandRequest(record, domain.WorkerCommandStop):
		return false, "the coordinator stopped the attempt"
	case record.Assignment.State != domain.AssignmentClaimed:
		return false, fmt.Sprintf("assignment is %s", record.Assignment.State)
	case !record.Assignment.LeaseExpiresAt.IsZero() && !record.Assignment.LeaseExpiresAt.After(now):
		return false, "assignment lease expired"
	default:
		return true, ""
	}
}

// reconcileLocalPause advances a locally paused attempt: it resumes the
// thread once the bucket has recovered and the attempt is still live, and
// it notices a thread that runs again by other means.
func (r *Runtime) reconcileLocalPause(ctx context.Context, id string, record AttemptRecord, now time.Time) error {
	pkg := record.Package.Package
	threadState, observeErr := r.driver.ObserveThread(ctx, pkg)
	if observeErr != nil {
		r.log.Warn("T3 observation unavailable; paused attempt waits", "assignment", id, "error", observeErr)
		return nil
	}
	if err := r.noteThreadState(id, threadState); err != nil {
		return err
	}
	switch threadState {
	case backlog.DispatchThreadActive:
		// Someone resumed the thread by hand: the pause is over.
		return r.endLocalPause(id, "thread is running again")
	case backlog.DispatchThreadMissing:
		return r.markUnknown(id, "paused T3 thread is missing")
	}
	if live, why := attemptLive(record, now); !live {
		r.log.Debug("paused attempt is not resumed", "assignment", id, "reason", why)
		return nil
	}
	if r.config.Quota == nil {
		return nil
	}
	allowed, why, err := r.config.Quota.ResumeAllowed(ctx, *record.LocalThrottle, pkg.Route)
	if err != nil {
		r.log.Warn("host quota state unavailable; paused attempt waits", "assignment", id, "error", err)
		return nil
	}
	if !allowed {
		r.log.Debug("paused attempt waits for the quota to recover", "assignment", id, "reason", why)
		return nil
	}
	if pkg.Timeout > 0 && !now.Before(pkg.CreatedAt.Add(pkg.Timeout)) {
		return nil // expireTask handles it on the next pass
	}
	resume := *record.LocalThrottle
	resume.Kind = domain.ThrottleCommandResume
	resume.RequestedAt = now
	command := r.localThrottleCommand(record, resume)
	command.Reason = "quota recovered: " + why
	if err := r.driver.Resume(ctx, pkg, command); err != nil {
		r.log.Warn("resume after quota pause failed; retrying next reconcile", "assignment", id, "error", err)
		return nil
	}
	r.log.Info("quota recovered; owned attempt resumed", "assignment", id, "thread", pkg.Identity.ThreadID, "reason", why)
	return r.endLocalPause(id, why)
}

// endLocalPause moves the pause to LastLocalThrottle and returns the attempt
// to the running phase.
func (r *Runtime) endLocalPause(id, why string) error {
	now := r.now()
	return r.journal.update(func(state *journalState) error {
		current, ok := state.Attempts[id]
		if !ok {
			return fmt.Errorf("worker journal: unknown assignment %q", id)
		}
		if current.LocalThrottle != nil {
			ended := *current.LocalThrottle
			ended.ResumedAt = &now
			current.LastLocalThrottle = &ended
			current.LocalThrottle = nil
		}
		current.Phase = PhaseRunning
		current.Failure = ""
		current.StopObservedSequence = 0
		current.ObservedThreadState = string(backlog.DispatchThreadActive)
		current.UpdatedAt = now
		state.Attempts[id] = current
		state.Sequence++
		return nil
	})
}

// noteThreadState records the last T3 observation for operators. A journal
// write happens only when the observation changed.
func (r *Runtime) noteThreadState(id string, observed backlog.DispatchThreadState) error {
	return r.journal.update(func(state *journalState) error {
		current, ok := state.Attempts[id]
		if !ok || current.ObservedThreadState == string(observed) {
			return nil
		}
		current.ObservedThreadState = string(observed)
		state.Attempts[id] = current
		return nil
	})
}

// pausedCollector is implemented by a driver that can name a recorded quota
// pause in the failure text when it refuses a session that is not ready.
type pausedCollector interface {
	CollectAfterPause(ctx context.Context, pkg workerproto.ExecutionPackage, workspace, pauseReason string) error
}

// collectWithPauseEvidence collects through the driver, naming the most
// recent local quota pause when the driver can carry it.
func (r *Runtime) collectWithPauseEvidence(ctx context.Context, record AttemptRecord) error {
	pause := record.LastLocalThrottle
	if pause == nil {
		pause = record.LocalThrottle
	}
	if collector, ok := r.driver.(pausedCollector); ok && pause != nil {
		return collector.CollectAfterPause(ctx, record.Package.Package, record.WorkspacePath, pause.Reason)
	}
	return r.driver.Collect(ctx, record.Package.Package, record.WorkspacePath)
}
