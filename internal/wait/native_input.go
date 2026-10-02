package wait

import (
	"context"
	"sort"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// NativeInputStore records native question activity into a run's events.
type NativeInputStore interface {
	RecordNativeUserInput(ctx context.Context, workerID, attemptID, threadID string, events []domain.UserInputEvent, now time.Time) (int, error)
}

// NativeInputControl reads a task thread and its question cards.
type NativeInputControl interface {
	GetThread(ctx context.Context, threadID string) (*domain.Thread, error)
	UserInputEvents(ctx context.Context, threadID string) ([]domain.UserInputEvent, error)
}

// maxNativeInputReport bounds one report; it matches the coordinator's bound.
const maxNativeInputReport = 64

// tickNativeInput records the questions a task asks with its provider's own
// question tool in its own thread, and their answers, in the run's events.
// The card is already visible as Awaiting Input in every T3 UI and the
// attempt is kept alive while it is pending; what was missing is that the run
// kept no record of the decision.
//
// A thread is read only while T3 reports pending input on it, and once more
// after, to catch the answer; every other task thread costs nothing beyond
// the shell read GetThread already makes. Each activity is reported once per
// process, and the coordinator records it once ever.
func (r *Runner) tickNativeInput(ctx context.Context) {
	if r.TaskThreads == nil || r.DisableTaskWaitRuntime || r.DryRun || r.TaskWorkerID == "" {
		return
	}
	store := r.NativeStore
	if store == nil {
		store, _ = r.TaskStore.(NativeInputStore)
	}
	if store == nil {
		store, _ = r.store.(NativeInputStore)
	}
	control, ok := r.control.(NativeInputControl)
	if store == nil || !ok {
		return
	}
	threads, err := r.TaskThreads(ctx)
	if err != nil {
		logFailure(ctx, r.log, "list task threads for native questions", err, "err", err)
		return
	}
	if r.nativeWatched == nil {
		r.nativeWatched = map[string]bool{}
		r.nativeReported = map[string]map[string]bool{}
	}
	if r.nativePending == nil {
		r.nativePending = map[string][]domain.UserInputEvent{}
	}
	for thread := range r.nativeWatched {
		if _, live := threads[thread]; !live {
			delete(r.nativeWatched, thread)
			delete(r.nativeReported, thread)
			delete(r.nativePending, thread)
		}
	}
	ids := make([]string, 0, len(threads))
	for thread := range threads {
		ids = append(ids, thread)
	}
	sort.Strings(ids)
	for _, threadID := range ids {
		thread, err := control.GetThread(ctx, threadID)
		if err != nil || thread == nil {
			continue
		}
		if !thread.HasPendingUserInput && !r.nativeWatched[threadID] {
			continue
		}
		events, err := control.UserInputEvents(ctx, threadID)
		if err != nil {
			r.log.Warn("read a task thread's question cards", "thread", threadID, "err", err)
			continue
		}
		open := map[string]bool{}
		reported := r.nativeReported[threadID]
		if reported == nil {
			reported = map[string]bool{}
			r.nativeReported[threadID] = reported
		}
		fresh := false
		for _, event := range events {
			if event.Kind == domain.UserInputRequested {
				open[event.RequestID] = true
			} else {
				delete(open, event.RequestID)
			}
			if !reported[event.ActivityID] {
				fresh = true
			}
		}
		r.nativeWatched[threadID] = thread.HasPendingUserInput || len(open) != 0
		if !fresh {
			continue
		}
		if len(events) > maxNativeInputReport {
			events = events[len(events)-maxNativeInputReport:]
		}
		// The batch is kept until the coordinator acknowledges it, whatever
		// the thread shows next: an answer read once and lost to a failed
		// report would otherwise never be read again, because the card is no
		// longer pending.
		r.nativePending[threadID] = events
	}
	pending := make([]string, 0, len(r.nativePending))
	for threadID := range r.nativePending {
		pending = append(pending, threadID)
	}
	sort.Strings(pending)
	for _, threadID := range pending {
		events := r.nativePending[threadID]
		if _, err := store.RecordNativeUserInput(ctx, r.TaskWorkerID, threads[threadID], threadID, events, r.now()); err != nil {
			r.log.Warn("record a task's native question; retrying next tick", "thread", threadID, "err", err)
			continue
		}
		reported := r.nativeReported[threadID]
		if reported == nil {
			reported = map[string]bool{}
			r.nativeReported[threadID] = reported
		}
		for _, event := range events {
			reported[event.ActivityID] = true
		}
		delete(r.nativePending, threadID)
	}
}
