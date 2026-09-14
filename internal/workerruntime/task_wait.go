package workerruntime

import (
	"context"
	"errors"
	"time"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// ErrParkedReportStale reports that the coordinator's last statement about
// parked assignments is too old to act on. Collection defers until a fresh one
// arrives, because collecting is the dangerous direction: it publishes the
// outputs of a task that may not have written them yet and lets the coordinator
// verify against those.
var ErrParkedReportStale = errors.New("parked-assignment report is stale")

// ApplyParkedAssignments records the coordinator's complete statement of which
// of this worker's assignments are parked on a task-bound wait.
//
// The statement replaces whatever the worker believed. An entry naming an
// attempt revision older than one already recorded for the same assignment
// keeps the newer entry, because reports can overtake each other on a retried
// exchange.
//
// That rule is not a fence, and does not claim to be one. It cannot tell a late
// report from a current one that simply has not moved, so an overtaking report
// can leave an assignment marked parked for one more exchange after the park
// ended. The error is in the safe direction, it is self-healing on the next
// report, and the coordinator refuses the collection either way.
func (r *Runtime) ApplyParkedAssignments(request workerproto.SnapshotRequest) error {
	if err := workerproto.ValidateSnapshotRequest(request); err != nil {
		return err
	}
	if !request.ParkedReported {
		// An older coordinator says nothing about parked assignments. Keeping
		// the previous statement would freeze it forever, so the worker goes on
		// behaving as it did before task-bound waits existed and relies on the
		// coordinator's refusal instead.
		return nil
	}
	now := r.now()
	return r.journal.update(func(state *journalState) error {
		next := make(map[string]workerproto.ParkedAssignment, len(request.Parked))
		for _, parked := range request.Parked {
			if previous, ok := state.Parked[parked.AssignmentID]; ok && parked.AttemptRevision < previous.AttemptRevision {
				r.log.Warn("parked assignment report is behind the one already applied; entry kept",
					"assignment", parked.AssignmentID,
					"reported_revision", parked.AttemptRevision, "applied_revision", previous.AttemptRevision)
				next[parked.AssignmentID] = previous
				continue
			}
			next[parked.AssignmentID] = parked
		}
		state.Parked = next
		state.ParkedReported = true
		state.ParkedObservedAt = now
		state.Sequence++
		return nil
	})
}

// liveTaskWait answers whether the coordinator still holds a task-bound wait
// for one assignment.
//
// Config.LiveTaskWait overrides it for an embedded worker that can read
// coordinator state directly. Otherwise the answer comes from the last
// statement the coordinator made over the worker protocol, which is the
// production path: the worker is told, and never asks.
func (r *Runtime) liveTaskWait(ctx context.Context, record AttemptRecord) (bool, error) {
	if r.config.LiveTaskWait != nil {
		return r.config.LiveTaskWait(ctx, record.Package.Package)
	}
	state, err := r.journal.snapshot()
	if err != nil {
		return false, err
	}
	if !state.ParkedReported {
		// This coordinator has never reported parked assignments, so there are
		// none to report: task-bound waits and the report were added together.
		return false, nil
	}
	if age := r.now().Sub(state.ParkedObservedAt); age > r.parkedReportLifetime() {
		return false, ErrParkedReportStale
	}
	parked, ok := state.Parked[record.Assignment.ID]
	if !ok {
		return false, nil
	}
	if parked.AssignmentEpoch != record.Assignment.Epoch {
		// The statement is about a different execution of this assignment. It
		// says nothing about the one held here, and silence is not "not
		// parked".
		return false, ErrParkedReportStale
	}
	return true, nil
}

// parkedReportLifetime is how long a coordinator statement may be believed. It
// is the snapshot freshness the fleet already uses, because the statement rides
// the snapshot exchange and ages at the same rate.
func (r *Runtime) parkedReportLifetime() time.Duration {
	if r.config.SnapshotTTL > 0 {
		return r.config.SnapshotTTL
	}
	return time.Minute
}
