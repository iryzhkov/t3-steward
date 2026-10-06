package workerruntime

import (
	"context"
	"errors"
	"time"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// SessionTitleDriver is the optional driver ability to read and set the title
// of an execution's thread. A driver without it keeps the initial title, which
// is the behaviour before live titles existed.
type SessionTitleDriver interface {
	// ThreadTitle reports the thread's current title, and false when the
	// driver holds no thread it can retitle.
	ThreadTitle(context.Context, workerproto.ExecutionPackage) (string, bool, error)
	SetThreadTitle(context.Context, workerproto.ExecutionPackage, string) error
}

// ThreadTitleUpdater is the optional T3 control ability to set a thread title.
type ThreadTitleUpdater interface {
	UpdateThreadTitle(ctx context.Context, threadID, title string) error
}

// SessionTitleRecord is what the worker knows about the titles it set on one
// execution's thread.
type SessionTitleRecord struct {
	// Last is the last title Steward confirmed it set. Empty means the initial
	// title Steward created the thread with.
	Last string `json:"last,omitempty"`
	// Pending is a title dispatched without a confirmed outcome. It is recorded
	// before the effect, so that after a lost response or a restart the worker
	// recognises the title as its own rather than as an operator's rename.
	Pending string `json:"pending,omitempty"`
	// Stopped is set when the operator renamed the thread. Steward never
	// updates that thread's title again.
	Stopped    bool       `json:"stopped,omitempty"`
	StopReason string     `json:"stopReason,omitempty"`
	StoppedAt  *time.Time `json:"stoppedAt,omitempty"`
}

// applySessionStates stores the coordinator's complete statement of its
// executions' recorded lifecycle. A statement that is absent clears the
// previous one, so a coordinator that stops reporting cannot leave titles
// frozen on what it last said. An entry behind the revision already applied to
// the same execution keeps the newer one, as parked entries do.
func applySessionStates(state *journalState, request workerproto.SnapshotRequest) {
	if !request.SessionStatesReported {
		state.SessionStatesReported = false
		state.SessionStates = nil
		return
	}
	next := make(map[string]workerproto.AssignmentSessionState, len(request.SessionStates))
	for _, statement := range request.SessionStates {
		if previous, ok := state.SessionStates[statement.AssignmentID]; ok &&
			previous.AssignmentEpoch == statement.AssignmentEpoch && previous.AttemptID == statement.AttemptID &&
			statement.AttemptRevision < previous.AttemptRevision {
			next[statement.AssignmentID] = previous
			continue
		}
		next[statement.AssignmentID] = statement
	}
	state.SessionStatesReported = true
	state.SessionStates = next
}

// UpdateSessionTitles brings each execution's thread title in step with the
// coordinator's last statement. It is best effort: every failure is logged and
// nothing is returned, so a title can never block admission, dispatch,
// execution or collection. A failed update is retried when the stated title
// changes, not on every exchange; a restart retries it once.
//
// It is idempotent: a title equal to the last one Steward set is not sent
// again, and is not even read from T3.
func (r *Runtime) UpdateSessionTitles(ctx context.Context) {
	titles, ok := r.driver.(SessionTitleDriver)
	if !ok {
		return
	}
	state, err := r.journal.snapshot()
	if err != nil {
		r.log.Warn("session titles not updated: journal unavailable", "error", err)
		return
	}
	if !state.SessionStatesReported {
		return
	}
	for _, id := range sortedAttemptIDs(state.Attempts) {
		record := state.Attempts[id]
		statement, ok := state.SessionStates[id]
		if !ok || statement.AssignmentEpoch != record.Assignment.Epoch || statement.AttemptID != record.Assignment.AttemptID {
			// A statement about another execution of this assignment says
			// nothing about the thread held here.
			continue
		}
		r.updateSessionTitle(ctx, titles, record, statement)
	}
}

func (r *Runtime) updateSessionTitle(ctx context.Context, titles SessionTitleDriver, record AttemptRecord, statement workerproto.AssignmentSessionState) {
	pkg := record.Package.Package
	if pkg.Display == nil {
		// A package without frozen display metadata keeps its initial title.
		return
	}
	switch record.Phase {
	case PhaseClaimed, PhasePreparing, PhasePrepared, PhaseDispatching:
		// The thread may not exist yet; its creation sets the initial title.
		return
	}
	var current SessionTitleRecord
	if record.SessionTitle != nil {
		current = *record.SessionTitle
	}
	if current.Stopped {
		return
	}
	id := record.Assignment.ID
	desired := workerproto.SessionTitle(pkg, statement.State, statement.Progress)
	last := current.Last
	if last == "" {
		last = workerproto.InitialSessionTitle(pkg)
	}
	if desired == last && current.Pending == "" {
		return
	}
	if r.titleFailed(id, desired) {
		return
	}
	observed, found, err := titles.ThreadTitle(ctx, pkg)
	if err != nil {
		r.markTitleFailed(id, desired)
		r.log.Warn("session title not updated: current title unavailable", "assignment", id, "error", err)
		return
	}
	if !found {
		return
	}
	if observed == desired {
		// Already showing it: a dispatch whose response was lost, or a restart
		// after one. Record it rather than sending it again.
		r.recordSessionTitle(record, func(title *SessionTitleRecord) { title.Last, title.Pending = desired, "" })
		return
	}
	if observed != last && (current.Pending == "" || observed != current.Pending) {
		// T3 has no fenced title mutation, so this comparison is the only
		// protection of an operator's rename, and it is not atomic with the
		// write below: a rename landing between the two is overwritten once.
		now := r.now()
		reason := "operator renamed the thread: its title differs from the last title Steward set"
		r.recordSessionTitle(record, func(title *SessionTitleRecord) {
			title.Stopped, title.StopReason, title.StoppedAt = true, reason, &now
		})
		r.log.Info("session title updates stopped", "assignment", id, "reason", reason, "title", observed)
		return
	}
	// The thread shows either the last title or the pending one, so the
	// observed title is Steward's own and is confirmed as the last title in the
	// same write that replaces the pending one. Otherwise a restart between this
	// write and the dispatch would find a title that is neither, and read
	// Steward's own title as an operator's rename.
	if !r.recordSessionTitle(record, func(title *SessionTitleRecord) { title.Last, title.Pending = observed, desired }) {
		return
	}
	if err := titles.SetThreadTitle(ctx, pkg, desired); err != nil {
		r.markTitleFailed(id, desired)
		r.log.Warn("session title not updated; retried at the next state change", "assignment", id, "error", err)
		return
	}
	r.clearTitleFailure(id)
	r.recordSessionTitle(record, func(title *SessionTitleRecord) { title.Last, title.Pending = desired, "" })
}

// recordSessionTitle changes the title record of the execution the caller read,
// and only that execution. It reports whether the change was stored.
func (r *Runtime) recordSessionTitle(read AttemptRecord, change func(*SessionTitleRecord)) bool {
	err := r.journal.update(func(state *journalState) error {
		record, ok := state.Attempts[read.Assignment.ID]
		if !ok || record.Assignment.Epoch != read.Assignment.Epoch || record.Assignment.AttemptID != read.Assignment.AttemptID {
			return errSessionTitleSuperseded
		}
		var title SessionTitleRecord
		if record.SessionTitle != nil {
			title = *record.SessionTitle
		}
		change(&title)
		record.SessionTitle = &title
		state.Attempts[read.Assignment.ID] = record
		return nil
	})
	if err != nil {
		if !errors.Is(err, errSessionTitleSuperseded) {
			r.log.Warn("session title record not stored", "assignment", read.Assignment.ID, "error", err)
		}
		return false
	}
	return true
}

var errSessionTitleSuperseded = errors.New("session title: execution superseded")

// The failure memory is deliberately not durable: a restart retries a failed
// title once, which is how titles converge after a worker restart.
func (r *Runtime) titleFailed(id, title string) bool {
	r.titleMu.Lock()
	defer r.titleMu.Unlock()
	return r.titleFailures[id] == title
}

func (r *Runtime) markTitleFailed(id, title string) {
	r.titleMu.Lock()
	defer r.titleMu.Unlock()
	if r.titleFailures == nil {
		r.titleFailures = map[string]string{}
	}
	r.titleFailures[id] = title
}

func (r *Runtime) clearTitleFailure(id string) {
	r.titleMu.Lock()
	defer r.titleMu.Unlock()
	delete(r.titleFailures, id)
}

// ThreadTitle reads the title of the execution's thread. No-effects mode and
// drivers whose T3 control cannot set titles report no thread, so their titles
// are left as they are. A contained execution's thread is read through its
// identity-fenced scoped T3, never the host's.
func (d *LocalDriver) ThreadTitle(ctx context.Context, pkg workerproto.ExecutionPackage) (string, bool, error) {
	d, err := d.titleDriver(ctx, pkg)
	if err != nil || d == nil {
		return "", false, err
	}
	thread, err := d.T3.GetThread(ctx, pkg.Identity.ThreadID)
	if err != nil || thread == nil {
		return "", false, err
	}
	return thread.Title, true, nil
}

// SetThreadTitle sets the title of the execution's thread.
func (d *LocalDriver) SetThreadTitle(ctx context.Context, pkg workerproto.ExecutionPackage, title string) error {
	d, err := d.titleDriver(ctx, pkg)
	if err != nil {
		return err
	}
	if d == nil {
		return errors.New("this execution's thread cannot be retitled")
	}
	return d.T3.(ThreadTitleUpdater).UpdateThreadTitle(ctx, pkg.Identity.ThreadID, title)
}

// titleDriver is the driver whose T3 holds the execution's thread, or nil when
// that thread cannot be retitled: no-effects mode, or a control without the
// ability, such as the retained record of a stopped contained execution. A
// contained execution attaches to its existing supervisor, which never
// launches one, and a failed attachment never falls back to the host's T3.
func (d *LocalDriver) titleDriver(ctx context.Context, pkg workerproto.ExecutionPackage) (*LocalDriver, error) {
	if d.Config.DryRun {
		return nil, nil
	}
	if scoped, err := d.scopedDriver(ctx, pkg); err != nil {
		return nil, err
	} else if scoped != nil {
		d = scoped
	}
	if d.T3 == nil {
		return nil, nil
	}
	control := d.T3
	if cached, ok := control.(*CachedT3); ok {
		// The cache forwards the ability only when the control it wraps has it.
		control = cached.Inner
	}
	if _, ok := control.(ThreadTitleUpdater); !ok {
		return nil, nil
	}
	return d, nil
}

// UpdateThreadTitle forwards to the wrapped control and invalidates the cached
// listing, like every other mutation.
func (c *CachedT3) UpdateThreadTitle(ctx context.Context, threadID, title string) error {
	updater, ok := c.Inner.(ThreadTitleUpdater)
	if !ok {
		return errors.New("T3 control cannot update thread titles")
	}
	defer c.invalidate()
	return updater.UpdateThreadTitle(ctx, threadID, title)
}
