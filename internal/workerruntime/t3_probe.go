package workerruntime

import (
	"context"
	"sync"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// t3ProbeTimeout bounds one probe made with the host lock released. A probe
// that does not answer within it is an unavailable observation, which every
// caller already treats as "decide on a later pass".
const t3ProbeTimeout = time.Minute

// maxProbeRounds bounds how many times one reconcile tick runs probes with
// the lock released and takes the lock again to act on their answers. Two
// is what the longest chain needs today: the collection decision's turn
// observation, then the workspace inspection collection itself makes.
const maxProbeRounds = 2

// turnObserver is the optional driver method that binds a stopped observation
// to a concrete provider turn.
type turnObserver interface {
	ObserveThreadTurn(context.Context, workerproto.ExecutionPackage) (backlog.DispatchThreadState, string, error)
}

type probeKind string

const (
	// probeCollection is the evidence the collection decision needs: the
	// provider turn identity and, when the worker is configured with one, the
	// task-wait probe. Both are gathered in the order the decision reads them.
	probeCollection probeKind = "collection"
	// probeWorkspace is the inspection of the attempt's prepared workspace.
	probeWorkspace probeKind = "workspace"
)

// probeKey names the journal state a probe was requested for. An answer is
// used only while the attempt's record still matches it; otherwise the
// attempt moved while the lock was released (it was superseded, stopped,
// paused, collected, prepared again) and the answer describes a state the
// worker is no longer in.
//
// Fields a pass rewrites without changing what the probe is about, such as
// the last observed thread state, the lease expiry and the update time, are
// deliberately left out: including them would discard every answer whenever
// a lease renewal or snapshot happened to land between the two passes.
type probeKey struct {
	kind            probeKind
	assignmentID    string
	assignmentEpoch int64
	attemptID       string
	threadID        string
	phase           Phase
	workspacePath   string
	prepareAttempts int
	commands        int
	stopConfirmed   bool
	paused          bool
}

func probeKeyFor(kind probeKind, record AttemptRecord) probeKey {
	return probeKey{
		kind: kind, assignmentID: record.Assignment.ID, assignmentEpoch: record.Assignment.Epoch,
		attemptID: record.Assignment.AttemptID, threadID: record.Package.Package.Identity.ThreadID,
		phase: record.Phase, workspacePath: record.WorkspacePath, prepareAttempts: record.PrepareAttempts,
		commands: len(record.CommandRequests), stopConfirmed: record.StopConfirmed,
		paused: record.LocalThrottle != nil || record.PendingThrottle != nil,
	}
}

// probeAnswer is what one probe observed.
type probeAnswer struct {
	// turn evidence, for probeCollection with a turn-observing driver.
	turnState backlog.DispatchThreadState
	turnID    string
	turnErr   error
	// task-wait evidence, for probeCollection with Config.LiveTaskWait.
	waiting bool
	waitErr error
	// workspace evidence, for probeWorkspace.
	workspace string
	exists    bool
	inspected error
}

type probeRequest struct {
	key     probeKey
	runtime *Runtime
	pkg     workerproto.ExecutionPackage
	answer  *probeAnswer
}

// t3Probes keeps the slow observations of a reconcile pass off the lock
// every worker exchange needs.
//
// A persistent worker serializes every exchange and reconcile tick behind
// CatalogHost.mu. The collection decision asks T3 for the provider turn,
// may ask the configured task-wait probe whether the attempt is parked, and
// inspects the prepared workspace. Each of those used to run under that lock,
// bounded only by the exchange timeout, so one slow T3 call held every
// snapshot, offer, lease renewal and result exchange of the worker behind it.
//
// A pass carrying a t3Probes never makes those calls. Where it needs one it
// records a request keyed by the attempt's journal state and defers the
// decision, which every such site already does when T3 cannot answer. The
// reconcile tick then runs the requests with the lock released and takes the
// lock again for a second pass, which uses an answer only when the attempt's
// record still matches the key it was requested for. An answer that went
// stale in between is discarded and asked for again on the next tick. An
// exchange carries a t3Probes that is never run, so it defers these
// decisions to the tick instead of waiting for T3 itself.
//
// A pass without a t3Probes, as a bare Runtime runs in tests and in a
// one-shot worker that holds no shared lock, calls the driver directly.
type t3Probes struct {
	mu       sync.Mutex
	requests map[string]*probeRequest
	order    []string
}

type t3ProbesKey struct{}

func withT3Probes(ctx context.Context, probes *t3Probes) context.Context {
	if probes == nil {
		probes = &t3Probes{}
	}
	return context.WithValue(ctx, t3ProbesKey{}, probes)
}

func t3ProbesFrom(ctx context.Context) *t3Probes {
	probes, _ := ctx.Value(t3ProbesKey{}).(*t3Probes)
	return probes
}

func probeSlot(kind probeKind, assignmentID string) string {
	return string(kind) + "\x00" + assignmentID
}

// take returns the answer for record's current state, consuming it, or
// records a request for it and reports false. An answer whose key no longer
// matches the record, or that was gathered through a runtime since replaced,
// is discarded.
func (p *t3Probes) take(r *Runtime, kind probeKind, record AttemptRecord) (probeAnswer, bool) {
	key := probeKeyFor(kind, record)
	slot := probeSlot(kind, record.Assignment.ID)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.requests == nil {
		p.requests = make(map[string]*probeRequest)
	}
	if existing, ok := p.requests[slot]; ok {
		if existing.answer != nil {
			delete(p.requests, slot)
			if existing.key == key && existing.runtime == r {
				return *existing.answer, true
			}
			r.log.Info("T3 observation went stale while the worker lock was released; it is discarded and taken again",
				"assignment", record.Assignment.ID, "probe", string(kind))
		} else if existing.key == key && existing.runtime == r {
			return probeAnswer{}, false
		}
	}
	p.requests[slot] = &probeRequest{key: key, runtime: r, pkg: record.Package.Package}
	p.order = append(p.order, slot)
	return probeAnswer{}, false
}

// run answers every outstanding request, in the order they were made, and
// reports whether any was answered. It must be called without the host lock.
func (p *t3Probes) run(ctx context.Context) bool {
	p.mu.Lock()
	var pending []*probeRequest
	for _, slot := range p.order {
		if request, ok := p.requests[slot]; ok && request.answer == nil {
			pending = append(pending, request)
		}
	}
	p.order = nil
	p.mu.Unlock()
	for _, request := range pending {
		if ctx.Err() != nil {
			break
		}
		answer := request.runtime.probe(ctx, request.key.kind, request.pkg)
		p.mu.Lock()
		request.answer = &answer
		p.mu.Unlock()
	}
	return len(pending) != 0
}

// probe makes the driver calls behind one request.
func (r *Runtime) probe(ctx context.Context, kind probeKind, pkg workerproto.ExecutionPackage) probeAnswer {
	ctx, cancel := context.WithTimeout(ctx, t3ProbeTimeout)
	defer cancel()
	var answer probeAnswer
	switch kind {
	case probeCollection:
		if observer, ok := r.driver.(turnObserver); ok {
			answer.turnState, answer.turnID, answer.turnErr = observer.ObserveThreadTurn(ctx, pkg)
		}
		if r.config.LiveTaskWait != nil {
			answer.waiting, answer.waitErr = r.config.LiveTaskWait(ctx, pkg)
		}
	case probeWorkspace:
		answer.workspace, answer.exists, answer.inspected = r.driver.InspectWorkspace(ctx, pkg)
	}
	return answer
}

// currentRecord reads the attempt as the journal holds it now, which is what
// a probe key must describe: callers often hold a copy from before an earlier
// step of the same pass moved the phase.
func (r *Runtime) currentRecord(id string) (AttemptRecord, bool, error) {
	state, err := r.journal.snapshot()
	if err != nil {
		return AttemptRecord{}, false, err
	}
	record, ok := state.Attempts[id]
	return record, ok, nil
}

// probed answers kind for the attempt from the pass's probes. With no probes
// on ctx it reports direct: the caller makes the driver calls itself, as
// before. Otherwise ready is false when the decision must wait for a probe
// the reconcile tick runs with the lock released. err is a journal error.
func (r *Runtime) probed(ctx context.Context, kind probeKind, id string) (answer probeAnswer, direct, ready bool, err error) {
	probes := t3ProbesFrom(ctx)
	if probes == nil {
		return probeAnswer{}, true, true, nil
	}
	current, ok, err := r.currentRecord(id)
	if err != nil || !ok {
		return probeAnswer{}, false, false, err
	}
	answer, ready = probes.take(r, kind, current)
	if !ready {
		r.log.Debug("decision waits for an observation made without the worker lock", "assignment", id, "probe", string(kind))
	}
	return answer, false, ready, nil
}
