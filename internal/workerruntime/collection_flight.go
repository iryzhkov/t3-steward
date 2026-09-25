package workerruntime

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DefaultFinalizationTimeout bounds one collection of an attempt: reading the
// finished turn, running every declared verification command, and publishing
// the result. It is deliberately generous. Verification commands that check a
// real system take minutes, and what used to bound them instead was whatever
// reconcile pass or exchange happened to reach the attempt first.
const DefaultFinalizationTimeout = time.Hour

// collectionWaitGrace is how long a pass waits for a collection it has just
// started before leaving it to finish in the background. A collection that
// is quick completes inside the pass that started it, as it always did; a
// long one no longer holds the pass, or the exchange around it, hostage.
const collectionWaitGrace = 10 * time.Second

// errCollectionRunning reports that an attempt's collection is still running
// from an earlier pass. Its result is taken by a later pass.
var errCollectionRunning = errors.New("finalization is still running from an earlier pass; its result is taken on a later one")

// errCollectionNotClaimed is the outcome of a collection that was registered
// but never started, because the journal no longer named the attempt as
// collecting once the registration was in place. A pass that finds it treats
// it like any other failed collection: nothing was published and nothing is
// finalized from it.
var errCollectionNotClaimed = errors.New("the attempt is no longer collecting; the collection was not started")

// collectionFlight is one collection running for one attempt.
type collectionFlight struct {
	done chan struct{}
	err  error
}

// collectionPass is how a caller that holds a lock other work needs reaches
// collections without waiting for them.
//
// A persistent worker serializes every exchange and reconcile tick behind one
// lock (CatalogHost.mu). A pass that waited under that lock for the collection
// it had just started held every snapshot, offer, lease renewal and result
// exchange of the worker for as long as it waited. A pass carrying a
// collectionPass starts the collection and returns at once; the caller waits
// for the collections it started after releasing its lock, and then takes
// their results in a fresh pass.
type collectionPass struct {
	mu      sync.Mutex
	started []*collectionFlight
}

type collectionPassKey struct{}

// withCollectionPass marks ctx as belonging to a caller that must not wait
// for a collection while it holds its lock. pass may be nil when the caller
// has no use for the collections it starts: a later pass takes their results.
func withCollectionPass(ctx context.Context, pass *collectionPass) context.Context {
	if pass == nil {
		pass = &collectionPass{}
	}
	return context.WithValue(ctx, collectionPassKey{}, pass)
}

func collectionPassFrom(ctx context.Context) *collectionPass {
	pass, _ := ctx.Value(collectionPassKey{}).(*collectionPass)
	return pass
}

func (p *collectionPass) add(flight *collectionFlight) {
	p.mu.Lock()
	p.started = append(p.started, flight)
	p.mu.Unlock()
}

// wait waits up to grace for the collections this pass started, and reports
// whether any of them has finished, so that the caller knows a further pass
// would take a result. It must be called without the caller's lock held.
func (p *collectionPass) wait(ctx context.Context, grace time.Duration) bool {
	p.mu.Lock()
	started := append([]*collectionFlight(nil), p.started...)
	p.mu.Unlock()
	if len(started) == 0 {
		return false
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	finished := false
	for _, flight := range started {
		select {
		case <-flight.done:
			finished = true
		case <-timer.C:
			return finished
		case <-ctx.Done():
			return finished
		}
	}
	return finished
}

// collectionFlights holds the collections running in this process.
//
// It is process-wide rather than a field of the Runtime because a persistent
// worker replaces its Runtime when the coordinator epoch moves, over the same
// journal and the same workspaces. A registry owned by the Runtime would let
// the replacement start a second verification of an attempt the first one is
// still verifying. The key names the journal, so two workers in one process
// (tests do this) never share an entry.
var collectionFlights = struct {
	sync.Mutex
	running map[string]*collectionFlight
}{running: make(map[string]*collectionFlight)}

func (r *Runtime) collectionFlightKey(record AttemptRecord) string {
	return strings.Join([]string{
		r.journal.root, record.Assignment.ID,
		strconv.FormatInt(record.Assignment.Epoch, 10),
		record.Package.Package.Identity.AttemptID,
	}, "\x00")
}

// collectionRunning reports whether a collection of this attempt is running.
func (r *Runtime) collectionRunning(record AttemptRecord) bool {
	collectionFlights.Lock()
	flight, ok := collectionFlights.running[r.collectionFlightKey(record)]
	collectionFlights.Unlock()
	if !ok {
		return false
	}
	select {
	case <-flight.done:
		return false
	default:
		return true
	}
}

// collectionRegistered reports whether a collection of this attempt is running
// or has finished without its result having been taken yet. Either way the
// attempt's outcome belongs to that collection: its result may already be in
// custody, and nothing may decide the attempt differently until a pass takes
// it.
func (r *Runtime) collectionRegistered(record AttemptRecord) bool {
	collectionFlights.Lock()
	_, ok := collectionFlights.running[r.collectionFlightKey(record)]
	collectionFlights.Unlock()
	return ok
}

// DrainCollections waits until every collection this process started has
// finished, or ctx ends. A worker calls it on shutdown after cancelling its
// lifetime, so that no verification it started is still running when the
// next process collects the same attempt again.
func DrainCollections(ctx context.Context) error {
	collectionFlights.Lock()
	pending := make([]*collectionFlight, 0, len(collectionFlights.running))
	for _, flight := range collectionFlights.running {
		pending = append(pending, flight)
	}
	collectionFlights.Unlock()
	for _, flight := range pending {
		select {
		case <-flight.done:
		case <-ctx.Done():
			return fmt.Errorf("collections still running at shutdown: %w", ctx.Err())
		}
	}
	return nil
}

// finalizationTimeout is the budget one collection of the attempt runs under.
func (r *Runtime) finalizationTimeout(record AttemptRecord) time.Duration {
	budget := r.config.FinalizationTimeout
	if budget <= 0 {
		budget = DefaultFinalizationTimeout
	}
	pkg := record.Package.Package
	if declared := time.Duration(len(pkg.Verification)) * pkg.Limits.VerificationTimeout; declared > budget {
		budget = declared
	}
	return budget
}

// collectOnce runs the driver's collection of an attempt exactly once at a
// time, under the attempt's own finalization budget.
//
// ctx is the reconcile pass or exchange that reached the attempt. It is used
// only to decide how long to wait here, never to bound the collection: the
// worker's reconcile passes and exchanges are cut at two minutes or less, and
// a verification command cut at that point is killed, reported as a deferred
// collection, and started again from the beginning by the next pass, which is
// cut at the same point. Worse, a turn read again after the provider session
// has idled out no longer reads as finished, so the eventual result was a
// failure for a task whose outputs and verification were both good.
//
// A collection that finishes within the wait completes in this call. One that
// does not keeps running, and every later pass that reaches the attempt
// before it finishes defers instead of starting a second one. The pass after
// it finishes takes its result. A ctx carrying a collectionPass does not wait
// at all: its caller holds a lock that exchanges need, and waits for the
// collection after releasing it. Nothing here is durable: a worker that
// restarts mid-collection finds the attempt collecting in its journal and
// collects it again, as it always has.
//
// It returns a finished collection, still registered, whose outcome the caller
// records in the journal and then releases; errCollectionRunning while the
// collection runs; or neither when the journal no longer names the attempt as
// collecting, so that there is nothing to collect.
//
// The registry entry is what makes a collection exactly-once, and it spans
// both journal transitions. It is added before the journal is read again to
// confirm that the attempt is still collecting, and it is removed only after
// the finished outcome is in the journal (see finishCollection). A second
// pass racing this one therefore either finds the entry, or reads a journal
// that already records the outcome; it can never start a second collection
// of an attempt the first has finished.
func (r *Runtime) collectOnce(ctx context.Context, id string, record AttemptRecord) (*collectionFlight, error) {
	key := r.collectionFlightKey(record)
	collectionFlights.Lock()
	flight, running := collectionFlights.running[key]
	started := !running
	if started {
		flight = &collectionFlight{done: make(chan struct{})}
		collectionFlights.running[key] = flight
	}
	collectionFlights.Unlock()

	if !started {
		select {
		case <-flight.done:
			return flight, nil
		default:
			return nil, errCollectionRunning
		}
	}
	claimed, err := r.collectionClaimed(id, record)
	if err != nil || !claimed {
		flight.err = errCollectionNotClaimed
		close(flight.done)
		releaseCollection(key, flight)
		return nil, err
	}
	budget := r.finalizationTimeout(record)
	flightCtx, cancel := context.WithTimeout(r.config.Lifetime, budget)
	go func() {
		defer cancel()
		flight.err = r.collectWithPauseEvidence(flightCtx, record)
		close(flight.done)
	}()

	if pass := collectionPassFrom(ctx); pass != nil {
		pass.add(flight)
		r.log.Info("collection started; it runs outside the exchange lock and its result is taken by a later pass",
			"assignment", record.Assignment.ID, "attempt", record.Package.Package.Identity.AttemptID, "budget", budget)
		return nil, fmt.Errorf("%w (started this pass)", errCollectionRunning)
	}
	timer := time.NewTimer(collectionWaitGrace)
	defer timer.Stop()
	select {
	case <-flight.done:
		return flight, nil
	case <-timer.C:
	case <-ctx.Done():
	}
	r.log.Info("collection continues past the pass that started it",
		"assignment", record.Assignment.ID, "attempt", record.Package.Package.Identity.AttemptID,
		"budget", budget)
	return nil, fmt.Errorf("%w (started this pass)", errCollectionRunning)
}

// collectionClaimed reports whether the journal still names the attempt a
// collection was started for as collecting.
func (r *Runtime) collectionClaimed(id string, record AttemptRecord) (bool, error) {
	state, err := r.journal.snapshot()
	if err != nil {
		return false, err
	}
	current, ok := state.Attempts[id]
	return ok && sameCollection(current, record), nil
}

// sameCollection reports whether current, the attempt as the journal records
// it now, is still the collection record started: the same assignment epoch
// and attempt, collecting, and not paused. Anything else means the attempt was
// superseded or already finalized while the collection ran, and a result from
// it must not decide the attempt.
//
// The assignment lease is deliberately not part of this. The worker's copy of
// the lease is only as fresh as the last renewal it applied, and a coordinator
// that saw a lease lapse re-claims the assignment from the next observation
// that shows it present; the coordinator, not the worker's clock, fences a
// result by assignment epoch and lease token.
func sameCollection(current, record AttemptRecord) bool {
	return current.Phase == PhaseCollecting && current.LocalThrottle == nil &&
		current.Assignment.ID == record.Assignment.ID &&
		current.Assignment.Epoch == record.Assignment.Epoch &&
		current.Package.Package.Identity.AttemptID == record.Package.Package.Identity.AttemptID
}

// releaseCollection removes a finished collection from the registry. A failed
// collection is gone once released, so the next pass that reaches the attempt
// starts a fresh one, as a deferred collection always has.
func releaseCollection(key string, flight *collectionFlight) {
	collectionFlights.Lock()
	if collectionFlights.running[key] == flight {
		delete(collectionFlights.running, key)
	}
	collectionFlights.Unlock()
}

// finishCollection records the outcome of a finished collection in the
// journal and only then releases it from the registry, so that no pass can
// read the attempt as collecting with no collection registered in between.
//
// A successful outcome completes the attempt only when the journal still
// names the same collection. An attempt superseded or finalized while its
// collection ran keeps what the journal says; the result is discarded here
// and the attempt is never reported completed from it.
func (r *Runtime) finishCollection(id string, record AttemptRecord, flight *collectionFlight) error {
	defer releaseCollection(r.collectionFlightKey(record), flight)
	settleUnproven := errors.Is(flight.err, ErrSettleUnproven)
	if flight.err != nil && !settleUnproven {
		return fmt.Errorf("collection deferred: %w", flight.err)
	}
	if settleUnproven {
		// The result is durable in custody; only the provider settlement is
		// still unproven. Complete the attempt and retry settlement later
		// instead of repeating collection.
		r.log.Warn("result published; T3 settlement deferred", "assignment", id, "error", flight.err)
	}
	return r.journal.update(func(state *journalState) error {
		current, ok := state.Attempts[id]
		if !ok || !sameCollection(current, record) {
			phase := Phase("")
			if ok {
				phase = current.Phase
			}
			r.log.Warn("collection result discarded; the attempt changed while it was being collected",
				"assignment", id, "attempt", record.Package.Package.Identity.AttemptID,
				"epoch", record.Assignment.Epoch, "present", ok, "phase", phase)
			return nil
		}
		current.Phase = PhaseCompleted
		current.Failure = ""
		current.SettlePending = settleUnproven
		if record.WorkspacePath != "" {
			current.WorkspacePath = record.WorkspacePath
		}
		if record.ThreadID != "" {
			current.ThreadID = record.ThreadID
		}
		current.UpdatedAt = r.now()
		state.Attempts[id] = current
		state.Sequence++
		return nil
	})
}
