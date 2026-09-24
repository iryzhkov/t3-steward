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

// collectionFlight is one collection running for one attempt.
type collectionFlight struct {
	done chan struct{}
	err  error
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
// it finishes takes its result. Nothing here is durable: a worker that
// restarts mid-collection finds the attempt collecting in its journal and
// collects it again, as it always has.
func (r *Runtime) collectOnce(ctx context.Context, record AttemptRecord) error {
	key := r.collectionFlightKey(record)
	collectionFlights.Lock()
	flight, running := collectionFlights.running[key]
	started := !running
	if started {
		flight = &collectionFlight{done: make(chan struct{})}
		collectionFlights.running[key] = flight
		budget := r.finalizationTimeout(record)
		flightCtx, cancel := context.WithTimeout(r.config.Lifetime, budget)
		go func() {
			defer cancel()
			flight.err = r.collectWithPauseEvidence(flightCtx, record)
			close(flight.done)
		}()
	}
	collectionFlights.Unlock()

	if !started {
		select {
		case <-flight.done:
			return r.takeCollection(key, flight)
		default:
			return errCollectionRunning
		}
	}
	timer := time.NewTimer(collectionWaitGrace)
	defer timer.Stop()
	select {
	case <-flight.done:
		return r.takeCollection(key, flight)
	case <-timer.C:
	case <-ctx.Done():
	}
	r.log.Info("collection continues past the pass that started it",
		"assignment", record.Assignment.ID, "attempt", record.Package.Package.Identity.AttemptID,
		"budget", r.finalizationTimeout(record))
	return fmt.Errorf("%w (started this pass)", errCollectionRunning)
}

// takeCollection removes a finished collection from the registry and returns
// its outcome. A failed collection is gone once taken, so the next pass that
// reaches the attempt starts a fresh one, as a deferred collection always has.
func (r *Runtime) takeCollection(key string, flight *collectionFlight) error {
	collectionFlights.Lock()
	if collectionFlights.running[key] == flight {
		delete(collectionFlights.running, key)
	}
	collectionFlights.Unlock()
	return flight.err
}
