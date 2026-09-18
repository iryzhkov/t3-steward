package workerruntime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// JournalThreadOwnership answers the watchdog's ownership question from the
// worker's durable journal on this host. It reads the journal file the
// persistent worker writes, so it works from the watchdog's own process and
// after either process restarts; nothing is remembered in memory except the
// once-only log state.
//
// Ownership read from the journal is bounded by freshness. The journal is
// advanced only by a live worker process, so a record that says "running" is
// evidence only while its assignment lease, which the coordinator renews
// through that worker, has not expired. A crashed or stopped worker stops
// renewing, its leases expire, and its threads become the watchdog's again
// rather than staying immune forever. A record without a lease expiry is
// bounded by OwnershipMaxAge since its last update instead.
type JournalThreadOwnership struct {
	// Home is the worker's home directory, where its bootstrap and retained
	// catalog live.
	Home string
	// Now is the clock for the freshness bound; nil means time.Now.
	Now func() time.Time
	// Log receives the once-only notice when stale records are ignored; nil
	// discards it.
	Log *slog.Logger

	mu          sync.Mutex
	staleWarned bool
}

// OwnershipMaxAge bounds ownership read from a record whose assignment carries
// no lease expiry: after this long without a journal update the record no
// longer proves a live worker, and its thread is unowned for the watchdog.
const OwnershipMaxAge = time.Hour

// OwnedThreads implements daemon.ThreadOwnership. A host without a worker
// bootstrap owns nothing; a worker without a catalog or journal owns nothing;
// a record whose lease has expired owns nothing.
func (o *JournalThreadOwnership) OwnedThreads(context.Context) (map[string]string, error) {
	attempts, err := hostJournalAttempts(o.Home)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	now := o.now()
	owned := make(map[string]string)
	var stale []string
	for _, id := range sortedAttemptIDs(attempts) {
		record := attempts[id]
		if !attemptOwnsThread(record) {
			continue
		}
		threadID := record.ThreadID
		if threadID == "" {
			threadID = record.Package.Package.Identity.ThreadID
		}
		if threadID == "" {
			continue
		}
		if why := ownershipStale(record, now); why != "" {
			stale = append(stale, fmt.Sprintf("%s (thread %s): %s", record.Assignment.AttemptID, threadID, why))
			continue
		}
		owned[threadID] = record.Assignment.AttemptID
	}
	o.noteStale(stale)
	return owned, nil
}

func (o *JournalThreadOwnership) now() time.Time {
	if o.Now != nil {
		return o.Now()
	}
	return time.Now()
}

// noteStale logs once when records are ignored for staleness and once more
// when none are, so a dead worker's journal is named in the log exactly once
// rather than on every tick.
func (o *JournalThreadOwnership) noteStale(stale []string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	switch {
	case len(stale) != 0 && !o.staleWarned:
		o.staleWarned = true
		if o.Log != nil {
			o.Log.Warn("thread ownership ignored for stale attempt records; the watchdog treats their threads as unowned",
				"attempts", strings.Join(stale, "; "))
		}
	case len(stale) == 0 && o.staleWarned:
		o.staleWarned = false
		if o.Log != nil {
			o.Log.Info("no stale attempt records remain in the worker journal")
		}
	}
}

// ownershipStale reports why a record that owns its thread by phase no longer
// proves a live worker, or "" while it does. The assignment lease is the
// bound when the record has one; the journal update age is the bound
// otherwise.
func ownershipStale(record AttemptRecord, now time.Time) string {
	if lease := record.Assignment.LeaseExpiresAt; !lease.IsZero() {
		if !lease.After(now) {
			return "assignment lease expired at " + lease.UTC().Format(time.RFC3339)
		}
		return ""
	}
	if !record.UpdatedAt.IsZero() && now.Sub(record.UpdatedAt) > OwnershipMaxAge {
		return fmt.Sprintf("no lease and the record was last updated at %s, more than %s ago", record.UpdatedAt.UTC().Format(time.RFC3339), OwnershipMaxAge)
	}
	return ""
}

// attemptOwnsThread reports whether an attempt still owns its thread by its
// phase: from dispatch until the attempt is terminal on the worker or its
// assignment has been released by a confirmed coordinator stop.
func attemptOwnsThread(record AttemptRecord) bool {
	switch record.Phase {
	case PhaseDispatching, PhaseRunning, PhaseStopping, PhaseWaiting, PhaseCollecting:
		return true
	case PhaseStopped:
		released := record.StopConfirmed && hasCommandRequest(record, domain.WorkerCommandStop)
		return !released
	default:
		return false
	}
}
