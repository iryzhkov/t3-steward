package workerruntime

import (
	"context"
	"errors"
	"os"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// JournalThreadOwnership answers the watchdog's ownership question from the
// worker's durable journal on this host. It reads the journal file the
// persistent worker writes, so it works from the watchdog's own process and
// after either process restarts; nothing is remembered in memory.
type JournalThreadOwnership struct {
	// Home is the worker's home directory, where its bootstrap and retained
	// catalog live.
	Home string
}

// OwnedThreads implements daemon.ThreadOwnership. A host without a worker
// bootstrap owns nothing; a worker without a catalog or journal owns nothing.
func (o JournalThreadOwnership) OwnedThreads(context.Context) (map[string]string, error) {
	attempts, err := hostJournalAttempts(o.Home)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	owned := make(map[string]string)
	for _, record := range attempts {
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
		owned[threadID] = record.Assignment.AttemptID
	}
	return owned, nil
}

// attemptOwnsThread reports whether an attempt still owns its thread: from
// dispatch until the attempt is terminal on the worker or its assignment has
// been released by a confirmed coordinator stop.
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
