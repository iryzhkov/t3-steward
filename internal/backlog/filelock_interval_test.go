package backlog

import "time"

// Lock-race tests contend on staging and artifact locks thousands of times, and
// each contended attempt used to wait the production retry interval before
// polling again. Polling every millisecond changes only how soon a waiter sees
// a released lock, never whether it may take it, so every assertion about who
// holds a lock and what it found is unchanged.
func init() {
	fileLockRetryInterval = time.Millisecond
}
