package domain

// ThreadPresence is what T3's answer says about one thread, for a caller that
// must not mistake "not in this answer" for "deleted".
type ThreadPresence string

const (
	// ThreadLive is a thread T3 holds and has not archived.
	ThreadLive ThreadPresence = "live"
	// ThreadArchived is a thread T3 holds archived: out of the shell
	// snapshot, present in the full index.
	ThreadArchived ThreadPresence = "archived"
	// ThreadDeleted is a thread T3 reports deleted. It is authoritative.
	ThreadDeleted ThreadPresence = "deleted"
	// ThreadAbsent is a thread in neither the shell snapshot nor the full
	// index of an answer that held other threads. It is evidence, not proof:
	// a caller confirms it over time before acting on it.
	ThreadAbsent ThreadPresence = "absent"
)
