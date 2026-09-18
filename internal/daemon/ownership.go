package daemon

import (
	"context"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// ThreadOwnership answers which T3 threads on this host belong to a live
// steward attempt. The authority is the worker runtime's durable journal on the
// same host: a thread the worker created for an attempt is owned by that
// attempt from dispatch until the attempt is terminal on the worker or its
// assignment is released, and only while the journal is fresh: an attempt
// whose assignment lease has expired no longer owns its thread, because only
// a live worker renews leases, so a crashed worker's threads return to the
// watchdog once its leases lapse.
//
// The watchdog consults it on every tick and leaves owned threads alone. It
// never warns, drains, stops or resumes them, and it cancels any resume intent
// recorded for them, including an intent written before this rule existed. The
// worker runtime pauses and resumes its own threads through the throttle path
// instead, because only the worker knows whether the attempt is still live.
//
// A nil ThreadOwnership means the host runs no worker; every thread is then
// unowned and the watchdog behaves as it always has.
type ThreadOwnership interface {
	// OwnedThreads maps each owned thread id to the attempt id that owns it.
	OwnedThreads(ctx context.Context) (map[string]string, error)
}

// OwnedThreadReason is the reason recorded on a resume intent that is
// cancelled because a steward attempt owns its thread.
func OwnedThreadReason(attemptID string) string {
	return "thread owned by steward attempt " + attemptID
}

// ownedThreads reads the current ownership. A read failure degrades to
// "nothing is owned", which is today's behaviour, and is logged once until the
// next successful read; the reverse degradation, treating every thread as
// owned, would silence the watchdog for interactive sessions.
func (d *Daemon) ownedThreads(ctx context.Context) map[string]string {
	if d.Ownership == nil {
		return nil
	}
	owned, err := d.Ownership.OwnedThreads(ctx)
	d.mu.Lock()
	defer d.mu.Unlock()
	if err != nil {
		if !d.ownershipWarned {
			d.log.Warn("thread ownership unavailable; treating every thread as unowned", "err", err)
			d.ownershipWarned = true
		}
		return nil
	}
	if d.ownershipWarned {
		d.log.Info("thread ownership readable again")
		d.ownershipWarned = false
	}
	return owned
}

// unownedThreads drops the threads owned by a live attempt from a thread list.
func unownedThreads(threads []domain.Thread, owned map[string]string) []domain.Thread {
	if len(owned) == 0 {
		return threads
	}
	out := make([]domain.Thread, 0, len(threads))
	for _, t := range threads {
		if _, ok := owned[t.ID]; ok {
			continue
		}
		out = append(out, t)
	}
	return out
}
