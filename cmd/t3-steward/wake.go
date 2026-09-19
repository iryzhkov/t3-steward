package main

import (
	"context"
	"fmt"
	"io"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Contract 1 lives here, once.
//
// Two verbs start a run and attach a wake to it: "task run" and "campaign
// submit". Both close their record with exactly one instruction about the
// caller's turn, and the rule behind that instruction is the same rule: the
// wake promise and the wait are the same fact. "End this turn now" is printed
// if and only if a wait exists that will fire for the calling thread.
//
// The rule is in this file rather than in either verb because it was fixed in
// one of them first and went on being violated by the other for a whole
// release. A contract with two implementations is a contract only one of them
// keeps.

// startedRunWake is everything a start knows about the wake it attached to the
// run it just created or replayed onto.
type startedRunWake struct {
	// Run is the run the wake watches. Every alternative instruction names it,
	// because a caller who is not being woken has to be told what to do instead.
	Run string
	// Notify is the registration the coordinator answered with, or nil when no
	// wake was asked for.
	Notify *campaignNotification
	// Progress is the progress of the run an idempotency key replayed onto, and
	// is empty when this start created the run. ProgressUnavailable is why that
	// progress could not be read, and is empty when it was read.
	Progress            string
	ProgressUnavailable string
}

// runIsTerminal reports whether the run has already ended. Nothing is
// registered to wake a thread for one: the outcome is there to be collected
// now, and a wait on a sink that has already settled would never fire.
func (wake startedRunWake) runIsTerminal() bool {
	return wake.Progress != "" && domain.ProgressState(wake.Progress).Terminal()
}

// spentWake reports whether a registration that came back can no longer wake
// this caller: its wake was already delivered or cancelled, or the coordinator
// holds it for a different thread.
//
// An undeliverable wake is not spent. It is a live registration with a
// delivery-host fault, which has its own message and its own fix.
func spentWake(notification campaignNotification, thread string) bool {
	if notification.Undeliverable != "" {
		return false
	}
	if notification.WaitThreadID != "" && notification.WaitThreadID != thread {
		return true
	}
	return notification.Delivery == "delivered" || notification.Delivery == "cancelled"
}

// willFire reports whether a wait exists that will fire for the calling
// thread: a wait was registered, its wake is deliverable to this host, it has
// not already been delivered or cancelled, it is held for this thread and not
// another, and the run it watches is known to be still running.
func (wake startedRunWake) willFire() bool {
	switch {
	case wake.Notify == nil, wake.Notify.WaitID == "":
		return false
	case wake.Notify.Undeliverable != "":
		return false
	case spentWake(*wake.Notify, wake.Notify.ThreadID):
		return false
	case wake.ProgressUnavailable != "":
		// The run's progress could not be read, so whether it is still running
		// to be woken from is unknown. An unknown is not a promise.
		return false
	case wake.runIsTerminal():
		return false
	}
	return true
}

// attachWake registers the wake a start promises, and is the one place either
// verb decides whether a wake exists at all.
//
// Nothing is registered for a run that has already ended. A registration that
// comes back spent -- delivered, cancelled, or held for another thread -- is
// replaced by a fresh one under a new ID, because reporting a spent wait as
// this call's wake is exactly the lie contract 1 forbids. A fresh one that is
// spent too, or that the coordinator refuses, is returned as it is, and the
// record then says plainly that no wake is attached.
func (c campaignCLI) attachWake(ctx context.Context, key, runID, thread string, terminal bool) (*campaignNotification, error) {
	if thread == "" || terminal {
		return nil, nil
	}
	notification, err := c.registerCampaignNotification(ctx, key, runID, thread)
	if err != nil {
		return nil, err
	}
	if spentWake(notification, thread) {
		if fresh, freshErr := c.registerCampaignNotification(ctx, key+"-"+newWaitID(), runID, thread); freshErr == nil {
			notification = fresh
		}
	}
	return &notification, nil
}

// renderRunProgress prints the progress of the run a start replayed onto, and
// reports whether it printed anything. A start that created the run has no
// progress of its own to state and prints nothing here.
//
// It is shared for the same reason the promise is: a caller told that no wake
// is attached has to be told why, and the why is this line.
func renderRunProgress(out io.Writer, wake startedRunWake) bool {
	switch {
	case wake.ProgressUnavailable != "":
		fmt.Fprintf(out, "progress unknown: %s\n", wake.ProgressUnavailable)
	case wake.Progress != "":
		fmt.Fprintf(out, "progress %s (this run already exists; nothing new was started)\n", wake.Progress)
	default:
		return false
	}
	return true
}

// renderWake prints the wait, if there is one, and then exactly one
// instruction about the caller's turn. The "End this turn now" line is printed
// if and only if willFire, so the promise and the wait are the same fact.
func renderWake(out io.Writer, wake startedRunWake) {
	if wake.Notify != nil {
		line := fmt.Sprintf("notify thread %s (wait %s)", wake.Notify.ThreadID, wake.Notify.WaitID)
		if wake.Notify.Delivery != "" {
			line += " delivery=" + wake.Notify.Delivery
		}
		if wake.Notify.Host != "" {
			// Host is printed because host is where D-4 hid: a wake recorded for
			// another host is sent into the T3 of that host.
			line += " host=" + wake.Notify.Host
		}
		if wake.Notify.WaitThreadID != "" && wake.Notify.WaitThreadID != wake.Notify.ThreadID {
			line += " held-for-thread=" + wake.Notify.WaitThreadID
		}
		if wake.Notify.Undeliverable != "" {
			line += " undeliverable: " + wake.Notify.Undeliverable
		}
		fmt.Fprintln(out, line)
	}
	if wake.willFire() {
		fmt.Fprint(out, "End this turn now; the steward wakes this thread when the run ends.\n")
		return
	}
	switch {
	case wake.ProgressUnavailable != "":
		fmt.Fprintf(out, "This run's progress could not be read, so nothing is promised about a wake. "+
			"Read it with:\n  t3-steward diagnose %s\n", wake.Run)
	case wake.runIsTerminal():
		fmt.Fprintf(out, "This run already ended %s, so there is nothing to wait for and nothing will wake "+
			"this thread. Collect it now.\n", wake.Progress)
	case wake.Notify == nil:
		fmt.Fprint(out, "notify none: nothing will wake a thread when this run ends\n")
	case wake.Notify.Undeliverable != "":
		// The run exists and the wait exists; what does not exist is a path from
		// one to this thread. Promising a wake here is worse than promising
		// nothing, because the agent would end its turn on it.
		fmt.Fprintf(out, "The run was started. Nothing will wake this thread, so do not end this turn "+
			"waiting for a wake.\nWatch it instead with:\n  t3-steward campaign show %s\n", wake.Run)
	default:
		fmt.Fprintf(out, "No wake is attached to this run, so do not end this turn waiting for one. "+
			"Attach one with:\n  t3-steward wait add --run %s\n", wake.Run)
	}
}
