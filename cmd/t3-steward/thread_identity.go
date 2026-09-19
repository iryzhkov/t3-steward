package main

import (
	"errors"
	"fmt"
	"strings"
)

// A refusal that names a flag the invoked verb rejects is not a refusal the
// caller can act on: it sends the agent to a second refusal, which is where it
// runs out of ideas. The flag that names a thread differs by verb -- "wait add"
// takes --thread, "task run" and "campaign submit" take --notify-thread -- so
// the resolution failures below carry no flag at all, and each verb renders its
// own advice through refuseUnresolvedThread.

// callerIdentityEnvironment names the environment variables that establish a
// caller identity. Every refusal states them, because a caller outside a T3
// session has no other way to learn what is missing.
func callerIdentityEnvironment() string {
	return strings.Join(providerSessionKeys, ", ")
}

// threadIdentityError is a failure to establish which T3 thread is calling. It
// states the evidence and names no flag; refuseUnresolvedThread adds the one
// flag the invoked verb accepts.
type threadIdentityError struct{ detail string }

func (e threadIdentityError) Error() string { return e.detail }

// unresolvedThread builds one of those failures.
func unresolvedThread(format string, args ...any) error {
	return threadIdentityError{detail: fmt.Sprintf(format, args...)}
}

// refuseUnresolvedThread is the whole refusal one verb prints when no thread
// resolves: what was not done, why, the environment that would fix it, and the
// corrected call for this verb and no other.
//
// verb is the invoked verb as the caller typed it, consequence says plainly
// what did not happen, flag is the flag this verb accepts for naming a thread,
// and rest is the remainder of the corrected call.
func refuseUnresolvedThread(verb, consequence, flag, rest string, cause error) error {
	corrected := strings.TrimSpace("t3-steward " + verb + " " + flag + " <THREAD-ID> " + rest)
	return fmt.Errorf("%s could not identify the calling agent's T3 thread, so %s: %w.\n"+
		"A caller identity comes from %s in the environment.\n"+
		"Name the thread for this call instead:\n  %s",
		verb, consequence, cause, callerIdentityEnvironment(), corrected)
}

// refuseWaitThread renders an unresolved thread for a verb of the wait family,
// which names the thread with --thread. Anything that is not an identity
// failure travels unchanged: a thread that T3 does not know is a different
// fault with a different fix.
func refuseWaitThread(verb, rest string, cause error) error {
	var identity threadIdentityError
	if !errors.As(cause, &identity) {
		return cause
	}
	return refuseUnresolvedThread(verb, "no wait was registered", "--thread", rest, cause)
}
