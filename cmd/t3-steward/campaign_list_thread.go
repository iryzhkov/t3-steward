package main

import (
	"context"
	"errors"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// listThreadFlag and listThreadNote document --thread on "backlog list" and
// on "campaign list", which takes its flags from that page.
var listThreadFlag = helpFlag{Name: "--thread", Value: "current|ID", Default: "every thread",
	Text: "Only runs with a pending or retained notification for this T3 thread: a node wait on the thread targets the run. current resolves as --notify-thread current does, and is refused rather than widened when it does not resolve."}

const listThreadNote = "--thread reads ownership from the coordinator's node waits, the notification that campaign submit, task run and review register for the calling thread, so it lists the runs a coordinating session submitted. " +
	"A run submitted with --no-notify has no notification and is never listed for any thread; a recorded run owner arrives with Wave C."

// listThreadRuns answers "list --thread": the runs whose completion notifies
// the named T3 thread.
//
// Ownership is read from the coordinator's node waits rather than asked of the
// coordinator, because no release records who submitted a run. A run belongs
// to a thread when any node wait on that thread targets one of the run's
// nodes, which is what "campaign submit", "task run" and "review" register
// through registerCampaignNotification, and also what "wait add --run"
// registers by hand. A run submitted with --no-notify has no such wait and so
// no owner; a recorded owner is Wave C's fix.
//
// The node-wait list is the unnarrowed one, and the coordinator keeps every
// row of it (coordinator_node_waits is append-only), so a run whose wake was
// delivered, cancelled or timed out is still found. The filter is applied to
// the ordinary workflow list here, on the client, so a coordinator of any
// release answers both questions unchanged.
func (c backlogAdminCLI) listThreadRuns(ctx context.Context, requested string) (map[string]bool, error) {
	ownership, err := readThreadOwnership(ctx, c.nodeWaits, c.resolveThread, requested, "campaign list", "nothing was listed")
	return ownership.runs, err
}

// threadOwnership is what the coordinator's node waits say a thread owns: the
// runs any of its waits targets, and those waits.
type threadOwnership struct {
	thread string
	runs   map[string]bool
	// waits are the thread's own node waits that target a run.
	waits []domain.NodeWait
}

// resolveThreadFlag turns a --thread value into a T3 thread id: "current" is
// resolved exactly as --notify-thread current is and refused, never widened
// to every thread, when it does not resolve. verb and consequence render the
// refusal for the invoked command.
func resolveThreadFlag(resolveThread func(string) (string, error), requested, verb, consequence string) (string, error) {
	if resolveThread == nil {
		return "", errors.New("thread identity resolution is unavailable")
	}
	explicit := requested
	if requested == "current" {
		explicit = ""
	}
	thread, err := resolveThread(explicit)
	if err != nil {
		return "", refuseUnresolvedThread(verb, consequence, "--thread", "", err)
	}
	if thread == "" {
		return "", errors.New("--thread resolved to an empty T3 thread id")
	}
	return thread, nil
}

// readThreadOwnership resolves --thread and reads which runs that thread owns
// from the unnarrowed node-wait list, the rule "campaign list --thread" and
// the run-collection verbs share.
func readThreadOwnership(ctx context.Context, nodeWaits func(context.Context, backlogadmin.NodeWaitOperation) (backlogadmin.NodeWaitResponse, error),
	resolveThread func(string) (string, error), requested, verb, consequence string) (threadOwnership, error) {
	if resolveThread == nil {
		return threadOwnership{}, errors.New("thread identity resolution is unavailable")
	}
	if nodeWaits == nil {
		return threadOwnership{}, errors.New("coordinator node-wait transport is unavailable")
	}
	thread, err := resolveThreadFlag(resolveThread, requested, verb, consequence)
	if err != nil {
		return threadOwnership{}, err
	}
	response, err := nodeWaits(ctx, backlogadmin.NodeWaitOperation{Action: "list"})
	if err != nil {
		return threadOwnership{}, err
	}
	ownership := threadOwnership{thread: thread, runs: map[string]bool{}}
	for _, wait := range response.Waits {
		if wait.Request.ThreadID == thread && wait.Request.Target.RunID != "" {
			ownership.runs[wait.Request.Target.RunID] = true
			ownership.waits = append(ownership.waits, wait)
		}
	}
	return ownership, nil
}
