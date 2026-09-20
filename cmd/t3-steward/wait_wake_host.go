package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Where a wake goes, asked and answered the same way by every command that
// registers an interactive coordinator-held wait.
//
// There are two registration paths -- the kind flags (--node, --quota) and the
// superseded target spellings (--run, --task <run>/<task>) -- and they used to
// decide this separately. Only one of them stated the calling host, so the same
// wait registered as "--node run-x" and as "--run run-x" was recorded for two
// different hosts, and on every host that is not the coordinator the first of
// them was never delivered: the wake was sent into the coordinator's own T3,
// where the calling thread does not exist. The command still printed "end this
// turn", so the thread ended its turn and nothing ever woke it. These three
// functions are that decision, written once.

// coordinatorWaitRelease asks what release the coordinator runs, over the
// transport the calling command already holds. A release that cannot be read is
// the empty string, which statedWakeHost treats as a coordinator that does not
// record the calling host.
func coordinatorWaitRelease(transport coordinatorTransport) func(context.Context) string {
	return func(ctx context.Context) string {
		return coordinatorRelease(ctx, func(ctx context.Context, query backlogadmin.Query) (backlogadmin.Response, error) {
			query.Version = backlogadmin.Version
			query.Principal = transport.principal
			return transport.client.Query(ctx, query)
		})
	}
}

// statedWakeHost is the delivery host to send with a registration: this host,
// for a coordinator that records it, and nothing otherwise.
//
// An older coordinator decodes the operation with unknown fields disallowed and
// would refuse the whole registration over a field it does not know, so a
// client one release ahead of the fleet must be able to register without it.
func statedWakeHost(release, caller string) string {
	if newEnough, known := releaseAtLeast(release, nodeWakeDeliveryHostRelease); known && newEnough {
		return caller
	}
	return ""
}

// reportNodeWaitRegistration prints what the caller has to do next: end the
// turn, or the reason nothing will wake this thread.
//
// It reads the host from the coordinator's own answer rather than from what the
// client asked for, because an older coordinator records its own hostname and a
// replayed registration keeps the host it was first registered under. A wait
// whose wake has already been delivered or cancelled has nothing to instruct.
//
// The instruction goes to stderr: under --json stdout is one document a strict
// reader has to be able to parse.
func reportNodeWaitRegistration(out io.Writer, registered domain.NodeWait, caller, release string, delivery nodeWakeDelivery, now time.Time) {
	if registered.Delivery == "delivered" || registered.Delivery == "cancelled" {
		return
	}
	if reason := undeliverableWake(registered.Host, caller, release, delivery, now); reason != "" {
		fmt.Fprintln(out, "The wait was registered, but its wake is undeliverable: "+reason+".")
		fmt.Fprintln(out, "Nothing will wake this thread, so do not end this turn waiting for a wake.")
		return
	}
	fmt.Fprintln(out, "End this turn now; the coordinator has registered the wait.")
}
