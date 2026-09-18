package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/wait"
)

// An interactive node wait registered with --or-timeout settles at its
// deadline the way the local kinds do: outcome timed-out, exit 0, and a wake
// whose trailer says or-timeout=true. Without the flag the deadline is the
// failure form: exit 2, "timed out", no or-timeout pair.
func TestInteractiveNodeWaitHonoursOrTimeout(t *testing.T) {
	for _, orTimeout := range []bool{true, false} {
		ctx := context.Background()
		store, _, now := taskWaitFixture(t)
		otherRun(t, store, now, domain.ProgressActive)
		request := domain.NodeWaitRequest{
			ID: "nw-deadline", ThreadID: "thread-1", Name: "deploy settles",
			Target: domain.NodeRef{RunID: "r2", TaskID: domain.SinkTaskName}, Timeout: time.Hour, OrTimeout: orTimeout,
		}
		registered, err := store.RegisterNodeWait(ctx, request, "operator", "host", now)
		if err != nil {
			t.Fatal(err)
		}
		if registered.SettledAt != nil || registered.Request.OrTimeout != orTimeout {
			t.Fatalf("or-timeout=%v: registered = %+v", orTimeout, registered)
		}
		// The run is still active when the deadline passes.
		if err := store.SettleNodeWaits(ctx, now.Add(2*time.Hour)); err != nil {
			t.Fatal(err)
		}
		waits, _ := store.ListNodeWaits(ctx)
		if len(waits) != 1 || waits[0].Observation == nil || waits[0].Observation.Outcome != domain.TaskWaitTimedOut {
			t.Fatalf("or-timeout=%v: the deadline did not settle the wait: %+v", orTimeout, waits)
		}
		observation := *waits[0].Observation
		if (observation.ExitCode == 0) != orTimeout {
			t.Fatalf("or-timeout=%v: exit = %d reason=%q", orTimeout, observation.ExitCode, observation.Reason)
		}
		if strings.Contains(observation.Reason, "normal outcome") != orTimeout {
			t.Fatalf("or-timeout=%v: reason = %q", orTimeout, observation.Reason)
		}
		clock := now.Add(3 * time.Hour)
		runner, control := fleetRunner(t, store, &clock, nil)
		runner.NodeHost = "host"
		runner.Tick(ctx, nil, nil)
		if len(control.texts) != 1 || !strings.HasPrefix(control.texts[0], "t3-steward-wait kind=node outcome=timed-out wait=nw-deadline") {
			t.Fatalf("or-timeout=%v: wake = %v", orTimeout, control.texts)
		}
		parsed, _ := wait.ParseWakeTrailer(control.texts[0])
		if (parsed["or-timeout"] == "true") != orTimeout {
			t.Fatalf("or-timeout=%v: trailer = %v", orTimeout, parsed)
		}
	}
}
