package sqlite

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

// A campaign submitted with --notify-thread registers an ordinary node wait on
// the run's sink. Two submitting agents may be waiting on the same run, so the
// thing that has to be true is that each settled wait carries the thread it was
// registered for and nobody else's.
func TestRunSinkNotificationSettlesToTheThreadItWasRegisteredFor(t *testing.T) {
	ctx := context.Background()
	store, before, now := sinkStoreFixture(t)
	sink := domain.NodeRef{RunID: "r", TaskID: domain.SinkTaskName}
	for id, thread := range map[string]string{
		"nw-campaign-one": "thread-one",
		"nw-campaign-two": "thread-two",
	} {
		request := domain.NodeWaitRequest{
			ID: id, ThreadID: thread, Name: sink.String(), Target: sink, Timeout: time.Hour,
		}
		if _, err := store.RegisterNodeWait(ctx, request, "operator", "host", now); err != nil {
			t.Fatal(err)
		}
	}
	// Registration alone settles nothing and changes no workflow state.
	waits, err := store.ListNodeWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(waits) != 2 {
		t.Fatalf("waits = %d", len(waits))
	}
	for _, wait := range waits {
		if wait.Observation != nil || wait.SettledAt != nil {
			t.Fatalf("registration settled a wait: %+v", wait)
		}
	}
	after, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	unchanged, _ := json.Marshal(before.Run)
	observed, _ := json.Marshal(after.WorkflowRuns[0])
	if string(unchanged) != string(observed) {
		t.Fatalf("registering a notification changed the run:\n%s\n%s", unchanged, observed)
	}
	if len(after.Attempts) != len(before.Attempts) {
		t.Fatal("registering a notification changed the attempts")
	}
	// The run reaches a terminal outcome: its one task failed, so the sink
	// settles as a failure and both waits observe the same thing.
	run := sinkProjected(t, before, now)
	if err = store.SaveCoordinatorRecords(ctx, CoordinatorRecords{WorkflowRuns: []domain.WorkflowRun{run}}); err != nil {
		t.Fatal(err)
	}
	if err = store.SettleNodeWaits(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	waits, err = store.ListNodeWaits(ctx)
	if err != nil {
		t.Fatal(err)
	}
	threads := map[string]string{}
	for _, wait := range waits {
		if wait.Observation == nil {
			t.Fatalf("a terminal sink left a wait pending: %+v", wait)
		}
		if wait.Observation.ExitCode != 2 {
			t.Fatalf("a failed run woke a thread with exit code %d", wait.Observation.ExitCode)
		}
		// The alias is resolved to the run's canonical sink identity when it
		// is registered, so the settled record names the node it observed.
		if wait.Request.Target.RunID != "r" || wait.Request.Target.TaskID != run.Sink.ID {
			t.Fatalf("wait target = %+v", wait.Request.Target)
		}
		threads[wait.Request.ID] = wait.Request.ThreadID
	}
	if threads["nw-campaign-one"] != "thread-one" || threads["nw-campaign-two"] != "thread-two" {
		t.Fatalf("settled waits are bound to %v", threads)
	}
}
