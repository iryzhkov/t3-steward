package wait

import (
	"context"
	"errors"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
	"time"
)

func TestCommandSafeRunIDAcceptsLegacyRerunIDs(t *testing.T) {
	for _, id := range []string{"run:rerun:fix-2", "run:rerun:" + strings.Repeat("a", 128)} {
		if !commandSafeRunID(id) {
			t.Fatal(id)
		}
		now := time.Now()
		w := domain.NodeWait{Request: domain.NodeWaitRequest{Target: domain.NodeRef{RunID: id, TaskID: domain.SinkTaskName}}, SettledAt: &now, Observation: &domain.NodeObservation{Target: domain.NodeRef{RunID: id, TaskID: domain.SinkTaskName}, Progress: domain.ProgressSucceeded}}
		summary := BuildNodeSummary(context.Background(), &fakeSummarySource{run: SummaryRun{ID: id, Workflow: "rerun", Progress: domain.ProgressSucceeded}}, w)
		if summary.Unavailable != "" || summary.Run != id {
			t.Fatalf("%+v", summary)
		}
		if !strings.Contains(nodeWakeResult(w), "task result "+id) {
			t.Fatal("missing collect command")
		}
	}
	for _, id := range []string{"run:rerun:a b", "run:x", "run:rerun:", "run:rerun:" + strings.Repeat("a", 139), "run:rerun:-a"} {
		if commandSafeRunID(id) {
			t.Fatal(id)
		}
		now := time.Now()
		w := domain.NodeWait{Request: domain.NodeWaitRequest{Target: domain.NodeRef{RunID: id, TaskID: domain.SinkTaskName}}, SettledAt: &now, Observation: &domain.NodeObservation{Target: domain.NodeRef{RunID: id, TaskID: domain.SinkTaskName}, Progress: domain.ProgressSucceeded}}
		summary := BuildNodeSummary(context.Background(), &fakeSummarySource{}, w)
		if summary.Unavailable != "unsafe-identity" {
			t.Fatalf("%s: %+v", id, summary)
		}
	}
}

func TestNodeWakeSummaryIsRecordedAtClaim(t *testing.T) {
	for _, fail := range []bool{false, true} {
		now := time.Now()
		id := "run-" + strings.Repeat("a", 32)
		store := &nativeMemory{w: domain.NodeWait{Request: domain.NodeWaitRequest{ID: "nw-record", ThreadID: "thread", Target: domain.NodeRef{RunID: id, TaskID: domain.SinkTaskName}}, Host: "host", SettledAt: &now, Observation: &domain.NodeObservation{Target: domain.NodeRef{RunID: id, TaskID: domain.SinkTaskName}, Progress: domain.ProgressSucceeded, Outcome: domain.TaskWaitMet}, Delivery: "pending", DeliveryID: "token"}}
		control := &nativeControl{}
		runner := New(store, control, nil)
		runner.NodeHost = "host"
		runner.NodeSummary = &fakeSummarySource{run: SummaryRun{ID: id, Workflow: "record", Progress: domain.ProgressSucceeded}}
		calls := 0
		runner.NodeSummaryRecord = func(_ context.Context, waitID string, s WakeSummary) error {
			calls++
			if waitID != "nw-record" || s.Schema != "t3-steward.wake-summary/v1" {
				t.Fatalf("%s %+v", waitID, s)
			}
			if fail {
				return errors.New("record refused")
			}
			return nil
		}
		runner.Tick(context.Background(), nil, nil)
		if calls != 1 || control.sends != 1 {
			t.Fatalf("calls=%d sends=%d", calls, control.sends)
		}
		store.w.Delivery = "pending"
		store.w.DeliveryPayload = control.texts[0]
		runner.Tick(context.Background(), nil, nil)
		if calls != 1 || control.sends != 2 {
			t.Fatalf("frozen calls=%d sends=%d", calls, control.sends)
		}
		store.w.Delivery = "pending"
		store.w.DeliveryPayload = ""
		runner.Tick(context.Background(), nil, nil)
		if calls != 2 || control.sends != 3 {
			t.Fatalf("rebuild calls=%d sends=%d", calls, control.sends)
		}
	}
}
