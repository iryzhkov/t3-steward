package wait

import (
	"context"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestAmbiguousGroupedWakeCannotUseMessageExistence(t *testing.T) {
	runner, store, control, now := taskWaitRunner(t, domain.ProgressActive)
	runner.buckets = healthyBuckets()
	control.observeOK = true
	control.observed = map[string]bool{"shared-delivery": true}
	w := domain.TaskWait{ID: "first", AttemptID: "a1", ThreadID: "t1", DeliveryID: "shared-delivery", Delivery: "manual-recovery-required", WokenAt: now}
	store.taskWaits = map[string]domain.TaskWait{w.ID: w}
	runner.deliverTaskWake(context.Background(), store, control, domain.TaskWaitWakeContext{AttemptID: "a1", ThreadID: "t1", Waits: []domain.TaskWait{w}}, *now)
	if len(control.sends) != 0 || store.taskWaits[w.ID].Delivery != "manual-recovery-required" {
		t.Fatal("ambiguous group sent or auto-settled")
	}
}

func TestOneGroupedWakeSendsAllResultsOnce(t *testing.T) {
	runner, store, control, now := taskWaitRunner(t, domain.ProgressActive)
	store.taskWaits = map[string]domain.TaskWait{}
	runner.buckets = healthyBuckets()
	var members []domain.TaskWait
	for _, id := range []string{"first", "second"} {
		w := domain.TaskWait{ID: id, AttemptID: "a1", ThreadID: "t1", Name: id, DeliveryID: "shared-delivery",
			Delivery: "pending", WokenAt: now, Result: &domain.TaskWaitResult{Outcome: domain.TaskWaitMet}}
		members = append(members, w)
		store.taskWaits[id] = w
	}
	runner.deliverTaskWake(context.Background(), store, control, domain.TaskWaitWakeContext{
		AttemptID: "a1", ThreadID: "t1", Waits: members}, *now)
	if len(control.sends) != 1 {
		t.Fatalf("one grouped wake sent %d turns", len(control.sends))
	}
	if !strings.Contains(control.texts[0], "first") || !strings.Contains(control.texts[0], "second") {
		t.Fatalf("wake omitted a member's evidence: %s", control.texts[0])
	}
}
