package wait

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"strings"
	"testing"
	"time"
)

func TestIndependentMessageFix1NativeFailureWake(t *testing.T) {
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, progress := range []domain.ProgressState{domain.ProgressFailed, domain.ProgressCancelled} {
		store := &nativeMemory{w: domain.NodeWait{Request: domain.NodeWaitRequest{ID: "nw-review", ThreadID: "thread", Target: domain.NodeRef{RunID: "run-review", TaskID: "sink"}}, Host: "host", SettledAt: &now, Observation: &domain.NodeObservation{Target: domain.NodeRef{RunID: "run-review", TaskID: "sink"}, Progress: progress, Outcome: domain.TaskWaitMet, ExitCode: 0, Reason: "terminal"}, DeliveryID: "token", Delivery: "pending"}}
		control := &nativeControl{fail: true}
		runner := New(store, control, nil)
		runner.NodeHost = "host"
		runner.Tick(context.Background(), nil, nil)
		if control.sends != 1 || store.w.Delivery != "recovery-required" {
			t.Fatal("lost-response control failed")
		}
		sent := control.texts[0]
		for _, want := range []string{"outcome=met", "progress=" + string(progress), "task result run-review", "exit 0 does not establish task success or review ACCEPT", "Cancellation and pause instructions take precedence"} {
			if !strings.Contains(sent, want) {
				t.Fatalf("missing %q in %q", want, sent)
			}
		}
		// A fresh runner must not resend on an absent, inconclusive receipt.
		runner = New(store, control, nil)
		runner.NodeHost = "host"
		runner.Tick(context.Background(), nil, nil)
		if control.sends != 1 {
			t.Fatal("unknown receipt resent")
		}
		control.seen = true
		runner.Tick(context.Background(), nil, nil)
		if control.sends != 1 || store.w.Delivery != "delivered" {
			t.Fatal("confirmed receipt not settled once")
		}
		t.Logf("terminal %s remains met; actual result/verdict required; lost response recovered without resend", progress)
	}
	for _, id := range []string{"a", strings.Repeat("a", 128), "A0._-"} {
		w := domain.NodeWait{Observation: &domain.NodeObservation{Target: domain.NodeRef{RunID: id}, Progress: domain.ProgressSucceeded}}
		if !strings.Contains(nodeWakeResult(w), "task result "+id+"`") {
			t.Fatal("valid boundary identity omitted")
		}
	}
	for _, id := range []string{strings.Repeat("a", 129), "a/b", "a b", "a\x00b", "aé", "a`b", "-x"} {
		w := domain.NodeWait{Observation: &domain.NodeObservation{Target: domain.NodeRef{RunID: id}, Progress: domain.ProgressFailed}}
		if nodeWakeResult(w) != "" {
			t.Fatal("unsafe or oversized identity rendered")
		}
	}
}
