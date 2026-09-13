package workerruntime

import (
	"context"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestWorkerWaitsForTerminalTurnEvidence(t *testing.T) {
	for _, state := range []string{"", "queued", "pending", "running", "completed", "interrupted", "error"} {
		t.Run(state, func(t *testing.T) {
			control := &recordingT3{thread: &domain.Thread{ID: "thread-1", TurnState: state}}
			publisher := &recordingPublisher{}
			driver := &LocalDriver{T3: control, Publisher: publisher}
			observed, err := driver.ObserveThread(context.Background(), testPackage())
			if err != nil {
				t.Fatal(err)
			}
			terminal := state == "completed" || state == "interrupted" || state == "error"
			if (observed == backlog.DispatchThreadStopped) != terminal {
				t.Fatalf("state %q observed as %q", state, observed)
			}
			if !terminal {
				if err := driver.Collect(context.Background(), testPackage(), t.TempDir()); err == nil {
					t.Fatal("nonterminal turn collected")
				}
				if len(publisher.results) != 0 || len(control.settlements) != 0 {
					t.Fatal("nonterminal turn published or settled")
				}
				if err := driver.StopThread(context.Background(), testPackage()); err != nil {
					t.Fatal(err)
				}
				if control.stops != 1 {
					t.Fatal("unproven terminal state skipped containment")
				}
			}
		})
	}
}
