package wait

import (
	"strings"
	"testing"
)

func TestNodeWakeSummaryNamesTheFailureClass(t *testing.T) {
	t.Parallel()
	failed := WakeSummaryTask{Task: "build", Progress: "failed", Failure: "provider turn did not complete successfully: overloaded",
		FailureClass: "infrastructure/provider-turn-failed"}
	rows := 10
	single := WakeSummary{Run: "run-1", Workflow: "campaign", Tasks: []WakeSummaryTask{failed}}
	if text := single.nodeProse(&rows); !strings.Contains(text, "campaign/build (run-1) failed (infrastructure/provider-turn-failed): ") {
		t.Fatalf("single task prose:\n%s", text)
	}
	rows = 10
	sink := WakeSummary{Run: "run-1", Workflow: "campaign", Progress: "failed", sink: true, Tasks: []WakeSummaryTask{failed}}
	if text := sink.nodeProse(&rows); !strings.Contains(text, "failed at build (infrastructure/provider-turn-failed): ") {
		t.Fatalf("sink prose:\n%s", text)
	}
	// Without a class the prose is what it was before classification.
	rows = 10
	failed.FailureClass = ""
	plain := WakeSummary{Run: "run-1", Workflow: "campaign", Tasks: []WakeSummaryTask{failed}}
	if text := plain.nodeProse(&rows); !strings.Contains(text, "campaign/build (run-1) failed: ") {
		t.Fatalf("unclassified prose:\n%s", text)
	}
}
