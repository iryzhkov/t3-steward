package backlogadmin

import (
	"context"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

// Explain shows the completion gate's decision for a finished attempt: the
// heads it compared and, after a commit that invalidated an accepted round,
// that a new round is needed.
func TestExplainShowsTheReviewGateDecision(t *testing.T) {
	ctx := context.Background()
	store := openAdminTestStore(t)
	seedAdminTestStore(t, store)
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var attempt domain.Attempt
	for _, candidate := range records.Attempts {
		if candidate.ID == "attempt-inspect" {
			attempt = candidate
		}
	}
	reviewed, physical := strings.Repeat("a", 40), strings.Repeat("b", 40)
	gate := domain.EvaluateReviewCompletionGate(
		&domain.ReviewRoundHead{RoundID: "rc-1", Number: 1, CheckpointID: "cp-1", HeadCommit: reviewed, Verdict: "accept", Accepted: true},
		&domain.WorkspaceHead{Schema: domain.WorkspaceHeadSchema, Head: physical}, nil)
	attempt.Progress, attempt.Failure, attempt.ReviewGate = domain.ProgressFailed, gate.Failure(), &gate
	attempt.Revision++
	if err := store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Attempts: []domain.Attempt{attempt}}); err != nil {
		t.Fatal(err)
	}
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.Query(ctx, Query{Version: ExtendedReadVersion, Kind: QueryExplanation, WorkflowRunID: "run-1", TaskID: "task-inspect"})
	if err != nil {
		t.Fatal(err)
	}
	explanation := response.Explanation
	if explanation.ReviewGate == nil || explanation.ReviewGate.Code != domain.ReviewGateHeadChanged ||
		explanation.ReviewGate.ReviewedHead != reviewed || explanation.ReviewGate.PhysicalHead != physical {
		t.Fatalf("explanation = %+v", explanation)
	}
	details := strings.Join(explanation.Details, "\n")
	for _, want := range []string{"head-changed-after-review", reviewed, physical, "new review round"} {
		if !strings.Contains(details, want) {
			t.Fatalf("details %q do not name %q", details, want)
		}
	}
}
