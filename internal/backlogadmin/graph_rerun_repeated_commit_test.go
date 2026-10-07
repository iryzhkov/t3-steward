package backlogadmin

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"testing"
)

func TestRepeatedUseCommitPreservesFailureReceipt(t *testing.T) {
	ctx := context.Background()
	service, store, _ := failedCommitRerunFixture(t)
	request := rerunRequest("lens-first", "qualify")
	request.UseCommit = true
	first, err := service.AmendGraph(ctx, Principal{ID: "operator"}, request)
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := range records.WorkflowRuns {
		if records.WorkflowRuns[i].ID == first.Run.ID {
			records.WorkflowRuns[i].Progress = domain.ProgressFailed
		}
	}
	for i := range records.Attempts {
		if records.Attempts[i].WorkflowRunID == first.Run.ID {
			records.Attempts[i].Progress = domain.ProgressFailed
			records.Attempts[i].Failure = "review failed"
		}
	}
	records.Tasks = nil
	records.Artifacts = nil
	if err := store.SaveCoordinatorRecords(ctx, records); err != nil {
		t.Fatal(err)
	}
	request.ID = "lens-second"
	request.RunID = first.Run.ID
	second, err := service.AmendGraph(ctx, Principal{ID: "operator"}, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Graph.Tasks[0].CarriedInputs) != 2 {
		t.Fatal("lost carried inputs")
	}
	if second.Graph.RerunOf.ReusedCommits == nil {
		t.Fatal("repeated --use-commit rerun still carries failed candidate, but dropped failed-attempt and verification-failure receipt provenance")
	}
	reused := *second.Graph.RerunOf.ReusedCommits
	if len(reused) != 1 || reused[0].SourceAttemptID != "attempt-implement" || len(reused[0].VerificationFailures) != 1 || reused[0].VerificationFailures[0] != "verification command failed (1): go test ./..." {
		t.Fatalf("lost original failure detail: %+v", reused)
	}
	if details := reusedCommitDetails(second.Run); len(details) != 1 {
		t.Fatalf("repeat explain lost details: %v", details)
	}
}
