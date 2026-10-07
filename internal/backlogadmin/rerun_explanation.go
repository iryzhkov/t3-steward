package backlogadmin

import (
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func reusedCommitDetails(run domain.WorkflowRun) []string {
	if run.Graph == nil || run.Graph.RerunOf == nil || run.Graph.RerunOf.ReusedCommits == nil {
		return nil
	}
	var details []string
	for _, commit := range *run.Graph.RerunOf.ReusedCommits {
		failure := "verification failed"
		if len(commit.VerificationFailures) > 0 {
			failure = commit.VerificationFailures[0]
		}
		details = append(details, fmt.Sprintf("reusing %s/%s %s from failed attempt %s (verification failed: %s)", commit.Producer, commit.Name, commit.Commit, commit.SourceAttemptID, failure))
	}
	return details
}
