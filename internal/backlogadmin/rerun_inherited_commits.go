package backlogadmin

import (
	"github.com/iryzhkov/t3-steward/internal/domain"
	"slices"
)

// The source graph's recorded reuse authority survives when its exact carried
// inputs travel on. Receipts and explain both consume this durable provenance,
// rather than each reconstructing a failure from an earlier run.
func inheritFailedCommitReceipts(source domain.WorkflowRun, tasks []domain.Task, reused []domain.ReusedCommit) []domain.ReusedCommit {
	if source.Graph == nil || source.Graph.RerunOf == nil || source.Graph.RerunOf.ReusedCommits == nil {
		return reused
	}
	for _, record := range *source.Graph.RerunOf.ReusedCommits {
		carried := false
		for _, task := range tasks {
			for _, input := range task.CarriedInputs {
				if input.Producer == record.Producer && input.Name == record.Name && input.SourceAttemptID == record.SourceAttemptID {
					carried = true
				}
			}
		}
		if !carried || slices.ContainsFunc(reused, func(existing domain.ReusedCommit) bool {
			return existing.Producer == record.Producer && existing.Name == record.Name && existing.Commit == record.Commit && existing.SourceAttemptID == record.SourceAttemptID
		}) {
			continue
		}
		record.VerificationFailures = slices.Clone(record.VerificationFailures)
		reused = append(reused, record)
	}
	return reused
}
