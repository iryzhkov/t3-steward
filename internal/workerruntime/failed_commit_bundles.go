package workerruntime

import (
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// The carried dependency binding identifies the retained source attempt without
// changing the bundle protocol. Provenance validation checks this binding again
// before obtaining a candidate; the bundle importer checks the exact recorded ref.
func failedCommitBundleRef(pkg workerproto.ExecutionPackage, input workerproto.CommitBundleInput) string {
	for _, dependency := range pkg.Dependencies {
		source := dependency.Provenance
		if source != nil && source.RunID == input.WorkflowRunID && source.TaskID == input.TaskID && source.AttemptID != "" {
			return backlog.FailedCampaignRef(source.RunID, source.TaskID, source.AttemptID, input.Name)
		}
	}
	return ""
}
