package backlogadmin

import (
	"context"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
)

func TestCampaignCheckRefusesUnknownCommitReferences(t *testing.T) {
	for _, value := range []string{"campaign:missing/producer/repair", "campaign:source/unknown/repair", "campaign:source/producer/unknown", "campaign:source/producer/repair"} {
		t.Run(value, func(t *testing.T) {
			v := viabilityViewWith(t, func(records *sqlite.CoordinatorRecords) {
				records.WorkflowRuns = []domain.WorkflowRun{{ID: "source", WorkflowID: "workflow"}}
				records.Tasks = []domain.Task{{ID: "producer-id", WorkflowID: "workflow", Name: "producer", Outputs: []domain.ArtifactDeclaration{{Name: "repair", Commit: &domain.CommitOutput{}}}}}
				records.Attempts = []domain.Attempt{{ID: "attempt", WorkflowRunID: "source", TaskID: "producer-id", Number: 1, Progress: domain.ProgressSucceeded}}
			}, nil)
			task := viabilityTaskRequest()
			task.Ref = value
			result := v.viabilityTask(context.Background(), viabilityCatalog(t), task, v.viabilityWorkers())
			found := false
			for _, reason := range result.Reasons {
				if reason.Code == ReasonUnknownNode && reason.Permanent {
					found = true
				}
			}
			if !found || result.Outcome != ViabilityImpossible {
				t.Fatalf("result = %+v", result)
			}
		})
	}
}
