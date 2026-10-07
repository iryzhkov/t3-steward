package backlogadmin

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"io"
	"strings"
	"testing"
)

func TestExplanationIncludesRecordedGateEvidence(t *testing.T) {
	reader := explainReader{records: sqlite.CoordinatorRecords{
		Workflows:    []domain.Workflow{{ID: "wf", TaskIDs: []string{"task"}}},
		WorkflowRuns: []domain.WorkflowRun{{ID: "run-1", WorkflowID: "wf"}},
		Tasks:        []domain.Task{{ID: "task", WorkflowID: "wf", Name: "build"}},
		Attempts:     []domain.Attempt{{ID: "attempt", WorkflowRunID: "run-1", TaskID: "task", Progress: domain.ProgressSucceeded}},
		Artifacts:    []domain.Artifact{{ID: "gate-attempt", WorkflowRunID: "run-1", TaskID: "task", AttemptID: "attempt", Kind: domain.ArtifactGate, Name: "gate", MediaType: "application/json"}},
	}}
	service := explainService(t, reader, false)
	service.SetArtifactOpener(func(_ context.Context, id string) (domain.Artifact, io.ReadCloser, error) {
		if id != "gate-attempt" {
			t.Fatalf("unexpected gate artifact %q", id)
		}
		return reader.records.Artifacts[0], io.NopCloser(strings.NewReader(`{"passed":true,"attempt":"attempt","treeHash":"tree-identity","logArtifact":"gate/log.txt"}`)), nil
	})
	explanation := explainTask(t, service, "task")
	// Read the whole transport document because gate evidence is part of JSON,
	// with the same provenance also summarized in text by informational details.
	found := false
	for _, detail := range explanation.Details {
		if strings.Contains(detail, "passed=true attempt=attempt tree=tree-identity") {
			found = true
		}
	}
	if !found {
		t.Fatalf("gate provenance absent: %+v", explanation)
	}
}
